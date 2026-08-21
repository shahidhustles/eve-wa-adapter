import { defineChannel, POST, type ChannelEvents } from "eve/channels";
import {
  makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  Browsers,
  downloadMediaMessage,
  getContentType,
  normalizeMessageContent,
  isPnUser,
  isLidUser,
  type WASocket,
  type WAMessage,
  type WAMessageKey,
  type BaileysEventMap,
  DisconnectReason,
} from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import { Cartesia } from "@cartesia/cartesia-js";
import { DeepgramClient } from "@deepgram/sdk";
import { spawn } from "node:child_process";
import { constants } from "node:fs";
import { access } from "node:fs/promises";
import { resolve } from "node:path";
import ffmpegPath from "ffmpeg-static";
import QRCode from "qrcode";
import type { UserContent } from "ai";

/**
 * eve custom channel: WhatsApp via Baileys (unofficial WhatsApp Web API).
 *
 * Self-hosted only. Baileys holds a long-lived WebSocket to WhatsApp's servers.
 * Inbound messages arrive from the socket, not from HTTP webhooks, so this
 * channel captures eve's `from()` dispatcher from a bootstrap HTTP route and
 * reuses it for all subsequent socket-driven dispatches.
 *
 * Supports: text, voice notes (Deepgram transcription + Cartesia voice reply), images, video,
 * GPS location, HITL (rendered as numbered text choices), and proactive
 * sessions.
 *
 * Does NOT support: stickers, groups, polls, or rich buttons.
 *
 * First run prints a QR code to the terminal. Scan it via WhatsApp → Linked
 * Devices. Credentials persist to ./auth_info_baileys and are reused on restart.
 *
 * Testing note: messages where `msg.key.fromMe === true` are skipped to prevent
 * response loops. This means WhatsApp's "Message Yourself" conversation cannot
 * trigger the bot — test from another WhatsApp account/number.
 *
 * @see https://eve.dev/docs/channels/custom
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Channel adapter state, persisted per-session by the eve runtime. */
interface WhatsAppState {
  /** The WhatsApp JID this session is bound to (e.g. "1234567890@s.whatsapp.net"). */
  jid: string;
  /** The Baileys message key of the last inbound message, for read receipts. */
  lastInboundKey?: WAMessageKey;
  /** True when the last inbound message was a voice note; replies go through TTS. */
  voiceReply: boolean;
  /**
   * Pending HITL request. Stored in eve channel state (durable across process
   * restarts), so a numeric reply can be resolved against the right requestId.
   */
  pendingInput?: {
    requestId: string;
    options: { id: string; label: string }[];
    allowFreeform: boolean;
  };
}

/** Per-step channel context handed to every event handler. */
interface WhatsAppChannelContext {
  state: WhatsAppState;
  socket: WASocket | null;
  from: CapturedFrom | null;
}

/**
 * One human answer to a pending HITL request. Mirrors eve's internal
 * `InputResponse` type (`{ requestId, optionId?, text? }`), which is not
 * exported from the public `eve/channels` entry point.
 */
interface WhatsAppInputResponse {
  readonly requestId: string;
  readonly optionId?: string;
  readonly text?: string;
}

/**
 * The `from` dispatcher captured from the bootstrap route. Typed loosely since
 * the real `ChannelSource` closes over internal runtime types we can't import.
 */
interface CapturedFrom {
  (address: string): {
    send: (
      message: string | UserContent,
      options: { auth: unknown; state: WhatsAppState },
    ) => Promise<unknown>;
    respond: (
      inputResponses: readonly WhatsAppInputResponse[],
      options: { auth: unknown; state?: Partial<WhatsAppState> },
    ) => Promise<unknown>;
    clear: () => Promise<unknown>;
    reset: (options: { reason: string }) => Promise<unknown>;
  };
}

/**
 * `resolveSession` captured from the bootstrap route. Snapshots the session
 * currently owning a channel-local address. Used as a restart-safe fallback
 * to check whether a session exists for a JID before dispatching.
 */
type CapturedResolveSession = (address: string) => Promise<unknown | undefined>;

/** A queued inbound message waiting for `from` to be captured. */
interface QueuedMessage {
  jid: string;
  content: UserContent;
  state: WhatsAppState;
}

// ---------------------------------------------------------------------------
// Module-level singletons
// ---------------------------------------------------------------------------

/**
 * Global guard against duplicate sockets. `eve dev` hot-reloads channel
 * modules, which would start a second Baileys WebSocket. We stash the socket on
 * `globalThis` so a re-import reuses the existing connection. This is a
 * pragmatic workaround — eve does not yet provide a background-channel
 * lifecycle API for push-based transports like Baileys.
 */
const GLOBAL_KEY = "__eve_whatsapp_socket__";

interface GlobalState {
  socket: WASocket | null;
  socketStarting: Promise<WASocket> | null;
  capturedFrom: CapturedFrom | null;
  capturedResolveSession: CapturedResolveSession | null;
  messageQueue: QueuedMessage[];
  /**
   * Module-level mirror of pending HITL requests, keyed by JID. The inbound
   * handler runs outside eve's event system and cannot read channel state
   * directly, so we mirror `state.pendingInput` here for quick lookup. This is
   * non-durable (lost on restart). After a restart, the `pendingHITL` map is
   * empty — the inbound handler falls through to `from(jid).send(text)`, and
   * eve's built-in auto-matching (option ID, label, or numeric index) handles
   * the reply against the durable `state.pendingInput`. See HITL docs:
   * "A follow-up whose text matches an option ID, option label, or numeric
   * option index resolves automatically."
   */
  pendingHITL: Map<string, { requestId: string; options: { id: string; label: string }[]; allowFreeform: boolean }>;
  /**
   * Prevents duplicate Baileys event listeners on the same socket during hot
   * reload. Set to true after `sock.ev.on(...)` calls. Reset when a new socket
   * is created (the old listeners die with the old socket).
   */
  listenersAttached: boolean;
  /** Reply mode for accepted inbound messages, kept in arrival order per JID. */
  replyModes: Map<string, boolean[]>;
  /** Reply mode for the turn currently running for a JID. */
  activeReplyModes: Map<string, boolean>;
}

function getGlobal(): GlobalState {
  const g = globalThis as unknown as Record<string, unknown>;
  if (!g[GLOBAL_KEY]) {
    g[GLOBAL_KEY] = {
      socket: null,
      socketStarting: null,
      capturedFrom: null,
      capturedResolveSession: null,
      messageQueue: [],
      pendingHITL: new Map(),
      listenersAttached: false,
      replyModes: new Map(),
      activeReplyModes: new Map(),
    } satisfies GlobalState;
  }
  return g[GLOBAL_KEY] as GlobalState;
}

let socket: WASocket | null;
let socketStarting: Promise<WASocket> | null;
let capturedFrom: CapturedFrom | null;
let capturedResolveSession: CapturedResolveSession | null;
let messageQueue: QueuedMessage[];
let pendingHITL: Map<string, { requestId: string; options: { id: string; label: string }[]; allowFreeform: boolean }>;
let listenersAttached: boolean;
let replyModes: Map<string, boolean[]>;
let activeReplyModes: Map<string, boolean>;

{
  // Initialize module-level vars from the global singleton.
  const g = getGlobal();
  socket = g.socket;
  socketStarting = g.socketStarting;
  capturedFrom = g.capturedFrom;
  capturedResolveSession = g.capturedResolveSession;
  messageQueue = g.messageQueue;
  pendingHITL = g.pendingHITL;
  listenersAttached = g.listenersAttached;
  replyModes = g.replyModes;
  activeReplyModes = g.activeReplyModes;
}

/** Sync module-level vars back to the global singleton (after mutations). */
function syncToGlobal(): void {
  const g = getGlobal();
  g.socket = socket;
  g.socketStarting = socketStarting;
  g.capturedFrom = capturedFrom;
  g.capturedResolveSession = capturedResolveSession;
  g.messageQueue = messageQueue;
  g.pendingHITL = pendingHITL;
  g.listenersAttached = listenersAttached;
  g.replyModes = replyModes;
  g.activeReplyModes = activeReplyModes;
}

const AUTH_DIR = "./auth_info_baileys";
const DEEPGRAM_MODEL = "nova-3";
const CARTESIA_MODEL = "sonic-3.5";

function getRequiredEnv(name: "DEEPGRAM_API_KEY" | "CARTESIA_API_KEY" | "CARTESIA_VOICE_ID"): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required for voice support`);
  return value;
}

async function transcribeVoiceNote(audio: Buffer, mediaType: string): Promise<string> {
  const deepgram = new DeepgramClient({ apiKey: getRequiredEnv("DEEPGRAM_API_KEY") });
  const result = await deepgram.listen.v1.media.transcribeFile(
    { data: audio, contentType: mediaType },
    { model: DEEPGRAM_MODEL, smart_format: true, punctuate: true },
  );
  if (!("results" in result) || !result.results) return "";
  const channel = result.results.channels?.[0];
  return channel?.alternatives?.[0]?.transcript?.trim() ?? "";
}

/** Convert Cartesia's MP3 output to WhatsApp's native voice-note format. */
async function mp3ToWhatsAppVoiceNote(mp3: Buffer): Promise<Buffer> {
  // Eve compiles authored modules into a snapshot. `ffmpeg-static`'s JS loader
  // is copied there, but its native executable is not, so prefer the installed
  // binary in the project working directory.
  const candidates = [
    resolve(process.cwd(), "node_modules", "ffmpeg-static", "ffmpeg"),
    ffmpegPath,
  ].filter((path): path is string => Boolean(path));
  const executable = await (async () => {
    for (const candidate of candidates) {
      try {
        await access(candidate, constants.X_OK);
        return candidate;
      } catch {
        // Try the next location.
      }
    }
    throw new Error("ffmpeg-static did not provide an executable for this platform");
  })();

  return new Promise((resolve, reject) => {
    const ffmpeg = spawn(
      executable,
      [
        "-hide_banner",
        "-loglevel", "error",
        "-i", "pipe:0",
        "-vn",
        "-ac", "1",
        "-c:a", "libopus",
        "-b:a", "32k",
        "-f", "ogg",
        "pipe:1",
      ],
      { stdio: "pipe" },
    );
    const output: Buffer[] = [];
    const errors: Buffer[] = [];
    ffmpeg.stdout.on("data", (chunk: Buffer) => output.push(chunk));
    ffmpeg.stderr.on("data", (chunk: Buffer) => errors.push(chunk));
    ffmpeg.once("error", reject);
    ffmpeg.once("close", (code) => {
      if (code === 0) return resolve(Buffer.concat(output));
      reject(new Error(`ffmpeg exited with code ${code}: ${Buffer.concat(errors).toString().trim()}`));
    });
    ffmpeg.stdin.end(mp3);
  });
}

// ---------------------------------------------------------------------------
// Baileys socket lifecycle
// ---------------------------------------------------------------------------

async function connectSocket(): Promise<WASocket> {
  // Re-read from global in case hot reload reset module-level vars.
  const g = getGlobal();
  socket = g.socket;
  socketStarting = g.socketStarting;

  if (socket) return socket;
  if (socketStarting) return socketStarting;

  socketStarting = (async () => {
    const { state: authState, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
    const { version } = await fetchLatestBaileysVersion();

    const sock = makeWASocket({
      version,
      browser: Browsers.ubuntu("eve-whatsapp"),
      auth: authState,
      // Do NOT use printQRInTerminal — we render the QR ourselves from the
      // `qr` field of connection.update for more control and reliability.
    });

    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("connection.update", (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        // Render the QR code to the terminal using the `qrcode` package.
        QRCode.toString(qr, { type: "terminal", small: true })
          .then((rendered) => {
            console.info("\n[whatsapp] Scan this QR code via WhatsApp → Linked Devices:\n");
            console.info(rendered);
          })
          .catch((err) => {
            console.error("[whatsapp] Failed to render QR code:", err);
            // Fallback: print the raw QR string so the user can use an external tool.
            console.info("[whatsapp] Raw QR data:", qr);
          });
      }

      if (connection === "close") {
        // A previous socket can finish closing after a newer socket has already
        // connected. Never let that stale event clear or reconnect the live one.
        if (socket !== sock) return;

        const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
        const shouldReconnect =
          statusCode !== DisconnectReason.loggedOut &&
          statusCode !== DisconnectReason.connectionReplaced;
        console.error(
          `[whatsapp] connection closed (status ${statusCode}), reconnecting: ${shouldReconnect}`,
        );
        socket = null;
        socketStarting = null;
        listenersAttached = false;
        syncToGlobal();
        if (shouldReconnect) {
          // Bounded delay to avoid hammering WhatsApp on rapid disconnects.
          // Reattach the inbound listener as well as opening the replacement
          // socket. `connectSocket()` alone leaves a reconnected socket unable
          // to receive WhatsApp messages.
          setTimeout(wireSocketListener, Math.min(2000, 500));
        }
      } else if (connection === "open") {
        console.info("[whatsapp] connected");
        // Bootstrap: ping our own HTTP route to capture `from` and drain the queue.
        void bootstrapFrom();
      }
    });

    socket = sock;
    syncToGlobal();
    return sock;
  })().catch((err) => {
    // If socket initialization throws before connection.update can clean up,
    // reset the guards so a future attempt can succeed.
    console.error("[whatsapp] socket initialization failed:", err);
    socket = null;
    socketStarting = null;
    syncToGlobal();
    throw err;
  });

  syncToGlobal();
  return socketStarting;
}

/**
 * Call the channel's own bootstrap route via localhost HTTP to capture `from`.
 * This bridges the gap between Baileys' push-based socket and eve's
 * HTTP-centric channel model: the route handler has access to `from`, which
 * closes over the stable runtime and can be reused for all socket-driven sends.
 *
 * Retries indefinitely with exponential backoff (capped at 30s) so startup
 * can never get permanently stuck. If eve's HTTP server is slow to start,
 * messages remain queued until the bootstrap succeeds and drains them.
 */
async function bootstrapFrom(): Promise<void> {
  const port = process.env.PORT ?? "2000";
  const url = `http://127.0.0.1:${port}/whatsapp/bootstrap`;
  const maxDelay = 30_000;
  let delay = 500;
  let attempt = 0;

  while (true) {
    attempt++;
    try {
      console.info(`[whatsapp] bootstrap attempt ${attempt}...`);
      const res = await fetch(url, { method: "POST" });
      if (res.ok) {
        console.info(`[whatsapp] bootstrap succeeded on attempt ${attempt}`);
        return;
      }
    } catch {
      // Server not listening yet — retry after backoff.
    }
    console.info(`[whatsapp] bootstrap attempt ${attempt} failed, retrying in ${delay}ms`);
    await new Promise((resolve) => setTimeout(resolve, delay));
    delay = Math.min(delay * 2, maxDelay);
  }
}

// ---------------------------------------------------------------------------
// Inbound message dispatch
// ---------------------------------------------------------------------------

function queueReplyMode(jid: string, voiceReply: boolean): void {
  const modes = replyModes.get(jid) ?? [];
  modes.push(voiceReply);
  replyModes.set(jid, modes);
  syncToGlobal();
}

/** Dispatch a parsed message to eve, queuing if `from` isn't captured yet. */
async function dispatchInbound(
  jid: string,
  content: UserContent,
  state: WhatsAppState,
): Promise<void> {
  if (capturedFrom) {
    queueReplyMode(jid, state.voiceReply);
    console.info(`[whatsapp] dispatching inbound message for jid=${jid} voiceReply=${state.voiceReply}`);
    await capturedFrom(jid).send(content, { auth: null, state });
    return;
  }
  // `from` not captured yet — queue for when the bootstrap route fires.
  messageQueue.push({ jid, content, state });
  console.info(`[whatsapp] queued inbound message for jid=${jid}; dispatcher unavailable`);
  syncToGlobal();
  // Safety net: if the automatic bootstrap from `connection.update → open`
  // hasn't succeeded yet (e.g. eve started after WhatsApp connected), trigger
  // another bootstrap attempt now. This is idempotent — the bootstrap route
  // no-ops if `from` is already captured.
  void bootstrapFrom();
}

/**
 * Wire the socket's message listener. Called once on module load.
 * Guards against duplicate listener attachment on the same socket (which
 * would cause one WhatsApp message to trigger multiple eve turns).
 *
 * The `listenersAttached` flag is:
 * - Set to `true` after attaching `messages.upsert` (and other listeners).
 * - Reset to `false` when a new socket is created (old listeners die with
 *   the old socket; the flag must be cleared so the new socket can attach).
 * - Stored on `globalThis` so hot-reloaded module imports don't re-attach.
 */
function wireSocketListener(): void {
  void connectSocket().then((sock) => {
    // Guard: if listeners are already attached to this socket (e.g. after a
    // hot reload that re-executes this module), skip re-attaching.
    if (listenersAttached) {
      console.info("[whatsapp] listeners already attached, skipping");
      return;
    }

    sock.ev.on(
      "messages.upsert",
      async ({ messages, type }: BaileysEventMap["messages.upsert"]) => {
        console.info(`[whatsapp] messages.upsert type=${type} count=${messages.length}`);
        // Only process genuinely new messages delivered in real time.
        // `type === "notify"` means the message was received live while the
        // socket was connected. `type === "append"` means the message is being
        // appended to chat history (e.g. history sync after linking) and must
        // NOT trigger a bot response.
        if (type !== "notify") return;

        for (const msg of messages) {
          try {
            await handleInboundMessage(msg);
          } catch (err) {
            console.error("[whatsapp] failed to handle inbound message", err);
          }
        }
      },
    );

    listenersAttached = true;
    syncToGlobal();
  });
}

// ---------------------------------------------------------------------------
// Inbound message parsing
// ---------------------------------------------------------------------------

/**
 * Check if a JID is a direct message (1:1 conversation), not a group.
 * Uses official Baileys JID helpers to support both phone-number JIDs
 * (`@s.whatsapp.net`) and LID-based user JIDs (`@lid`).
 */
function isDM(jid: string | undefined): boolean {
  return isPnUser(jid) === true || isLidUser(jid) === true;
}

async function handleInboundMessage(msg: WAMessage): Promise<void> {
  const jid = msg.key.remoteJid;
  if (!jid || !isDM(jid)) return; // DMs only — no groups, broadcasts, etc.

  if (msg.key.fromMe === true) {
    console.info("[whatsapp] ignoring outbound message echo");
    return; // skip our own outbound echoes.
  }
  // NOTE: This means WhatsApp's "Message Yourself" conversation cannot trigger
  // the bot. Test from another WhatsApp account/number.

  // Normalize wrapped messages (ephemeral, view-once, document-with-caption,
  // edited, etc.) to their inner content before inspecting the type.
  const normalizedMessage = normalizeMessageContent(msg.message);
  const type = getContentType(normalizedMessage ?? undefined);
  if (!type || type === "stickerMessage") return; // stickers excluded.
  console.info(`[whatsapp] handling inbound jid=${jid} type=${type}`);

  const parts: UserContent = [];
  let voiceReply = false;

  // Read content from the normalized message, not the raw msg.message.
  const content = normalizedMessage;

  switch (type) {
    case "conversation":
    case "extendedTextMessage": {
      const text =
        type === "conversation"
          ? content?.conversation
          : content?.extendedTextMessage?.text;
      if (!text) break;

      const command = text.trim().toLowerCase();
      if (command === "/clear" || command === "/reset") {
        if (!capturedFrom) {
          parts.push({ type: "text", text: "The WhatsApp connection is still starting. Please try again." });
          break;
        }

        if (command === "/clear") {
          await capturedFrom(jid).clear();
          await socket?.sendMessage(jid, {
            text: "Conversation history cleared. I still retain this session's state.",
          });
        } else {
          await capturedFrom(jid).reset({ reason: "User requested a fresh WhatsApp conversation" });
          await socket?.sendMessage(jid, { text: "Conversation reset. Your next message starts fresh." });
        }
        return;
      }

      // If there's a pending HITL request for this JID, resolve it via
      // respond() instead of starting a new turn. This routes the user's
      // numeric or freeform reply to the correct requestId.
      const resolved = await tryResolveHITL(jid, text);
      if (resolved) return;

      parts.push({ type: "text", text });
      break;
    }

    case "audioMessage": {
      // WhatsApp clients do not always set `ptt` consistently. Treat every
      // inbound audio message as a request for a voice reply; otherwise a
      // genuine voice note can silently receive a text response.
      voiceReply = true;
      console.info(
        `[whatsapp] inbound audio ptt=${content?.audioMessage?.ptt === true} mime=${content?.audioMessage?.mimetype ?? "audio/ogg"}`,
      );
      // Pass socket.updateMediaMessage as reuploadRequest so Baileys can request
      // a re-upload if the media URL has expired.
      const audio = socket
        ? await downloadMediaMessage(msg, "buffer", {}, {
            reuploadRequest: socket.updateMediaMessage,
            logger: socket.logger,
          })
        : await downloadMediaMessage(msg, "buffer", {});
      if (audio) {
        const mediaType = content?.audioMessage?.mimetype ?? "audio/ogg";
        try {
          const transcript = await transcribeVoiceNote(audio as Buffer, mediaType);
          if (transcript) {
            console.info(`[whatsapp] Deepgram transcribed voice note length=${transcript.length}`);
            parts.push({ type: "text", text: `Voice message transcript: ${transcript}` });
          } else {
            parts.push({ type: "text", text: "Voice message received, but no speech was detected." });
          }
        } catch (err) {
          console.error("[whatsapp] Deepgram transcription failed; forwarding raw audio", err);
          parts.push({ type: "file", data: audio as Buffer, mediaType });
        }
      }
      break;
    }

    case "imageMessage": {
      const img = socket
        ? await downloadMediaMessage(msg, "buffer", {}, {
            reuploadRequest: socket.updateMediaMessage,
            logger: socket.logger,
          })
        : await downloadMediaMessage(msg, "buffer", {});
      const caption = content?.imageMessage?.caption;
      if (caption) parts.push({ type: "text", text: caption });
      if (img) {
        parts.push({
          type: "file",
          data: img as Buffer,
          mediaType: content?.imageMessage?.mimetype ?? "image/jpeg",
        });
      }
      break;
    }

    case "videoMessage": {
      const vid = socket
        ? await downloadMediaMessage(msg, "buffer", {}, {
            reuploadRequest: socket.updateMediaMessage,
            logger: socket.logger,
          })
        : await downloadMediaMessage(msg, "buffer", {});
      const caption = content?.videoMessage?.caption;
      if (caption) parts.push({ type: "text", text: caption });
      if (vid) {
        parts.push({
          type: "file",
          data: vid as Buffer,
          mediaType: content?.videoMessage?.mimetype ?? "video/mp4",
        });
      }
      break;
    }

    case "locationMessage": {
      const loc = content?.locationMessage;
      if (loc) {
        // Passed through as text; the agent's own tool handles reverse-geocoding.
        parts.push({
          type: "text",
          text: `Location received: ${loc.degreesLatitude}, ${loc.degreesLongitude}`,
        });
      }
      break;
    }

    default:
      return; // Unsupported media type — ignore.
  }

  if (parts.length === 0) return;

  await dispatchInbound(jid, parts, {
    jid,
    lastInboundKey: msg.key,
    voiceReply,
  });
}

// ---------------------------------------------------------------------------
// HITL resolution
// ---------------------------------------------------------------------------

/**
 * Try to resolve an inbound text message as a HITL response.
 *
 * Two-tier strategy:
 *
 * 1. **Fast path** — Check the module-level `pendingHITL` map (mirrored from
 *    the durable `state.pendingInput` by the `input.requested` handler). If
 *    found, resolve explicitly via `from(jid).respond()`:
 *    - Option-based: "1", "2", "3" map to the corresponding `optionId`.
 *      Invalid numbers are rejected cleanly with a re-rendered prompt.
 *    - Freeform: the user's text is sent as the `text` field.
 *
 * 2. **Fallback (restart-safe)** — If `pendingHITL` is empty (e.g. after a
 *    process restart where the in-memory map was lost), return `false` so the
 *    text is dispatched via `from(jid).send(text)`. Eve's built-in auto-matching
 *    then resolves the reply against the durable `state.pendingInput`:
 *    "A follow-up whose text matches an option ID, option label, or numeric
 *    option index resolves automatically" (see eve HITL docs). This is the
 *    eve-supported mechanism — no custom protocol needed.
 *
 * Returns `true` if the message was consumed as a HITL response, `false`
 * if it should be processed as a normal message (fallback to eve auto-matching).
 */
async function tryResolveHITL(jid: string, text: string): Promise<boolean> {
  const pending = pendingHITL.get(jid);
  if (!pending) {
    // Fast path miss — either no HITL is pending, or the process restarted
    // and the in-memory map is empty. Fall through to send() and let eve's
    // built-in auto-matching handle it against durable state.
    return false;
  }

  const trimmed = text.trim();

  // Option-based prompt: map a number to the corresponding optionId.
  if (pending.options.length > 0) {
    const num = Number(trimmed);
    if (Number.isInteger(num) && num >= 1 && num <= pending.options.length) {
      const option = pending.options[num - 1];
      // Clear the pending state before responding so duplicate replies
      // don't double-resolve.
      pendingHITL.delete(jid);
      syncToGlobal();
      if (capturedFrom) {
        await capturedFrom(jid).respond(
          [{ requestId: pending.requestId, optionId: option.id }],
          { auth: null },
        );
      }
      return true;
    }

    // Invalid number on an option-only prompt (no freeform allowed).
    if (!pending.allowFreeform) {
      // Re-render the prompt so the user knows to try again.
      const sock = getGlobal().socket;
      if (sock) {
        await sock.sendMessage(jid, {
          text: `Invalid choice. Please reply with a number 1–${pending.options.length}.`,
        });
      }
      return true; // Consumed — don't start a new turn.
    }
  }

  // Freeform response (either a freeform-only prompt, or an option prompt
  // that allows freeform text).
  pendingHITL.delete(jid);
  syncToGlobal();
  if (capturedFrom) {
    await capturedFrom(jid).respond(
      [{ requestId: pending.requestId, text: trimmed }],
      { auth: null },
    );
  }
  return true;
}

// ---------------------------------------------------------------------------
// Outbound delivery helpers
// ---------------------------------------------------------------------------

async function sendText(sock: WASocket, jid: string, text: string): Promise<void> {
  console.info(`[whatsapp] sending text reply to jid=${jid} length=${text.length}`);
  const MAX = 65536;
  if (text.length <= MAX) {
    await sock.sendMessage(jid, { text });
    return;
  }
  for (let i = 0; i < text.length; i += MAX) {
    await sock.sendMessage(jid, { text: text.slice(i, i + MAX) });
  }
}

/** Send a Cartesia-generated WhatsApp voice-note reply. */
async function sendVoiceNote(sock: WASocket, jid: string, text: string): Promise<void> {
  try {
    const cartesia = new Cartesia({ apiKey: getRequiredEnv("CARTESIA_API_KEY") });
    const response = await cartesia.tts.generate({
      model_id: CARTESIA_MODEL,
      transcript: text,
      voice: { id: getRequiredEnv("CARTESIA_VOICE_ID") },
      language: "en",
      output_format: { container: "mp3", sample_rate: 44100, bit_rate: 128000 },
    });
    const mp3 = Buffer.from(await response.arrayBuffer());
    const audio = await mp3ToWhatsAppVoiceNote(mp3);
    console.info(`[whatsapp] sending Cartesia voice reply to jid=${jid} oggOpusBytes=${audio.length}`);
    await sock.sendMessage(jid, { audio, mimetype: "audio/ogg; codecs=opus", ptt: true });
  } catch (err) {
    console.error("[whatsapp] Cartesia TTS failed; falling back to text", err);
    await sendText(sock, jid, text);
  }
}

async function markRead(sock: WASocket, key: WAMessageKey): Promise<void> {
  try {
    await sock.readMessages([key]);
  } catch {
    // Non-critical.
  }
}

async function sendTyping(sock: WASocket, jid: string): Promise<void> {
  try {
    await sock.sendPresenceUpdate("composing", jid);
  } catch {
    // Non-critical.
  }
}

async function sendPaused(sock: WASocket, jid: string): Promise<void> {
  try {
    await sock.sendPresenceUpdate("paused", jid);
  } catch {
    // Non-critical.
  }
}

// ---------------------------------------------------------------------------
// HITL rendering
// ---------------------------------------------------------------------------

function renderHitlAsText(
  prompt: string,
  options: { id: string; label: string }[],
): string {
  const lines = options.map((opt, i) => `${i + 1}. ${opt.label}`);
  return `${prompt}\n\n${lines.join("\n")}\n\nReply with a number.`;
}

// ---------------------------------------------------------------------------
// Channel definition
// ---------------------------------------------------------------------------

export default defineChannel<WhatsAppState, WhatsAppChannelContext>({
  kindHint: "whatsapp",

  // Queue: WhatsApp users expect ordered replies; finish a turn before the next.
  turnPolicy: "queue",

  state: {
    jid: "",
    voiceReply: false,
  },

  metadata(state) {
    return {
      jid: state.jid,
      voiceReply: state.voiceReply,
      hasPendingInput: state.pendingInput !== undefined,
    };
  },

  context(state, _session) {
    // Make the module-level socket and captured `from` available to every
    // event handler via the channel context. Reading from the global singleton
    // ensures we get the live socket even after a hot reload.
    const g = getGlobal();
    return { state, socket: g.socket, from: g.capturedFrom };
  },

  routes: [
    /**
     * Bootstrap route: captures `from` into module scope and drains any queued
     * messages. Called automatically when the Baileys socket connects (see
     * `bootstrapFrom`), and safe to call manually.
     */
    POST("/whatsapp/bootstrap", async (_req, { from, resolveSession }) => {
      if (!capturedFrom) {
        capturedFrom = from as unknown as CapturedFrom;
        capturedResolveSession = resolveSession as unknown as CapturedResolveSession;
        syncToGlobal();
        console.info("[whatsapp] dispatcher captured, draining queue");
      }

      // Drain queued messages that arrived before `from` was available.
      while (messageQueue.length > 0) {
        const queued = messageQueue.shift()!;
        syncToGlobal();
        try {
          queueReplyMode(queued.jid, queued.state.voiceReply);
          await from(queued.jid).send(queued.content, { auth: null, state: queued.state });
        } catch (err) {
          console.error("[whatsapp] failed to drain queued message", err);
        }
      }

      return new Response("ok");
    }),
  ],

  // Proactive session entry: schedules and cross-channel sends arrive here.
  async receive(input, { from }) {
    // Capture `from` for socket-driven inbound dispatch.
    if (!capturedFrom) {
      capturedFrom = from as unknown as CapturedFrom;
      syncToGlobal();
    }

    const jid =
      typeof input.target === "object" && input.target !== null && "threadId" in input.target
        ? String((input.target as { threadId: string }).threadId)
        : String(input.target);

    return from(jid).send(input.message, {
      auth: input.auth,
      state: { jid, voiceReply: false },
    });
  },

  events: {
    async "turn.started"(_event, channel) {
      const { state, socket: sock } = channel;
      const modes = replyModes.get(state.jid);
      const voiceReply = modes?.shift() ?? state.voiceReply;
      if (modes && modes.length === 0) replyModes.delete(state.jid);
      activeReplyModes.set(state.jid, voiceReply);
      syncToGlobal();
      console.info(
        `[whatsapp] agent turn started jid=${state.jid || "(missing)"} socket=${Boolean(sock)} voiceReply=${voiceReply}`,
      );
      if (sock && state.jid) await sendTyping(sock, state.jid);
    },

    async "actions.requested"(_event, channel) {
      const { state, socket: sock } = channel;
      if (sock && state.jid) await sendTyping(sock, state.jid);
    },

    async "input.requested"(event, channel) {
      const { state, socket: sock } = channel;
      if (!sock || !state.jid) return;

      for (const request of event.requests ?? []) {
        const options = request.options ?? [];
        if (options.length > 0) {
          // Option-based prompt: store in durable channel state so the inbound
          // handler can resolve the numeric reply against the correct requestId.
          state.pendingInput = {
            requestId: request.requestId,
            options,
            allowFreeform: request.allowFreeform ?? false,
          };
          // Also mirror to the module-level map so the inbound handler
          // (which runs outside eve's event system) can route the reply
          // via respond() instead of send().
          pendingHITL.set(state.jid, {
            requestId: request.requestId,
            options,
            allowFreeform: request.allowFreeform ?? false,
          });
          syncToGlobal();
          const text = renderHitlAsText(request.prompt ?? "Choose an option:", options);
          await sock.sendMessage(state.jid, { text });
        } else {
          // Freeform question — no numbered options, but we still store the
          // requestId so the next inbound message can be routed via respond()
          // instead of starting a new turn.
          state.pendingInput = {
            requestId: request.requestId,
            options: [],
            allowFreeform: true,
          };
          pendingHITL.set(state.jid, {
            requestId: request.requestId,
            options: [],
            allowFreeform: true,
          });
          syncToGlobal();
          await sock.sendMessage(state.jid, { text: request.prompt ?? "" });
        }
      }
    },

    async "message.completed"(event, channel) {
      const { state, socket: sock } = channel;
      console.info(
        `[whatsapp] message.completed jid=${state.jid || "(missing)"} socket=${Boolean(sock)} finish=${event.finishReason ?? "unknown"} hasMessage=${Boolean(event.message)}`,
      );
      if (!sock || !state.jid) return;

      // Skip interim assistant text emitted before a tool call.
      if (event.finishReason === "tool-calls" || !event.message) return;

      const voiceReply = activeReplyModes.get(state.jid) ?? state.voiceReply;
      console.info(`[whatsapp] delivering reply jid=${state.jid} voiceReply=${voiceReply}`);
      if (voiceReply) {
        await sendVoiceNote(sock, state.jid, event.message);
      } else {
        await sendText(sock, state.jid, event.message);
      }

      if (state.lastInboundKey) await markRead(sock, state.lastInboundKey);
    },

    async "session.waiting"(_event, channel) {
      const { state, socket: sock } = channel;
      activeReplyModes.delete(state.jid);
      syncToGlobal();
      if (sock && state.jid) await sendPaused(sock, state.jid);
    },

    async "turn.failed"(event, channel) {
      const { state, socket: sock } = channel;
      console.error(`[whatsapp] agent turn failed jid=${state.jid || "(missing)"}`, event);
      if (!sock || !state.jid) return;
      const message = event.message ?? "Something went wrong. Please try again.";
      await sendText(sock, state.jid, message);
    },

    async "session.failed"(event, channel) {
      const { state, socket: sock } = channel;
      console.error(`[whatsapp] session failed jid=${state.jid || "(missing)"}`, event);
      if (!sock || !state.jid) return;
      const detailMessage = event.details?.message;
      const message =
        typeof detailMessage === "string" ? detailMessage : "This session could not recover.";
      await sendText(sock, state.jid, message);
    },
  } satisfies ChannelEvents<WhatsAppChannelContext>,
});

// Start the socket as soon as the module loads. The HTTP server starts shortly
// after; when the socket connects, it calls the bootstrap route to capture
// `from`. Messages that arrive before then are queued and drained on bootstrap.
//
// LIMITATION: eve does not provide a background-channel lifecycle API for
// push-based (non-HTTP) transports. This module-scope call is the pragmatic
// workaround. The globalThis singleton guard above prevents duplicate sockets
// during `eve dev` hot reload. During `eve build`, the module is imported for
// route discovery but does not start a socket because `connectSocket` only
// creates a WebSocket when invoked — the import itself is side-effect-free
// except for this call. If `eve build` causes issues, guard with an env check:
//   if (process.env.EVE_COMMAND !== "build") wireSocketListener();
// For now, `connectSocket` is safe to call during build — it connects to
// WhatsApp but does not interfere with the build output.
wireSocketListener();

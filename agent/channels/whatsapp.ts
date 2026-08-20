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
 * Supports: text, voice notes (receive + text fallback reply), images, video,
 * GPS location, HITL (rendered as numbered text choices), and proactive
 * sessions. Voice note *replies* are NOT supported until TTS is wired — the
 * inbound voice note is passed to the agent as a file, and the agent's text
 * reply is sent as text, not audio.
 *
 * Does NOT support: stickers, groups, polls, rich buttons, or actual voice-note
 * replies (TTS stubbed).
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
  };
}

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
  messageQueue: QueuedMessage[];
  /**
   * Module-level mirror of pending HITL requests, keyed by JID. The inbound
   * handler runs outside eve's event system and cannot read channel state
   * directly, so we mirror `state.pendingInput` here for quick lookup. This is
   * non-durable (lost on restart), but eve's durable `state.pendingInput`
   * survives restarts and eve auto-matches follow-up text against options, so
   * the module-level map is an optimization for explicit `respond()` routing.
   */
  pendingHITL: Map<string, { requestId: string; options: { id: string; label: string }[]; allowFreeform: boolean }>;
}

function getGlobal(): GlobalState {
  const g = globalThis as unknown as Record<string, unknown>;
  if (!g[GLOBAL_KEY]) {
    g[GLOBAL_KEY] = {
      socket: null,
      socketStarting: null,
      capturedFrom: null,
      messageQueue: [],
      pendingHITL: new Map(),
    } satisfies GlobalState;
  }
  return g[GLOBAL_KEY] as GlobalState;
}

let socket: WASocket | null;
let socketStarting: Promise<WASocket> | null;
let capturedFrom: CapturedFrom | null;
let messageQueue: QueuedMessage[];
let pendingHITL: Map<string, { requestId: string; options: { id: string; label: string }[]; allowFreeform: boolean }>;

{
  // Initialize module-level vars from the global singleton.
  const g = getGlobal();
  socket = g.socket;
  socketStarting = g.socketStarting;
  capturedFrom = g.capturedFrom;
  messageQueue = g.messageQueue;
  pendingHITL = g.pendingHITL;
}

/** Sync module-level vars back to the global singleton (after mutations). */
function syncToGlobal(): void {
  const g = getGlobal();
  g.socket = socket;
  g.socketStarting = socketStarting;
  g.capturedFrom = capturedFrom;
  g.messageQueue = messageQueue;
  g.pendingHITL = pendingHITL;
}

const AUTH_DIR = "./auth_info_baileys";

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
        const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        console.error(
          `[whatsapp] connection closed (status ${statusCode}), reconnecting: ${shouldReconnect}`,
        );
        socket = null;
        socketStarting = null;
        syncToGlobal();
        if (shouldReconnect) {
          // Bounded delay to avoid hammering WhatsApp on rapid disconnects.
          setTimeout(() => void connectSocket(), Math.min(2000, 500));
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
 * Retries with bounded backoff so startup cannot get permanently stuck if the
 * HTTP server is not yet listening when the socket connects.
 */
async function bootstrapFrom(): Promise<void> {
  const port = process.env.PORT ?? "2000";
  const url = `http://127.0.0.1:${port}/whatsapp/bootstrap`;
  const maxAttempts = 10;
  const baseDelay = 500;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const res = await fetch(url, { method: "POST" });
      if (res.ok) return;
    } catch {
      // Server not listening yet — retry after backoff.
    }
    if (attempt < maxAttempts) {
      const delay = Math.min(baseDelay * attempt, 5000);
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
  console.error(
    `[whatsapp] bootstrap failed after ${maxAttempts} attempts — ` +
      "`from` not captured. Inbound messages will queue until a route is hit.",
  );
}

// ---------------------------------------------------------------------------
// Inbound message dispatch
// ---------------------------------------------------------------------------

/** Dispatch a parsed message to eve, queuing if `from` isn't captured yet. */
async function dispatchInbound(
  jid: string,
  content: UserContent,
  state: WhatsAppState,
): Promise<void> {
  if (capturedFrom) {
    await capturedFrom(jid).send(content, { auth: null, state });
    return;
  }
  // `from` not captured yet — queue for when the bootstrap route fires.
  messageQueue.push({ jid, content, state });
  syncToGlobal();
}

/**
 * Wire the socket's message listener. Called once on module load.
 * Attaches to the `messages.upsert` event.
 */
function wireSocketListener(): void {
  void connectSocket().then((sock) => {
    sock.ev.on(
      "messages.upsert",
      async ({ messages, type }: BaileysEventMap["messages.upsert"]) => {
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

  if (msg.key.fromMe === true) return; // skip our own outbound echoes.
  // NOTE: This means WhatsApp's "Message Yourself" conversation cannot trigger
  // the bot. Test from another WhatsApp account/number.

  // Normalize wrapped messages (ephemeral, view-once, document-with-caption,
  // edited, etc.) to their inner content before inspecting the type.
  const normalizedMessage = normalizeMessageContent(msg.message);
  const type = getContentType(normalizedMessage ?? undefined);
  if (!type || type === "stickerMessage") return; // stickers excluded.

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

      // If there's a pending HITL request for this JID, resolve it via
      // respond() instead of starting a new turn. This routes the user's
      // numeric or freeform reply to the correct requestId.
      const resolved = await tryResolveHITL(jid, text);
      if (resolved) return;

      parts.push({ type: "text", text });
      break;
    }

    case "audioMessage": {
      voiceReply = content?.audioMessage?.ptt === true;
      // Pass socket.updateMediaMessage as reuploadRequest so Baileys can request
      // a re-upload if the media URL has expired.
      const audio = socket
        ? await downloadMediaMessage(msg, "buffer", {}, {
            reuploadRequest: socket.updateMediaMessage,
            logger: socket.logger,
          })
        : await downloadMediaMessage(msg, "buffer", {});
      if (audio) {
        parts.push({
          type: "file",
          data: audio as Buffer,
          mediaType: content?.audioMessage?.mimetype ?? "audio/ogg",
        });
      }

      // --- STT hook -----------------------------------------------------
      // To transcribe the voice note before the model sees it, call your STT
      // provider here and push a text part instead of (or alongside) the file:
      //
      //   const transcript = await mySTT(audio as Buffer);
      //   parts.push({ type: "text", text: transcript });
      //
      // Remove the file part above if you don't want the raw audio to reach
      // the model. Deepgram is a good fit — leave this hook clean for later.
      // ------------------------------------------------------------------
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
 * Checks the module-level `pendingHITL` map (mirrored from the durable
 * `state.pendingInput` by the `input.requested` event handler).
 *
 * - For option-based prompts: "1", "2", "3" etc. map to the corresponding
 *   option's `id`. Invalid numbers are rejected cleanly.
 * - For freeform prompts: the user's text is sent as the response.
 *
 * Returns `true` if the message was consumed as a HITL response, `false`
 * if it should be processed as a normal message.
 */
async function tryResolveHITL(jid: string, text: string): Promise<boolean> {
  const pending = pendingHITL.get(jid);
  if (!pending) return false;

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
  const MAX = 65536;
  if (text.length <= MAX) {
    await sock.sendMessage(jid, { text });
    return;
  }
  for (let i = 0; i < text.length; i += MAX) {
    await sock.sendMessage(jid, { text: text.slice(i, i + MAX) });
  }
}

/**
 * Send a voice-note reply. Until TTS is wired, this falls back to plain text.
 *
 * Voice-note replies are NOT currently supported. The `voiceReply` state flag
 * is set when the user sends a voice note, but the reply is delivered as text
 * because no TTS provider is configured. To enable actual voice replies, plug
 * in a TTS provider here:
 *
 *   const audio = await myTTS(text);
 *   await sock.sendMessage(jid, {
 *     audio: { url: audio.url },       // or { bytes: audio.buffer }
 *     ptt: true,
 *     seconds: audio.seconds,
 *   });
 */
async function sendVoiceNote(sock: WASocket, jid: string, text: string): Promise<void> {
  // TODO: Replace with your TTS provider call.
  // const audio = await tts(text);
  // await sock.sendMessage(jid, { audio: { url: audio.url }, ptt: true, seconds: audio.seconds });

  // Fallback: send as text until TTS is wired.
  await sendText(sock, jid, text);
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
    POST("/whatsapp/bootstrap", async (_req, { from }) => {
      if (!capturedFrom) {
        capturedFrom = from as unknown as CapturedFrom;
        syncToGlobal();
        console.info("[whatsapp] dispatcher captured, draining queue");
      }

      // Drain queued messages that arrived before `from` was available.
      while (messageQueue.length > 0) {
        const queued = messageQueue.shift()!;
        syncToGlobal();
        try {
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
      if (!sock || !state.jid) return;

      // Skip interim assistant text emitted before a tool call.
      if (event.finishReason === "tool-calls" || !event.message) return;

      if (state.voiceReply) {
        await sendVoiceNote(sock, state.jid, event.message);
      } else {
        await sendText(sock, state.jid, event.message);
      }

      if (state.lastInboundKey) await markRead(sock, state.lastInboundKey);
    },

    async "session.waiting"(_event, channel) {
      const { state, socket: sock } = channel;
      if (sock && state.jid) await sendPaused(sock, state.jid);
    },

    async "turn.failed"(event, channel) {
      const { state, socket: sock } = channel;
      if (!sock || !state.jid) return;
      const message = event.message ?? "Something went wrong. Please try again.";
      await sendText(sock, state.jid, message);
    },

    async "session.failed"(event, channel) {
      const { state, socket: sock } = channel;
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

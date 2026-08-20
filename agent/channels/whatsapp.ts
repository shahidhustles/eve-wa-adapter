import { defineChannel, POST, type ChannelEvents } from "eve/channels";
import {
  makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  Browsers,
  downloadMediaMessage,
  getContentType,
  type WASocket,
  type WAMessage,
  type WAMessageKey,
  type BaileysEventMap,
  DisconnectReason,
} from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import type { UserContent } from "ai";

/**
 * eve custom channel: WhatsApp via Baileys (unofficial WhatsApp Web API).
 *
 * Self-hosted only. Baileys holds a long-lived WebSocket to WhatsApp's servers.
 * Inbound messages arrive from the socket, not from HTTP webhooks, so this
 * channel captures eve's `from()` dispatcher from a bootstrap HTTP route and
 * reuses it for all subsequent socket-driven dispatches.
 *
 * Supports: text, voice notes (send + receive), images, video, GPS location,
 * HITL (rendered as numbered text choices), and proactive sessions.
 * Does NOT support: stickers, groups, polls, or rich buttons.
 *
 * First run prints a QR code to the terminal. Scan it via WhatsApp -> Linked
 * Devices. Credentials persist to ./auth_info_baileys and are reused on restart.
 *
 * @see https://chat-sdk.dev/adapters/community/baileys
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
  /** Pending HITL request id -> options, so a numeric reply can resolve it. */
  pendingInput?: {
    requestId: string;
    options: { id: string; label: string }[];
  };
}

/** Per-step channel context handed to every event handler. */
interface WhatsAppChannelContext {
  state: WhatsAppState;
  socket: WASocket | null;
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

let socket: WASocket | null = null;
let socketStarting: Promise<WASocket> | null = null;

/** Captured `from` dispatcher — set by the bootstrap route on first HTTP hit. */
let capturedFrom: CapturedFrom | null = null;

/** Messages that arrived before `from` was captured. Drained on bootstrap. */
const messageQueue: QueuedMessage[] = [];

const AUTH_DIR = "./auth_info_baileys";

// ---------------------------------------------------------------------------
// Baileys socket lifecycle
// ---------------------------------------------------------------------------

async function connectSocket(): Promise<WASocket> {
  if (socket) return socket;
  if (socketStarting) return socketStarting;

  socketStarting = (async () => {
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
    const { version } = await fetchLatestBaileysVersion();

    const sock = makeWASocket({
      version,
      browser: Browsers.ubuntu("eve-whatsapp"),
      auth: state,
      printQRInTerminal: true,
    });

    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("connection.update", (update) => {
      const { connection, lastDisconnect } = update;
      if (connection === "close") {
        const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        console.error(
          `[whatsapp] connection closed (status ${statusCode}), reconnecting: ${shouldReconnect}`,
        );
        socket = null;
        socketStarting = null;
        if (shouldReconnect) void connectSocket();
      } else if (connection === "open") {
        console.info("[whatsapp] connected");
        // Bootstrap: ping our own HTTP route to capture `from` and drain the queue.
        void bootstrapFrom();
      }
    });

    socket = sock;
    return sock;
  })();

  return socketStarting;
}

/**
 * Call the channel's own bootstrap route via localhost HTTP to capture `from`.
 * This bridges the gap between Baileys' push-based socket and eve's
 * HTTP-centric channel model: the route handler has access to `from`, which
 * closes over the stable runtime and can be reused for all socket-driven sends.
 */
async function bootstrapFrom(): Promise<void> {
  const port = process.env.PORT ?? "2000";
  try {
    await fetch(`http://127.0.0.1:${port}/whatsapp/bootstrap`, {
      method: "POST",
    });
  } catch {
    // Server might not be listening yet. The next inbound HTTP request
    // (or a retry) will capture `from`. Messages are queued until then.
  }
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
}

/** Wire the socket's message listener. Called once on module load. */
function wireSocketListener(): void {
  void connectSocket().then((sock) => {
    sock.ev.on("messages.upsert", async ({ messages }: BaileysEventMap["messages.upsert"]) => {
      for (const msg of messages) {
        try {
          await handleInboundMessage(msg);
        } catch (err) {
          console.error("[whatsapp] failed to handle inbound message", err);
        }
      }
    });
  });
}

// ---------------------------------------------------------------------------
// Inbound message parsing
// ---------------------------------------------------------------------------

function isDM(jid: string): boolean {
  return jid.endsWith("@s.whatsapp.net");
}

async function handleInboundMessage(msg: WAMessage): Promise<void> {
  const jid = msg.key.remoteJid;
  if (!jid || !isDM(jid)) return; // DMs only — no groups.

  if (msg.key.fromMe === true) return; // skip our own outbound echoes.

  // msg.message is `IMessage | null | undefined`; getContentType expects non-null.
  const type = getContentType(msg.message ?? undefined);
  if (!type || type === "stickerMessage") return; // stickers excluded.

  const parts: UserContent = [];
  let voiceReply = false;

  switch (type) {
    case "conversation":
    case "extendedTextMessage": {
      const text =
        type === "conversation"
          ? msg.message?.conversation
          : msg.message?.extendedTextMessage?.text;
      if (text) parts.push({ type: "text", text });
      break;
    }

    case "audioMessage": {
      voiceReply = msg.message?.audioMessage?.ptt === true;
      // ctx (4th arg) omitted — it's optional. Pass sock.updateMediaMessage if
      // you need re-upload on expired media URLs:
      //   downloadMediaMessage(msg, "buffer", {}, { reuploadRequest: sock.updateMediaMessage, logger })
      const audio = await downloadMediaMessage(msg, "buffer", {});
      if (audio) {
        parts.push({
          type: "file",
          data: audio as Buffer,
          mediaType: "audio/ogg",
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
      // the model.
      // ------------------------------------------------------------------
      break;
    }

    case "imageMessage": {
      const img = await downloadMediaMessage(msg, "buffer", {});
      const caption = msg.message?.imageMessage?.caption;
      if (caption) parts.push({ type: "text", text: caption });
      if (img) {
        parts.push({
          type: "file",
          data: img as Buffer,
          mediaType: msg.message?.imageMessage?.mimetype ?? "image/jpeg",
        });
      }
      break;
    }

    case "videoMessage": {
      const vid = await downloadMediaMessage(msg, "buffer", {});
      const caption = msg.message?.videoMessage?.caption;
      if (caption) parts.push({ type: "text", text: caption });
      if (vid) {
        parts.push({
          type: "file",
          data: vid as Buffer,
          mediaType: msg.message?.videoMessage?.mimetype ?? "video/mp4",
        });
      }
      break;
    }

    case "locationMessage": {
      const loc = msg.message?.locationMessage;
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
 * Send a voice-note reply. Takes the agent's text output, synthesizes audio,
 * and sends it as a WhatsApp PTT (push-to-talk) message.
 *
 * --- TTS hook -------------------------------------------------------------
 * This function is where you convert `text` to audio bytes. Plug in your TTS
 * provider (OpenAI tts-1, ElevenLabs, Google TTS, etc.) and return the audio
 * buffer + duration. Until you wire this in, the function falls back to text.
 *
 *   const audio = await myTTS(text);
 *   await sock.sendMessage(jid, {
 *     audio: { url: audio.url },       // or { bytes: audio.buffer }
 *     ptt: true,
 *     seconds: audio.seconds,
 *   });
 * -------------------------------------------------------------------------
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

  context(state) {
    return { state, socket };
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
        console.info("[whatsapp] dispatcher captured, draining queue");
      }

      // Drain queued messages that arrived before `from` was available.
      while (messageQueue.length > 0) {
        const queued = messageQueue.shift()!;
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
          state.pendingInput = { requestId: request.requestId, options };
          const text = renderHitlAsText(request.prompt ?? "Choose an option:", options);
          await sock.sendMessage(state.jid, { text });
        } else {
          // Freeform question — no options to render as a numbered list.
          state.pendingInput = undefined;
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
wireSocketListener();

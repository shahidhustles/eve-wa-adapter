# Architecture: WhatsApp (Baileys) Channel for eve

This document explains how the WhatsApp channel works internally: why it
diverges from eve's other platform channels, how it bridges Baileys'
push-based socket to eve's session model, and how each feature is wired.

## Context

eve ships first-class channels for Slack, Discord, Telegram, Teams, Twilio,
GitHub, and Linear. Each is webhook-based: the platform POSTs inbound events
to an HTTP route, the route handler calls `from(address).send(...)` to start
or resume an eve session, and eve's `events` map delivers the agent's reply
back through the platform API.

eve also ships a [Chat SDK channel bridge](https://eve.dev/docs/channels/chat-sdk)
(`chatSdkChannel`) that accepts any [Vercel Chat SDK](https://chat-sdk.dev)
adapter. The Baileys community adapter
([`chat-adapter-baileys`](https://chat-sdk.dev/adapters/community/baileys))
exists for exactly this use case — but the bridge is **not usable with Baileys**.
`chatSdkChannel` throws when `send()` is called outside an active HTTP webhook
context (it reads an `ActiveWebhookKey` from async-context storage that only
exists inside a route handler). Every adapter the bridge supports delivers
inbound messages as HTTP webhooks. Baileys does the opposite: it holds a
long-lived outbound WebSocket to WhatsApp's servers and pushes messages
asynchronously via `sock.ev.on("messages.upsert", ...)`.

So this channel is a **custom eve channel** (`defineChannel`) that owns the
Baileys socket directly, rather than going through the Chat SDK bridge.

## The transport mismatch and how we solve it

|                      | Webhook channels (Telegram, Slack…) | This channel (Baileys)             |
| -------------------- | ------------------------------------ | ---------------------------------- |
| Inbound transport    | Platform → HTTP POST to eve         | WhatsApp → WebSocket → Baileys ev  |
| `from()` availability| Inside each HTTP route handler      | Only inside route handlers / `receive` |
| Request lifecycle    | One request per message              | No request; push from socket      |

eve's `from(address)` dispatcher — the function that starts/resumes a session —
is only constructed inside route handlers and the `receive` hook (see
`createChannelOperations` in `node_modules/eve/dist/src/channel/channel-operations.js`).
It closes over the stable `runtime` object, so once captured, it can be reused
for any dispatch. But it is not available to background socket events.

The solution is a **bootstrap route**:

1. The module loads and `connectSocket()` starts the Baileys WebSocket.
2. When the socket opens, `bootstrapFrom()` sends an HTTP `POST` to the
   channel's own `/whatsapp/bootstrap` route (via `127.0.0.1`).
3. The bootstrap route handler receives `from` as a route argument, captures
   it into a module-level `capturedFrom` variable, and drains any messages
   that arrived while `from` was unavailable.
4. From that point on, the `messages.upsert` socket listener calls
   `capturedFrom(jid).send(...)` for every inbound message.

Messages that arrive in the gap between socket connect and `from` capture are
queued in `messageQueue` and drained when the bootstrap route fires.

```
 ┌─────────────┐  WebSocket   ┌──────────────┐
 │  WhatsApp    │ ───────────► │   Baileys     │
 │   servers    │              │   socket      │
 └─────────────┘              └──────┬───────┘
                                     │ messages.upsert
                                     ▼
                            ┌────────────────┐
                            │ handleInbound   │  parse → UserContent
                            │ Message()       │
                            └──────┬─────────┘
                                   │
                     capturedFrom? │
                     ┌─────────────┴─────────────┐
                     ▼                            ▼
              capturedFrom(jid).send()     messageQueue (wait)
                     │
                     ▼
              ┌──────────────────────────────────────┐
              │  eve runtime → agent → model         │
              │  session dispatch (durable, queued)  │
              └──────────────────┬───────────────────┘
                                 │ events
                                 ▼
              ┌──────────────────────────────────────┐
              │  message.completed → sendText()      │
              │                      → sendVoiceNote()│
              │  turn.started → sendTyping()         │
              │  session.waiting → sendPaused()     │
              │  input.requested → renderHitlAsText()│
              └──────────────────┬───────────────────┘
                                 │
                                 ▼
                         sock.sendMessage(jid, ...)
```

## Component walkthrough

### Module-level singletons

```ts
let socket: WASocket | null = null;           // the Baileys socket
let socketStarting: Promise<WASocket> | null; // guards against double-connect
let capturedFrom: CapturedFrom | null = null;  // the captured eve dispatcher
const messageQueue: QueuedMessage[] = [];      // buffer before `from` is ready
```

`socket` and `capturedFrom` are module-scoped because the channel file is a
singleton: eve loads `agent/channels/whatsapp.ts` once per process. A
`globalThis` guard is recommended if `eve dev` hot-reloads the module, to
avoid duplicate sockets — not implemented yet but noted as a known risk.

### Socket lifecycle (`connectSocket`)

- `useMultiFileAuthState("./auth_info_baileys")` loads existing credentials
  or prepares an empty auth store. First run has no credentials.
- `fetchLatestBaileysVersion()` fetches the current WhatsApp Web protocol
  version. If omitted, Baileys uses a baked-in version that may be stale.
- `makeWASocket({ ..., printQRInTerminal: true })` prints the QR to the
  terminal on first connect. After scanning via WhatsApp → Linked Devices,
  credentials are saved to `./auth_info_baileys/` and reused on restart.
- `connection.update` handler: on `close`, checks the `DisconnectReason`.
  If the account was not `loggedOut`, it reconnects. If `loggedOut`, it
  stays disconnected (the session was killed on the phone).
- On `open`, calls `bootstrapFrom()` to capture `from`.

### Inbound parsing (`handleInboundMessage`)

Filters first, then dispatches by content type:

| Filter                | Rule                                                        |
| --------------------- | ---------------------------------------------------------- |
| DMs only              | `jid.endsWith("@s.whatsapp.net")` — skips `@g.us` groups  |
| Skip own messages     | `msg.key.fromMe === true` (echo of our outbound)           |
| Skip stickers         | `type === "stickerMessage"`                                |

Then a `switch` on `getContentType(msg.message)`:

| Type               | Handling                                                        |
| ------------------ | --------------------------------------------------------------- |
| `conversation` / `extendedTextMessage` | Text part: `{ type: "text", text }`            |
| `audioMessage`     | `downloadMediaMessage(msg, "buffer")` → file part `audio/ogg`. Sets `voiceReply = ptt === true`. **STT hook** here. |
| `imageMessage`     | Download → file part with mimetype. Caption → text part.        |
| `videoMessage`     | Download → file part with mimetype. Caption → text part.        |
| `locationMessage`  | Text part: `"Location received: lat, lon"` (agent tool handles) |
| default            | Ignored (stickers already filtered above)                      |

All parts are assembled into a `UserContent` array (AI SDK format) and
passed to `dispatchInbound()` along with channel state: `{ jid, lastInboundKey, voiceReply }`.

### Outbound delivery (`events` map)

eve emits runtime stream events; the channel's `events` handlers deliver them
to WhatsApp via `sock.sendMessage`:

| Event              | Action                                                        |
| ------------------ | ------------------------------------------------------------- |
| `turn.started`     | `sock.sendPresenceUpdate("composing", jid)` — typing indicator |
| `actions.requested`| Same typing indicator (model is calling tools)                |
| `message.completed`| If `state.voiceReply` → `sendVoiceNote()`, else `sendText()`. Then `markRead()`. |
| `session.waiting`  | `sock.sendPresenceUpdate("paused", jid)` — stop typing        |
| `input.requested`  | Render HITL options as numbered text (see below)              |
| `turn.failed`      | Send error message as text                                    |
| `session.failed`   | Send recovery message as text                                  |

`message.completed` skips when `finishReason === "tool-calls"` — eve emits
interim assistant text before tool calls that should not be delivered as a
reply.

### Voice notes

Voice reply is **conditional**: the channel only replies as a voice note when
the inbound message was a PTT voice note (`audioMessage.ptt === true`). This
flag is stored in `state.voiceReply` and checked in `message.completed`.

- **Receive:** `downloadMediaMessage` gets the audio buffer → `{ type: "file", data, mediaType: "audio/ogg" }`. The model receives the raw audio. An STT hook (commented) lets you transcribe before the model sees it.
- **Send:** `sendVoiceNote()` is stubbed to fall back to text. A TTS hook (commented) shows where to plug in OpenAI/ElevenLabs/etc. to convert the agent's text reply to audio and send as `{ audio: { url }, ptt: true, seconds }`.

### HITL (human-in-the-loop)

WhatsApp has no native buttons. HITL option requests render as numbered text:

```
Choose a shipping option:

1. Standard (3-5 days)
2. Express (1-2 days)

Reply with a number.
```

The pending request is stored in `state.pendingInput`. When the user replies
with "1" or the label text, a future inbound handler can resolve it via
`from(jid).respond([{ optionId, requestId }])`. (Resolution matching is
designed but the inbound path currently sends as a normal message — see
[Known limitations](#known-limitations).)

### Proactive sessions

The `receive` hook is eve's entry point for cross-channel and schedule-driven
sends:

```ts
await ctx.to(whatsappChannel, { threadId: jid }).send("Digest ready", { auth });
```

`receive` extracts the JID from `input.target.threadId`, captures `from`
(if not already captured), and calls `from(jid).send(...)`.

### Turn policy

`turnPolicy: "queue"` — turns finish in order. WhatsApp users expect ordered
replies. If a new message arrives during an active turn, it waits rather
than steering (canceling the active turn).

## Self-hosting requirement

This channel **cannot run on Vercel serverless** or any short-lived
serverless platform. The Baileys socket must stay connected for the lifetime
of the process. Use:

- `eve start` — long-running Node process (local dev, a VM, or a container)
- A persistent container (Docker, Fly.io, Railway, a VPS)

The `eve dev` TUI also works, but hot module reload may create duplicate
sockets. A `globalThis` singleton guard (not yet implemented) would prevent
this.

## Known limitations

- **HITL resolution:** The inbound path sends numeric replies as normal
  messages rather than resolving them against `state.pendingInput` via
  `respond()`. eve's steering may auto-match option IDs/labels, but explicit
  `respond()` routing is not wired in the inbound handler yet.
- **No streaming:** Messages are sent once on `message.completed`, not
  edit-as-you-go. WhatsApp does not support message editing well.
- **No group support:** `@g.us` JIDs are filtered out by design.
- **No stickers:** Filtered out by design.
- **TTS/STT stubbed:** Both hooks are commented out. Text fallbacks are used.
- **Hot reload:** No `globalThis` guard against duplicate sockets in `eve dev`.

## Key source files

| File | Purpose |
| --- | --- |
| `agent/channels/whatsapp.ts` | The channel implementation |
| `agent/channels/eve.ts` | The default eve HTTP channel (unchanged) |
| `agent/agent.ts` | Agent config: model only (`zai/glm-5.2`) |
| `agent/instructions.md` | Agent system prompt (default) |
| `./auth_info_baileys/` | Baileys credential store (created on first run) |

## References

- [eve custom channels docs](https://eve.dev/docs/channels/custom)
- [Baileys (whiskeysockets) on GitHub](https://github.com/whiskeysockets/baileys)
- [Chat SDK Baileys adapter](https://chat-sdk.dev/adapters/community/baileys)
- [eve channel contract (overview)](https://eve.dev/docs/channels/overview)

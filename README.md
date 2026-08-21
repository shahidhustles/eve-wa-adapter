# eve-wa-adapter

WhatsApp channel for [eve](https://eve.dev) agents via [Baileys](https://github.com/whiskeysockets/baileys),
the unofficial WhatsApp Web API. Self-hosted, WebSocket-based, with voice notes,
media, location, HITL, and proactive sessions.

> **Warning:** Baileys is a third-party unofficial WhatsApp Web API. It is not
> affiliated with WhatsApp/Meta and may break when WhatsApp changes internal
> protocols. WhatsApp may suspend numbers that use unofficial automation.
> Evaluate compliance before production use.

## What this is

An eve custom channel (`agent/channels/whatsapp.ts`) that connects a Baileys
WhatsApp Web socket to your eve agent. Inbound WhatsApp messages become eve
session turns; agent replies are sent back as WhatsApp messages.

**Supported:**

- Text messages (send + receive)
- Voice notes / PTT (Deepgram Nova-3 transcription; Cartesia Sonic 3.5 voice-note replies)
- Images with captions (receive)
- Video with captions (receive)
- GPS location (receive, passed to agent as text coordinates)
- Human-in-the-loop (HITL) — rendered as numbered text choices; numeric replies
  resolve via eve's `respond()` API against the correct `requestId`
- Proactive sessions — send to a WhatsApp JID from a schedule or another channel
- Typing indicators, read receipts

**Not supported (by design):**

- Groups (`@g.us` filtered out — DMs only, including LID-based users)
- Stickers
- Polls, rich buttons, interactive cards
- Message editing / streaming edits
- Voice-note *replies* (TTS stubbed — see "Wiring TTS" below)

> **Testing note:** Messages where `msg.key.fromMe === true` are skipped to
> prevent response loops. This means WhatsApp's "Message Yourself" conversation
> cannot trigger the bot — test from another WhatsApp account/number.

## Requirements

- Node.js 24.x
- An eve project (this repo is one; or drop `agent/channels/whatsapp.ts`
  into any eve project's `agent/channels/` folder)
- A phone number to link as a WhatsApp "Linked Device"
- A persistent host (VM, container, VPS, or local machine) — **not** Vercel
  serverless. Baileys holds a long-lived WebSocket that must stay connected.

## Quick start

### 1. Install dependencies

```bash
npm install @whiskeysockets/baileys @hapi/boom qrcode
npm install -D @types/qrcode   # TypeScript types for qrcode
```

### 2. Add the channel file

Drop `agent/channels/whatsapp.ts` into your eve project. If you cloned this
repo, it's already there.

### 3. Run the agent

```bash
eve start
# or for local dev with the TUI:
eve dev
```

On first run, the terminal prints a QR code. Open WhatsApp on your phone →
**Settings → Linked Devices → Link a Device** and scan the QR.

Credentials are saved to `./auth_info_baileys/`. Subsequent restarts reuse
the session — no QR scan needed.

### 4. Send a message

Send a WhatsApp DM to the linked phone number. The agent will reply.

## Configuration

The channel reads its port from `process.env.PORT` (defaults to `2000`,
matching eve's default). No environment variables are required for basic
operation — the QR code is the only setup step.

| Option | Where | Default | Notes |
| --- | --- | --- | --- |
| Port | `PORT` env var | `2000` | Used by the bootstrap route to capture eve's `from()` dispatcher |
| Auth directory | `AUTH_DIR` constant in `whatsapp.ts` | `./auth_info_baileys` | Where Baileys stores session credentials |
| Browser name | `Browsers.ubuntu("eve-whatsapp")` in `connectSocket()` | `eve-whatsapp` | WhatsApp "Linked Device" label |
| Turn policy | `defineChannel({ turnPolicy: "queue" })` | `queue` | Turns finish in order; no steering |
| Deepgram key | `DEEPGRAM_API_KEY` env var | — | Required to transcribe inbound voice notes with Nova-3 |
| Cartesia key | `CARTESIA_API_KEY` env var | — | Required for voice-note replies with Sonic 3.5 |
| Cartesia voice | `CARTESIA_VOICE_ID` env var | — | The Cartesia voice ID used for replies |

## Voice setup

Add these values to `.env.local` (keep real keys out of source control):

```bash
DEEPGRAM_API_KEY=...
CARTESIA_API_KEY=...
CARTESIA_VOICE_ID=...
```

Inbound voice notes are transcribed with Deepgram Nova-3 before the agent sees
them. Replies to voice notes are synthesized with Cartesia Sonic 3.5, then
converted to WhatsApp-compatible Ogg/Opus audio. If either provider is unavailable, the adapter logs
the error and falls back to forwarding raw audio (STT) or sending text (TTS).

## How it works

```
WhatsApp  ──WebSocket──►  Baileys socket  ──messages.upsert──►  parse message
                                                                        │
                                                                        ▼
                                                              from(jid).send()
                                                                        │
                                                                        ▼
                                                        eve session (durable)
                                                                        │
                                                                    events
                                                                        ▼
                                                        message.completed
                                                        → sock.sendMessage()
```

Because Baileys is push-based (WebSocket) rather than pull-based (HTTP
webhook), the channel captures eve's `from()` dispatcher from a bootstrap
HTTP route on socket connect, then reuses it for all socket-driven
dispatches. Messages arriving before `from` is captured are queued and
drained automatically.

For the full design rationale, see [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md).

## Proactive sessions

Send a message to a WhatsApp user without an inbound message — from a
schedule, another channel, or a tool:

```ts
import whatsapp from "#channels/whatsapp";

await ctx.to(whatsapp, { threadId: "1234567890@s.whatsapp.net" }).send(
  "Your weekly digest is ready.",
  { auth: null },
);
```

The JID is the phone number in international format + `@s.whatsapp.net`
(e.g. `14155238886@s.whatsapp.net`).

## Self-hosting

This channel requires a long-running process. Options:

| Host | Command | Notes |
| --- | --- | --- |
| Local dev | `eve dev` | TUI with hot reload; may create duplicate sockets on reload |
| Local prod | `eve start` | Long-running Node process |
| VM / VPS | `eve start` (via pm2/systemd) | Recommended for always-on |
| Docker | `eve start` in a container | Mount `./auth_info_baileys` as a volume |

**Not supported:** Vercel serverless, or any platform where the process is
killed between requests. The Baileys socket must stay alive.

## Project structure

```
agent/
  channels/
    whatsapp.ts    ← the WhatsApp channel (this adapter)
    eve.ts         ← default eve HTTP channel
  agent.ts         ← agent config (model)
  instructions.md  ← system prompt
docs/
  ARCHITECTURE.md  ← how the channel works internally
```

## Dependencies

| Package | Purpose |
| --- | --- |
| [`eve`](https://www.npmjs.com/package/eve) | Agent framework |
| [`@whiskeysockets/baileys`](https://www.npmjs.com/package/@whiskeysockets/baileys) | WhatsApp Web API client |
| [`@hapi/boom`](https://www.npmjs.com/package/@hapi/boom) | HTTP-friendly errors (Baileys dependency) |
| [`qrcode`](https://www.npmjs.com/package/qrcode) | Terminal QR rendering (dev only) |

## Disclaimer

This is an unofficial adapter using an unofficial WhatsApp API. You are
responsible for complying with WhatsApp's Terms of Service and applicable
laws. WhatsApp may ban numbers that use unofficial automation.

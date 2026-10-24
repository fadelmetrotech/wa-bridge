// WhatsApp bridge: the client's WhatsApp is linked like WhatsApp Web (QR code, « Appareils connectés »), and the platform
// API asks this service to send the deposit details to a supplier as a normal message from his own number.
// The session is kept in Supabase (whatsapp_bridge_state) so a restart reconnects without a new QR scan.
// Every call except /health needs « Authorization: Bearer BRIDGE_SECRET ».
import http from "node:http";
import { timingSafeEqual } from "node:crypto";
import { makeWASocket, DisconnectReason, fetchLatestBaileysVersion, Browsers, initAuthCreds, BufferJSON, proto } from "@whiskeysockets/baileys";
import { createClient } from "@supabase/supabase-js";
import QRCode from "qrcode";
import pino from "pino";

const { PORT = "3000", BRIDGE_SECRET = "", SUPABASE_URL = "", SUPABASE_SERVICE_ROLE_KEY = "" } = process.env;
if (BRIDGE_SECRET.length < 24) throw new Error("BRIDGE_SECRET must be at least 24 characters");
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) throw new Error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required");

const db = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false, autoRefreshToken: false } });
const TABLE = "whatsapp_bridge_state";
const logger = pino({ level: process.env.LOG_LEVEL || "warn" });
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const encode = (value) => JSON.parse(JSON.stringify(value, BufferJSON.replacer));
const decode = (value) => JSON.parse(JSON.stringify(value), BufferJSON.reviver);

// ---------- Session stored in Supabase ----------
async function readKey(key) {
  const { data, error } = await db.from(TABLE).select("value").eq("key", key).maybeSingle();
  if (error) throw new Error(error.message);
  return data ? decode(data.value) : null;
}
async function writeRows(rows) {
  if (!rows.length) return;
  const { error } = await db.from(TABLE).upsert(rows.map((row) => ({ ...row, updated_at: new Date().toISOString() })));
  if (error) throw new Error(error.message);
}
async function clearSession() {
  const { error } = await db.from(TABLE).delete().neq("key", "");
  if (error) logger.error({ error: error.message }, "clear session failed");
}
async function useDatabaseAuthState() {
  const creds = (await readKey("creds")) ?? initAuthCreds();
  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const found = {};
          if (!ids.length) return found;
          const { data, error } = await db.from(TABLE).select("key,value").in("key", ids.map((id) => `${type}-${id}`));
          if (error) throw new Error(error.message);
          for (const row of data ?? []) {
            let value = decode(row.value);
            if (type === "app-state-sync-key" && value) value = proto.Message.AppStateSyncKeyData.fromObject(value);
            found[row.key.slice(type.length + 1)] = value;
          }
          return found;
        },
        set: async (data) => {
          const upserts = [];
          const deletes = [];
          for (const type of Object.keys(data)) {
            for (const id of Object.keys(data[type])) {
              const value = data[type][id];
              if (value) upserts.push({ key: `${type}-${id}`, value: encode(value) });
              else deletes.push(`${type}-${id}`);
            }
          }
          await writeRows(upserts);
          if (deletes.length) {
            const { error } = await db.from(TABLE).delete().in("key", deletes);
            if (error) throw new Error(error.message);
          }
        },
      },
    },
    saveCreds: () => writeRows([{ key: "creds", value: encode(creds) }]),
  };
}

// ---------- WhatsApp connection ----------
let sock = null;
let status = "starting"; // starting | qr | connected | reconnecting | logged_out
let qrDataUrl = null;
let phone = null;
let lastError = null;
let connecting = false;

async function connect() {
  if (connecting) return;
  connecting = true;
  try {
    const { state, saveCreds } = await useDatabaseAuthState();
    const { version } = await fetchLatestBaileysVersion().catch(() => ({ version: undefined }));
    sock = makeWASocket({
      version,
      auth: state,
      logger,
      browser: Browsers.macOS("Plateforme Fadel Change"),
      markOnlineOnConnect: false,
      syncFullHistory: false,
      generateHighQualityLinkPreview: false,
    });
    sock.ev.on("creds.update", saveCreds);
    sock.ev.on("connection.update", async (update) => {
      if (update.qr) {
        status = "qr";
        qrDataUrl = await QRCode.toDataURL(update.qr, { margin: 1, width: 320 });
      }
      if (update.connection === "open") {
        status = "connected";
        qrDataUrl = null;
        lastError = null;
        phone = sock.user?.id?.split(":")[0]?.split("@")[0] ?? null;
      }
      if (update.connection === "close") {
        const code = update.lastDisconnect?.error?.output?.statusCode;
        lastError = update.lastDisconnect?.error?.message ?? null;
        phone = null;
        if (code === DisconnectReason.loggedOut) {
          // Disconnected from the phone (« Appareils connectés » → Déconnexion): start over with a new QR code.
          status = "logged_out";
          await clearSession();
        } else {
          status = "reconnecting";
        }
        setTimeout(() => void connect(), code === DisconnectReason.loggedOut ? 1_000 : 3_000);
      }
    });
  } catch (error) {
    lastError = error instanceof Error ? error.message : String(error);
    status = "reconnecting";
    setTimeout(() => void connect(), 10_000);
  } finally {
    connecting = false;
  }
}

// ---------- Sending, one at a time, at a human pace ----------
let queue = Promise.resolve();
let lastSentAt = 0;

function send(to, text) {
  const job = queue.then(async () => {
    if (status !== "connected" || !sock) throw Object.assign(new Error("WhatsApp n’est pas connecté : scannez le QR code dans la plateforme."), { statusCode: 503 });
    await sleep(Math.max(0, lastSentAt + 4_000 + Math.random() * 3_000 - Date.now()));
    const jid = `${to}@s.whatsapp.net`;
    await sock.sendPresenceUpdate("composing", jid).catch(() => {});
    await sleep(Math.min(4_000, 1_200 + text.length * 15));
    await sock.sendPresenceUpdate("paused", jid).catch(() => {});
    const message = await sock.sendMessage(jid, { text });
    lastSentAt = Date.now();
    return message?.key?.id ?? null;
  });
  queue = job.catch(() => {});
  return job;
}

// ---------- HTTP ----------
const secret = Buffer.from(BRIDGE_SECRET);
function authorized(request) {
  const given = Buffer.from((request.headers.authorization || "").replace(/^Bearer\s+/i, ""));
  return given.length === secret.length && timingSafeEqual(given, secret);
}
function reply(response, statusCode, body) {
  response.writeHead(statusCode, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  response.end(JSON.stringify(body));
}
function readJson(request) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    request.on("data", (chunk) => { size += chunk.length; if (size > 16_384) { reject(new Error("too large")); request.destroy(); } else chunks.push(chunk); });
    request.on("end", () => { try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : {}); } catch { reject(new Error("invalid json")); } });
    request.on("error", reject);
  });
}

const server = http.createServer(async (request, response) => {
  const path = new URL(request.url || "/", "http://bridge").pathname;
  try {
    if (request.method === "GET" && path === "/health") return reply(response, 200, { ok: true, status });
    if (!authorized(request)) return reply(response, 401, { error: "unauthorized" });
    if (request.method === "GET" && path === "/status") {
      return reply(response, 200, { status, connected: status === "connected", phone, qr: status === "qr" ? qrDataUrl : null, error: status === "connected" ? null : lastError });
    }
    if (request.method === "POST" && path === "/send") {
      const body = await readJson(request);
      const to = String(body.to || "").replace(/\D/g, "").replace(/^00/, "");
      const text = String(body.text || "").trim();
      if (!/^[0-9]{8,15}$/.test(to) || !text || text.length > 2_000) return reply(response, 422, { error: "Numéro ou message invalide." });
      const id = await send(to, text);
      return reply(response, 200, { id });
    }
    if (request.method === "POST" && path === "/logout") {
      await sock?.logout().catch(() => {});
      await clearSession();
      status = "logged_out";
      phone = null;
      setTimeout(() => void connect(), 1_000);
      return reply(response, 200, { ok: true });
    }
    return reply(response, 404, { error: "not found" });
  } catch (error) {
    return reply(response, error?.statusCode || 500, { error: error instanceof Error ? error.message : "Erreur" });
  }
});

server.listen(Number(PORT), () => {
  logger.warn({ port: PORT }, "wa-bridge listening");
  void connect();
});

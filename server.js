"use strict";

// ═══════════════════════════════════════════════════════════════════════════════
//  EXILE — server.js
//  Full-featured chat backend: Google OAuth, JWT, MongoDB, WebSocket messaging
// ═══════════════════════════════════════════════════════════════════════════════
// Require and configure dotenv
require('dotenv').config();
const express      = require("express");
const http         = require("http");
const path = require("path");
const { Server: SocketIOServer } = require("socket.io");
const jwt          = require("jsonwebtoken");
const cookieParser = require("cookie-parser");
const multer       = require("multer");
const axios        = require("axios");
const FormData     = require("form-data");
const geoip        = require("geoip-lite");
const { MongoClient, ObjectId } = require("mongodb");
const { v4: uuidv4 } = require("uuid");

// ─── Config ───────────────────────────────────────────────────────────────────

const JWT_SECRET   = process.env.JWT_SECRET  || "change_this_secret_in_production";
const MONGO_URI    = process.env.MONGO_URI   || "mongodb://127.0.0.1:27017";
const DB_NAME      = process.env.DB_NAME     || "exileye";
const PORT         = process.env.PORT        || 3000;

const AUTZ_APP_ID  = process.env.AUTZ_APP_ID  || "chiq1ujiq";
const AUTZ_CALLBACK_URL = process.env.AUTZ_CALLBACK_URL || "https://exile.nett.to/auth/autz/callback";

const MSG_TEXT_MAX    = 1020;   // chars for plain text
const MSG_CAPTION_MAX = 500;    // chars for media caption
const MEDIA_MAX_BYTES = 30 * 1024 * 1024; // 30 MB

// Group message limits (same values as DMs — defined here so bot WS can reference them)
const GROUP_TEXT_MAX      = 1020;
const GROUP_CAPTION_MAX   = 500;
const SPACE_ALLOWED_TYPES = ["text", "image", "image_caption", "sticker", "audio", "voice"];

// ─── MongoDB ──────────────────────────────────────────────────────────────────

let db;

async function connectDB() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  db = client.db(DB_NAME);

  // ── users ──
  const users = db.collection("users");
  await users.createIndex({ autzId: 1 }, { unique: true, sparse: true });
  await users.createIndex({ userId: 1 },        { unique: true });
  await users.createIndex({ usernameLower: 1 }, { unique: true });
  await users.createIndex({ contactNumber: 1 }, { unique: true });
  await users.createIndex({ level: -1 });

  // ── messages ──
  const messages = db.collection("messages");
  await messages.createIndex({ messageId: 1 },                { unique: true });
  await messages.createIndex({ conversationId: 1, sentAt: -1 });
  await messages.createIndex({ senderId: 1 });
  await messages.createIndex({ recipientId: 1 });
  await messages.createIndex({ "content.text": "text" });    // full-text search

  // ── username text search ──
  await users.createIndex({ username: "text", name: "text" });

  // ── conversations ──
  const convos = db.collection("conversations");
  await convos.createIndex({ conversationId: 1 }, { unique: true });
  await convos.createIndex({ participants: 1 });

  // ── blocks ──
  await db.collection("blocks").createIndex(
    { blockerId: 1, blockedId: 1 }, { unique: true }
  );

  // ── bots ──
  const bots = db.collection("bots");
  await bots.createIndex({ botId: 1 },        { unique: true });
  await bots.createIndex({ botToken: 1 },     { unique: true });
  await bots.createIndex({ ownerId: 1 });
  await bots.createIndex({ usernameLower: 1 },{ unique: true });

  // ── reports ──
  await db.collection("reports").createIndex({ reportId: 1 }, { unique: true });
  await db.collection("reports").createIndex({ reportedId: 1 });
  await db.collection("reports").createIndex({ reporterId: 1 });

  // ── spaces ──
  const spaces = db.collection("spaces");
  await spaces.createIndex({ spaceId: 1 },   { unique: true });
  await spaces.createIndex({ joinId: 1 },    { unique: true });
  await spaces.createIndex({ ownerId: 1 });

  // ── feeds ──
  const feeds = db.collection("feeds");
  await feeds.createIndex({ feedId: 1 },    { unique: true });
  await feeds.createIndex({ joinId: 1 },    { unique: true });
  await feeds.createIndex({ ownerId: 1 });

  // ── group messages (spaces + feeds share this collection, scoped by groupId) ──
  const gmsgs = db.collection("groupMessages");
  await gmsgs.createIndex({ messageId: 1 },            { unique: true });
  await gmsgs.createIndex({ groupId: 1, sentAt: -1 });
  await gmsgs.createIndex({ senderId: 1 });
  await gmsgs.createIndex({ "content.text": "text" });
  await gmsgs.createIndex({ groupId: 1, readBy: 1 });  // unread count queries
  await gmsgs.createIndex({ groupId: 1, type: 1, deleted: 1 }); // search filter

  // ── conversations: sort by last message time ──
  await convos.createIndex({ participants: 1, lastMessageAt: -1 });

  // ── messages: unread query (recipientId, readAt, deleted) ──
  await messages.createIndex({ recipientId: 1, readAt: 1, deleted: 1 });
  // ── messages: type filter for exile history ──
  await messages.createIndex({ senderId: 1, type: 1 });
  await messages.createIndex({ recipientId: 1, type: 1 });

  // ── spaces/feeds: member lookup + xp leaderboard ──
  await spaces.createIndex({ "members.userId": 1 });
  await spaces.createIndex({ xp: -1 });
  await spaces.createIndex({ memberCount: -1 });
  await feeds.createIndex({ "members.userId": 1 });
  await feeds.createIndex({ xp: -1 });
  await feeds.createIndex({ memberCount: -1 });

  // ── users: leaderboard indexes ──
  await users.createIndex({ xp: -1 });
  await users.createIndex({ reputation: -1 });
  await users.createIndex({ streaks: -1 });
  await users.createIndex({ daysOnline: -1 });

  // ── rank snapshots (for trend arrows) ──
  await db.collection("rankSnapshots").createIndex({ userId: 1, type: 1 }, { unique: true });

  // ── spam tracking (rate-limit only, no auto-ban) ──
  await db.collection("spamLog").createIndex({ userId: 1, windowStart: 1 });

  // ── stickers ──
  const stickers = db.collection("stickers");
  await stickers.createIndex({ userId: 1 }, { unique: true });
  await stickers.createIndex({ "urls": 1 });

  console.log(`✓ MongoDB connected — database: "${DB_NAME}"`);
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function generateUserId() {
  const users = db.collection("users");
  let id;
  do { id = String(Math.floor(10000000 + Math.random() * 90000000)); }
  while (await users.findOne({ userId: id }));
  return id;
}

async function generateContactNumber() {
  const users = db.collection("users");
  let number, digits = 12;
  do {
    const raw    = Array.from({ length: digits }, () => Math.floor(Math.random() * 10)).join("");
    const groups = raw.match(/.{1,3}/g).join(" ");
    number       = `+${groups}`;
    const total  = await users.countDocuments();
    if (total > Math.pow(10, digits) * 0.9) digits++;
  } while (await users.findOne({ contactNumber: number }));
  return number;
}

function getCountryFromReq(req) {
  const rawIp = req.headers["x-forwarded-for"]?.split(",")[0].trim() || req.socket?.remoteAddress || "";
  const ip    = rawIp === "::1" || rawIp === "127.0.0.1" ? "8.8.8.8" : rawIp;
  const geo   = geoip.lookup(ip);
  return geo?.country || "Unknown";
}

function getIpFromReq(req) {
  return req.headers["x-forwarded-for"]?.split(",")[0].trim() || req.socket?.remoteAddress || "unknown";
}

async function uploadToCatbox(buffer, filename) {
  const form = new FormData();
  form.append("reqtype", "fileupload");
  form.append("fileToUpload", buffer, { filename });
  const res = await axios.post("https://catbox.moe/user.php", form, { headers: form.getHeaders() });
  const url = res.data.trim();
  if (!url.startsWith("http")) throw new Error("Catbox upload failed: " + url);
  return url;
}

/**
 * Upload to touch.io (fallback CDN).
 * Returns the public URL of the uploaded file.
 */
async function uploadToTouchIO(buffer, filename) {
  const form = new FormData();
  form.append("file", buffer, { filename });
  const res = await axios.post("https://touchio.vercel.app/api/upload", form, {
    headers: form.getHeaders(),
    timeout: 30000,
  });
  const data = res.data;
  // touch.io returns { short_url } or { url } depending on endpoint
  const url = data.url || data.short_url || data.file_url;
  if (!url || !url.startsWith("http")) throw new Error("touch.io upload failed: " + JSON.stringify(data));
  return url;
}

/**
 * Upload a file, trying catbox first, falling back to touch.io.
 */
async function uploadMedia(buffer, filename) {
  try {
    return await uploadToCatbox(buffer, filename);
  } catch (catboxErr) {
    console.warn("Catbox upload failed, trying touch.io fallback:", catboxErr.message);
    try {
      return await uploadToTouchIO(buffer, filename);
    } catch (touchErr) {
      throw new Error(`All upload providers failed. Catbox: ${catboxErr.message}. touch.io: ${touchErr.message}`);
    }
  }
}

function validateUsername(username) {
  if (!username)                          return "Username is required.";
  if (username.length < 3)               return "Username must be at least 3 characters.";
  if (!/^[a-zA-Z0-9_]+$/.test(username)) return "Username can only contain letters, numbers, and underscores.";
  if (username.startsWith("_") || username.endsWith("_")) return "Username cannot start or end with an underscore.";
  if (username.toLowerCase().endsWith("bot")) return 'Username cannot end with "bot".';
  return null;
}

function createTempToken(payload) { return jwt.sign(payload, JWT_SECRET, { expiresIn: "30m" }); }
function verifyTempToken(token)   { try { return jwt.verify(token, JWT_SECRET); } catch { return null; } }

/** Build a minimal user stub for embedding in messages / responses */
function userStub(u) {
  if (!u) return null;
  return {
    userId:         u.userId,
    name:           u.name,
    username:       u.username,
    tag:            u.tag,
    contactNumber:  u.hideContact ? null : u.contactNumber,
    profilePicture: u.profilePicture,
    premium:        u.premium,
    verified:       u.verified,
    level:          u.level || computeLevel(u.xp || 0),
  };
}

/** Strip internal fields before sending full user objects to clients */
// ════════════════════════════════════════════════════════════════════════════
//  BADGE + RANK SYSTEM
// ════════════════════════════════════════════════════════════════════════════

/**
 * Level-based ranks — shown as a title on the user profile.
 * Each rank covers a level range.
 */
const RANKS = [
  { id: "newcomer",       label: "Newcomer",       minLevel: 1  },
  { id: "wanderer",       label: "Wanderer",        minLevel: 5  },
  { id: "explorer",       label: "Explorer",        minLevel: 10 },
  { id: "adventurer",     label: "Adventurer",      minLevel: 15 },
  { id: "rogue",          label: "Rogue",           minLevel: 20 },
  { id: "warrior",        label: "Warrior",         minLevel: 25 },
  { id: "knight",         label: "Knight",          minLevel: 30 },
  { id: "guardian",       label: "Guardian",        minLevel: 35 },
  { id: "champion",       label: "Champion",        minLevel: 40 },
  { id: "hero",           label: "Hero",            minLevel: 45 },
  { id: "warlord",        label: "Warlord",         minLevel: 50 },
  { id: "elite",          label: "Elite",           minLevel: 55 },
  { id: "veteran",        label: "Veteran",         minLevel: 60 },
  { id: "master",         label: "Master",          minLevel: 70 },
  { id: "grandmaster",    label: "Grandmaster",     minLevel: 80 },
  { id: "sage",           label: "Sage",            minLevel: 90 },
  { id: "mythic",         label: "Mythic",          minLevel: 100 },
  { id: "immortal",       label: "Immortal",        minLevel: 120 },
  { id: "ascendant",      label: "Ascendant",       minLevel: 150 },
  { id: "legend",         label: "Legend",          minLevel: 200 },
  { id: "exiled_legend",  label: "Exiled Legend",   minLevel: 300 },
];

function getRank(level) {
  let rank = RANKS[0];
  for (const r of RANKS) {
    if (level >= r.minLevel) rank = r;
    else break;
  }
  return rank;
}

/**
 * Badges — awarded based on level, reputation, streaks, daysOnline, etc.
 * Each badge has: id, label, description, icon (emoji), tier (bronze/silver/gold/platinum/diamond/legendary)
 */
const BADGE_DEFS = [
  // ── Level badges ──
  { id: "first_step",      label: "First Step",       desc: "Reach level 5",         icon: "👣", tier: "bronze",    req: u => u.level >= 5   },
  { id: "rising_star",     label: "Rising Star",      desc: "Reach level 10",        icon: "⭐", tier: "bronze",    req: u => u.level >= 10  },
  { id: "adventurer_b",    label: "Adventurer",       desc: "Reach level 15",        icon: "🗺️", tier: "bronze",    req: u => u.level >= 15  },
  { id: "rogue_b",         label: "Rogue",            desc: "Reach level 20",        icon: "🗡️", tier: "silver",    req: u => u.level >= 20  },
  { id: "warrior_b",       label: "Warrior",          desc: "Reach level 30",        icon: "⚔️", tier: "silver",    req: u => u.level >= 30  },
  { id: "champion_b",      label: "Champion",         desc: "Reach level 40",        icon: "🏆", tier: "silver",    req: u => u.level >= 40  },
  { id: "hero_b",          label: "Hero",             desc: "Reach level 50",        icon: "🦸", tier: "gold",      req: u => u.level >= 50  },
  { id: "warlord_b",       label: "Warlord",          desc: "Reach level 60",        icon: "🏰", tier: "gold",      req: u => u.level >= 60  },
  { id: "master_b",        label: "Master",           desc: "Reach level 75",        icon: "🎓", tier: "gold",      req: u => u.level >= 75  },
  { id: "grandmaster_b",   label: "Grandmaster",      desc: "Reach level 100",       icon: "👑", tier: "platinum",  req: u => u.level >= 100 },
  { id: "sage_b",          label: "Sage",             desc: "Reach level 125",       icon: "🔮", tier: "platinum",  req: u => u.level >= 125 },
  { id: "mythic_b",        label: "Mythic",           desc: "Reach level 150",       icon: "💫", tier: "diamond",   req: u => u.level >= 150 },
  { id: "immortal_b",      label: "Immortal",         desc: "Reach level 200",       icon: "✨", tier: "diamond",   req: u => u.level >= 200 },
  { id: "legend_b",        label: "Legend",           desc: "Reach level 300",       icon: "🌟", tier: "legendary", req: u => u.level >= 300 },
  { id: "exiled_legend_b", label: "Exiled Legend",    desc: "Reach the max level",   icon: "💎", tier: "legendary", req: u => u.level >= 500 },

  // ── Reputation badges ──
  { id: "respected",     label: "Respected",      desc: "Earn 5 reputation",       icon: "🤝", tier: "bronze",    req: u => (u.reputation || 0) >= 5   },
  { id: "honored",       label: "Honored",        desc: "Earn 10 reputation",      icon: "🎖️", tier: "silver",    req: u => (u.reputation || 0) >= 10  },
  { id: "revered",       label: "Revered",        desc: "Earn 20 reputation",      icon: "🏅", tier: "gold",      req: u => (u.reputation || 0) >= 20  },
  { id: "exalted",       label: "Exalted",        desc: "Earn 30 reputation",      icon: "🌠", tier: "platinum",  req: u => (u.reputation || 0) >= 30  },
  { id: "transcendent",  label: "Transcendent",   desc: "Earn 50 reputation",      icon: "🌌", tier: "legendary", req: u => (u.reputation || 0) >= 50  },

  // ── Streak badges ──
  { id: "consistent",   label: "Consistent",     desc: "7-day login streak",      icon: "🔥", tier: "bronze",    req: u => (u.streaks || 0) >= 7   },
  { id: "dedicated",    label: "Dedicated",      desc: "30-day login streak",     icon: "🔥", tier: "silver",    req: u => (u.streaks || 0) >= 30  },
  { id: "obsessed",     label: "Obsessed",       desc: "100-day login streak",    icon: "🔥", tier: "gold",      req: u => (u.streaks || 0) >= 100 },
  { id: "unstoppable",  label: "Unstoppable",    desc: "365-day login streak",    icon: "🔥", tier: "diamond",   req: u => (u.streaks || 0) >= 365 },

  // ── Veteran badges ──
  { id: "regular",      label: "Regular",        desc: "30 days online",          icon: "📅", tier: "bronze",    req: u => (u.daysOnline || 0) >= 30  },
  { id: "old_timer",    label: "Old Timer",      desc: "100 days online",         icon: "🗓️", tier: "silver",    req: u => (u.daysOnline || 0) >= 100 },
  { id: "veteran_b",    label: "Veteran",        desc: "365 days online",         icon: "🎂", tier: "gold",      req: u => (u.daysOnline || 0) >= 365 },
  { id: "elder",        label: "Elder",          desc: "730 days online",         icon: "🧓", tier: "platinum",  req: u => (u.daysOnline || 0) >= 730 },

  // ── Special badges ──
  { id: "premium_user",  label: "Premium",       desc: "Active premium member",   icon: "💜", tier: "gold",      req: u => u.premium === true },
  { id: "gifter",        label: "Gifter",        desc: "Send 10 gifts",           icon: "🎁", tier: "silver",    req: u => (u.giftSent || 0) >= 10  },
  { id: "generous",      label: "Generous",      desc: "Send 50 gifts",           icon: "🎀", tier: "gold",      req: u => (u.giftSent || 0) >= 50  },
];

/** Compute which badges a user has earned */
function computeBadges(user) {
  const level = computeLevel(user.xp || 0);
  const u     = { ...user, level };
  return BADGE_DEFS.filter(b => b.req(u)).map(b => ({
    id:    b.id,
    label: b.label,
    desc:  b.desc,
    icon:  b.icon,
    tier:  b.tier,
  }));
}

/** The highest-tier badge (for quick display, e.g. next to name) */
const TIER_ORDER = ["bronze", "silver", "gold", "platinum", "diamond", "legendary"];
function getHighlightBadge(badges) {
  if (!badges || badges.length === 0) return null;
  return badges.slice().sort(
    (a, b) => TIER_ORDER.indexOf(b.tier) - TIER_ORDER.indexOf(a.tier)
  )[0];
}

function sanitizeUser(user, viewerUserId = null) {
  if (!user) return null;
  const { _id, autzId, usernameLower, creationIp, blockedUsers, ...safe } = user;
  // hide contact number if user opted to hide it (unless it's the user themselves)
  if (safe.hideContact && viewerUserId !== user.userId) safe.contactNumber = null;
  // Attach live level info (always computed from current XP so it's never stale)
  const levelInfo = buildLevelInfo(safe.xp || 0);
  safe.level      = levelInfo.level;
  safe.levelInfo  = levelInfo;
  // Attach rank (title) and earned badges
  safe.rank       = getRank(levelInfo.level);
  safe.badges     = computeBadges({ ...safe, level: levelInfo.level });
  safe.topBadge   = getHighlightBadge(safe.badges);
  return safe;
}

// ─── Conversation ID ──────────────────────────────────────────────────────────

function makeConversationId(a, b) {
  // deterministic: always same ID for same pair
  return [a, b].sort().join("_");
}

// ─── ID Generators ───────────────────────────────────────────────────────────

async function generateSpaceId() {
  let id;
  do { id = String(Math.floor(1000000 + Math.random() * 9000000)); }
  while (await db.collection("spaces").findOne({ spaceId: id }) ||
         await db.collection("feeds").findOne({ feedId: id }));
  return id;
}

function generateJoinId() {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 16; i++) id += chars[Math.floor(Math.random() * chars.length)];
  return id;
}

async function generateGroupMessageId() {
  return uuidv4();
}

// ─── Group broadcast helper ───────────────────────────────────────────────────
function requireSaulAdmin(req, res, next) {
  const token = req.cookies?.token;
  if (!token) return res.status(401).json({ error: "Not authenticated." });

  let decoded;
  try { decoded = jwt.verify(token, JWT_SECRET); }
  catch { return res.status(401).json({ error: "Invalid token." }); }
  req.user = decoded;

  db.collection("users").findOne({ userId: req.user.userId }).then(user => {
    if (!user)                       return res.status(404).json({ error: "User not found." });
    if (user.position !== "admin")   return res.status(403).json({ error: "Admin access required." });
    if (user.username.toLowerCase() !== "saul")
      return res.status(403).json({ error: "Access restricted to authorised personnel." });
    req.adminUser = user;
    next();
  }).catch(e => res.status(500).json({ error: e.message }));
}

/** Broadcast a payload to all online members of a space or feed (users + bots) */
async function broadcastToGroup(groupId, payload, excludeUserId = null) {

  const spaceDoc = await db.collection("spaces").findOne({ spaceId: groupId }).catch(() => null);

  const feedDoc  = spaceDoc ? null : await db.collection("feeds").findOne({ feedId: groupId }).catch(() => null);

  const members  = (spaceDoc || feedDoc)?.members || [];

  for (const m of members) {

    if (m.userId === excludeUserId) continue;

    const socks = onlineClients.get(m.userId) || new Set();

    for (const socketId of socks) {

      mainNsp.to(socketId).emit("message", payload);

    }

    const botSocketId = botClients.get(m.userId);

    if (botSocketId) {

      botNsp.to(botSocketId).emit("message", payload);

    }

  }

}

// ─── Rate-limit / spam detector (no auto-ban — use reports instead) ──────────

const SPAM_WINDOW_MS = 10_000; // 10 seconds
const SPAM_THRESHOLD = 15;     // messages per window before throttle

async function checkSpam(userId) {
  const log = db.collection("spamLog");
  const now = Date.now();

  let entry = await log.findOne({ userId });
  if (!entry || now - entry.windowStart > SPAM_WINDOW_MS) {
    await log.updateOne(
      { userId },
      { $set: { userId, windowStart: now, count: 1 } },
      { upsert: true }
    );
    return { throttled: false };
  }

  const newCount = (entry.count || 0) + 1;
  await log.updateOne({ userId }, { $set: { count: newCount } });

  if (newCount >= SPAM_THRESHOLD) {
    await log.updateOne({ userId }, { $set: { count: 0, windowStart: now } });
    return { throttled: true };
  }
  return { throttled: false };
}

// ─── App ──────────────────────────────────────────────────────────────────────

const app    = express();
const server = http.createServer(app);

const io = new SocketIOServer(server, {
  cors: { origin: "*", credentials: true },
  cookie: true,
});

// Three namespaces replacing three WebSocketServer instances
const mainNsp  = io.of("/");          // replaces wss
const botNsp   = io.of("/bot");       // replaces botWss
const cpNsp    = io.of("/controlpanel"); // replaces cpWss

app.use(express.json());
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));
const upload = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: MEDIA_MAX_BYTES },
});

// ─── Passport ─────────────────────────────────────────────────────────────────


// ─── Auth helpers ─────────────────────────────────────────────────────────────

const COOKIE_OPTS = { httpOnly: true, secure: true, sameSite: "lax" };

function setAuthCookie(res, userId) {
  const token = jwt.sign({ userId }, JWT_SECRET, { expiresIn: "90d" });
  res.cookie("token", token, { ...COOKIE_OPTS, maxAge: 90 * 24 * 60 * 60 * 1000 });
}

function requireAuth(req, res, next) {
  const token = req.cookies?.token;
  if (!token) return res.status(401).json({ error: "Not authenticated." });
  try { req.user = jwt.verify(token, JWT_SECRET); next(); }
  catch { res.status(401).json({ error: "Invalid or expired token." }); }
}

// ─── WebSocket server ─────────────────────────────────────────────────────────

// In-memory map: groupId -> Map<userId, { name, username, timer }>
// Used to build the "X, Y and N others are typing..." string
const groupTypingMap = new Map();

function getGroupTypers(groupId) {
  if (!groupTypingMap.has(groupId)) groupTypingMap.set(groupId, new Map());
  return groupTypingMap.get(groupId);
}

function buildTypingLabel(typers, myUserId) {
  // typers is a Map<userId, {name}>. Exclude the current user.
  const others = [...typers.entries()].filter(([uid]) => uid !== myUserId);
  if (others.length === 0) return null;
  const names = others.map(([, t]) => t.name);
  if (names.length <= 2) return `${names.join(" and ")} ${names.length === 1 ? "is" : "are"} typing...`;
  return `${names[0]}, ${names[1]} and ${names.length - 2} other${names.length - 2 > 1 ? "s" : ""} are typing...`;
}

function broadcastGroupTyping(groupId) {
  const typers = getGroupTypers(groupId);
  // Broadcast to each member — each sees a different "fromMe" perspective
  const userIds = [...(groupTypingMap.get(groupId)?.keys() || [])];
  broadcastToGroup(groupId, {
    type:    "groupTyping",
    groupId,
    typerIds: userIds,
    // label is computed per-client — we send all typer names so client builds its own string
    typers:  [...typers.values()].map(t => ({ userId: t.userId, name: t.name })),
    count:   typers.size,
  }, null);
}

// Map userId → Set<WebSocket>  (regular users)
const onlineClients = new Map();

// Map botId → WebSocket  (bots — one persistent connection each)
const botClients = new Map();

// Map userId → Set<WebSocket>  (control-panel connections — can be multiple tabs)
const cpClients = new Map();

// ─── XP Engine ────────────────────────────────────────────────────────────────
// XP values for each action
const XP_TABLE = {
  sendMessage:      2,   // sending a text message
  sendMedia:        3,   // sending image/video/audio/sticker
  receiveMessage:   1,   // receiving a message (passive engagement)
  addContact:       5,   // adding someone to contacts
  getAdded:         3,   // someone adds you
  reactToMessage:   1,   // reacting to a message
  getReacted:       2,   // someone reacts to your message
  editMessage:      0,   // no XP for editing
  dailyLogin:      10,   // first WS connect of the day
  streakBonus:      5,   // multiplied by streak days (capped at 50)
  completedProfile: 20,  // one-time: avatar + description filled
  reportSubmit:     2,   // submitting a report
  createBot:        15,  // creating a bot
};

const XP_COOLDOWN_MS = 2000; // min ms between XP grants for same action (anti-abuse)
const xpCooldowns    = new Map(); // userId:action → last grant timestamp

/**
 * Grant XP to a user.
 * @param {string} userId
 * @param {string} action  — key from XP_TABLE
 * @param {number} [multiplier=1]
 */
// ─── Level formula (matches frontend) ────────────────────────────────────────

function getNextLevelXp(level) {
  if (level <= 10) return 100;
  if (level <= 20) return 200;
  if (level <= 30) return 300;
  if (level <= 40) return 400;
  if (level <= 50) return 500;
  return 600;
}

function getTotalXpForLevel(level) {
  let total = 0;
  for (let i = 1; i < level; i++) total += getNextLevelXp(i);
  return total;
}

/** Compute level from total XP. */
function computeLevel(xp) {
  let level = 1;
  while (xp >= getTotalXpForLevel(level + 1)) level++;
  return level;
}

/** Build the full level info object returned in /api/me, /api/xp/me, etc. */
function buildLevelInfo(xp) {
  const level          = computeLevel(xp);
  const currentFloor   = getTotalXpForLevel(level);
  const nextFloor      = getTotalXpForLevel(level + 1);
  const progressXp     = xp - currentFloor;
  const neededXp       = nextFloor - currentFloor;
  return {
    level,
    xp,
    nextLevelXp:  nextFloor,
    progressXp,
    neededXp,
    progressPct:  Math.min(100, Math.round((progressXp / neededXp) * 100)),
  };
}

async function grantXP(userId, action, multiplier = 1) {
  if (!XP_TABLE[action]) return;

  // Cooldown guard for high-frequency actions
  const cooldownKey = `${userId}:${action}`;
  const lastGrant   = xpCooldowns.get(cooldownKey) || 0;
  if (Date.now() - lastGrant < XP_COOLDOWN_MS) return;
  xpCooldowns.set(cooldownKey, Date.now());

  const xpAmount = Math.round(XP_TABLE[action] * multiplier);
  const users    = db.collection("users");

  // Fetch current state before update
  const before = await users.findOne({ userId }, { projection: { xp: 1, level: 1, reputation: 1 } });
  if (!before) return;

  const oldXp    = before.xp    || 0;
  const oldLevel = before.level || computeLevel(oldXp);
  const newXp    = oldXp + xpAmount;
  const newLevel = computeLevel(newXp);

  const dbUpdate = { $inc: { xp: xpAmount }, $set: { level: newLevel } };

  // Reputation milestone: every multiple of 10 levels → reputation +1
  let reputationGained = 0;
  if (newLevel > oldLevel) {
    // Count how many multiples of 10 were crossed
    for (let lvl = oldLevel + 1; lvl <= newLevel; lvl++) {
      if (lvl % 10 === 0) reputationGained++;
    }
    if (reputationGained > 0) {
      dbUpdate.$inc.reputation = reputationGained;
    }
  }

  await users.updateOne({ userId }, dbUpdate);
  const updated = await users.findOne({ userId }, { projection: { xp: 1, level: 1, reputation: 1 } });
  if (!updated) return;

  const cpPayload = {
    type:   "xpUpdate",
    userId,
    ...buildLevelInfo(updated.xp),
    gained: xpAmount,
    action,
    reputation: updated.reputation,
    rank:   getRank(updated.level || computeLevel(updated.xp || 0)),
  };

  // Attach level-up info if level changed
  if (newLevel > oldLevel) {
    cpPayload.levelUp    = true;
    cpPayload.oldLevel   = oldLevel;
    cpPayload.newLevel   = newLevel;
    if (reputationGained > 0) {
      cpPayload.reputationGained = reputationGained;
      cpPayload.reputationMilestone = true;
    }
  }

  broadcastCP(userId, cpPayload);

  // Check for newly unlocked badges
  try {
    const userDoc_  = await db.collection("users").findOne(
      { userId },
      { projection: { xp: 1, level: 1, reputation: 1, streaks: 1, daysOnline: 1, premium: 1, giftSent: 1, _lastBadgeIds: 1 } }
    );
    if (userDoc_) {
      const allBadges_ = computeBadges({ ...userDoc_, level: updated.level || computeLevel(updated.xp || 0) });
      const lastIds_   = new Set(userDoc_._lastBadgeIds || []);
      const newBadges_ = allBadges_.filter(b => !lastIds_.has(b.id));
      if (newBadges_.length > 0) {
        for (const badge of newBadges_) {
          broadcastCP(userId, { type: "badgeUnlocked", badge });
        }
        await db.collection("users").updateOne(
          { userId },
          { $set: { _lastBadgeIds: allBadges_.map(b => b.id) } }
        );
      }
    }
  } catch (_) {}
}

// ─── Streak Engine ────────────────────────────────────────────────────────────
/**
 * Called on first WS connect of each day.
 * - If last active was yesterday → increment streak
 * - If last active was today → no change
 * - If last active was 2+ days ago → reset streak to 1
 * Also grants dailyLogin XP + streakBonus.
 */
async function processDailyStreak(userId) {
  const users    = db.collection("users");
  const user     = await users.findOne({ userId }, { projection: { lastActiveDate: 1, streaks: 1, daysOnline: 1 } });
  if (!user) return;

  const todayStr     = new Date().toISOString().slice(0, 10); // "YYYY-MM-DD"
  const lastDate     = user.lastActiveDate || null;

  if (lastDate === todayStr) return; // already processed today

  let newStreak   = user.streaks || 0;
  let streakBroke = false;

  if (lastDate) {
    const last    = new Date(lastDate);
    const today   = new Date(todayStr);
    const diffDays = Math.round((today - last) / 86400000);

    if (diffDays === 1) {
      newStreak += 1;       // consecutive day
    } else {
      newStreak  = 1;       // missed a day — reset
      streakBroke = true;
    }
  } else {
    newStreak = 1;          // first ever login
  }

  const streakBonus = Math.min(newStreak * XP_TABLE.streakBonus, 50); // cap at 50 XP

  await users.updateOne({ userId }, {
    $set: { lastActiveDate: todayStr, streaks: newStreak },
    $inc: { daysOnline: 1, xp: XP_TABLE.dailyLogin + streakBonus },
  });

  // Push to control panel
  const refreshed = await users.findOne({ userId }, { projection: { xp: 1, streaks: 1 } });
  broadcastCP(userId, {
    type:        "xpUpdate",
    userId,
    xp:          refreshed.xp,
    gained:      XP_TABLE.dailyLogin + streakBonus,
    action:      "dailyLogin",
    streaks:     newStreak,
    streakBroke,
  });
}

function getSocketsForUser(userId) {
  return onlineClients.get(userId) || new Set();
}

// Broadcast to all sockets belonging to a user (regular users)

function broadcast(userId, payload) {

  const socks = onlineClients.get(userId);

  if (socks) {

    for (const socketId of socks) {

      mainNsp.to(socketId).emit("message", payload);

    }

  }

  // Bot socket

  const botSocketId = botClients.get(userId);

  if (botSocketId) {

    botNsp.to(botSocketId).emit("message", payload);

  }

}

function broadcastToConversation(convId, payload, excludeUserId = null) {

  const parts = convId.split("_");

  for (const uid of parts) {

    if (uid !== excludeUserId) broadcast(uid, payload);

  }

}

function broadcastCP(userId, payload) {

  const socks = cpClients.get(userId);

  if (socks) {

    for (const socketId of socks) {

      cpNsp.to(socketId).emit("message", payload);

    }

  }

}

mainNsp.use(async (socket, next) => {
  // Parse cookie from handshake headers
  const rawCookie = socket.handshake.headers.cookie || "";
  const cookies   = Object.fromEntries(
    rawCookie.split(";").filter(Boolean).map(c => {
      const [k, ...v] = c.trim().split("=");
      return [decodeURIComponent(k), decodeURIComponent(v.join("="))];
    })
  );
  const token = cookies["token"] || socket.handshake.auth?.token;
  if (!token) return next(new Error("Unauthorized"));

  try {
    socket.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch {
    next(new Error("Invalid token"));
  }
});

mainNsp.on("connection", async (socket) => {
  const userId = socket.user.userId;

  const userDoc = await db.collection("users").findOne({ userId });
  if (!userDoc || userDoc.banned) {
    socket.disconnect(true);
    return;
  }

  // Register socket
  if (!onlineClients.has(userId)) onlineClients.set(userId, new Set());
  onlineClients.get(userId).add(socket.id);

  await db.collection("users").updateOne({ userId }, { $set: { status: "online" } });
  notifyPresence(userId, "online").catch(console.error);
  processDailyStreak(userId).catch(console.error);

  // ── typing (DM) ──
  socket.on("typing", ({ conversationId, isTyping }) => {
    if (!conversationId) return;
    broadcastToConversation(conversationId, {
      type: "typing", conversationId, userId, isTyping: !!isTyping,
    }, userId);
  });

  socket.on("joinGroup", ({ groupId, groupType }) => {
  socket.join(`group:${groupId}`);
});

socket.on("groupTyping", ({ groupId, groupType }) => {
  const typer = { userId: socket._userId, name: socket._userName };
  socket.to(`group:${groupId}`).emit("message", {
    type: "groupTyping",
    groupId,
    typers: [typer],
    count: 1,
  });
});
  
  // ── groupTyping ──
  socket.on("groupTyping", async ({ groupId, isTyping }) => {
    if (!groupId) return;
    const user_ = await db.collection("users").findOne({ userId }, { projection: { name: 1, username: 1 } });
    const typers = getGroupTypers(groupId);

    if (isTyping) {
      if (typers.has(userId)) clearTimeout(typers.get(userId).timer);
      const timer = setTimeout(() => {
        typers.delete(userId);
        broadcastGroupTyping(groupId);
      }, 6000);
      typers.set(userId, { userId, name: user_?.name || "Someone", username: user_?.username, timer });
    } else {
      if (typers.has(userId)) {
        clearTimeout(typers.get(userId).timer);
        typers.delete(userId);
      }
    }
    broadcastGroupTyping(groupId);
  });

  // ── read (DM) ──
  socket.on("read", async ({ conversationId, upToMessageId }) => {
    if (!conversationId || !upToMessageId) return;
    await markMessagesRead(userId, conversationId, upToMessageId);
  });

  // ── groupRead ──
  socket.on("groupRead", async ({ groupId, upToMessageId }) => {
    if (!groupId) return;
    await markGroupMessagesRead(userId, groupId, upToMessageId);
  });

  // ── ping ──
  socket.on("ping", () => socket.emit("message", { type: "pong" }));

  socket.on("disconnect", async () => {
    const socks = onlineClients.get(userId);
    if (socks) {
      socks.delete(socket.id);
      if (socks.size === 0) {
        onlineClients.delete(userId);
        await db.collection("users").updateOne(
          { userId },
          { $set: { status: "offline", lastSeen: new Date().toISOString() } }
        );
        notifyPresence(userId, "offline").catch(console.error);
      }
    }
  });
});

async function notifyPresence(userId, status) {
  const user = await db.collection("users").findOne({ userId });
  if (!user) return;

  const privacySetting = user.onlineVisibility || "everybody"; // everybody | nobody | contacts
  if (privacySetting === "nobody") return;

  let notifyIds = [];
  if (privacySetting === "everybody") {
    // notify all online users who have open sockets (cheap approach — send to contacts at minimum)
    notifyIds = [...onlineClients.keys()].filter(id => id !== userId);
  } else if (privacySetting === "contacts") {
    notifyIds = (user.contactsList || []);
  }

  const payload = { type: "presence", userId, status, lastSeen: user.lastSeen || null };
  for (const uid of notifyIds) broadcast(uid, payload);
}

async function markMessagesRead(readerId, conversationId, upToMessageId) {
  const messages = db.collection("messages");
  const msg      = await messages.findOne({ messageId: upToMessageId });
  if (!msg) return;

  await messages.updateMany(
    {
      conversationId,
      recipientId: readerId,
      readAt:      null,
      deleted:     { $ne: true },
      sentAt:      { $lte: msg.sentAt },
    },
    { $set: { readAt: new Date().toISOString() } }
  );

  // Notify sender that messages were read
  broadcastToConversation(conversationId, {
    type: "messagesRead",
    conversationId,
    readerId,
    upToMessageId,
  }, readerId);
}

// ─── Group read receipts ─────────────────────────────────────────────────────

/**
 * Mark all unread group messages in a group as read for a user.
 * Notifies the group (so senders can see read counts update) via "groupMessagesRead".
 * Called when user fetches messages (already done silently in GET route)
 * AND can be called explicitly via WS { type:"groupRead", groupId }.
 */
async function markGroupMessagesRead(userId, groupId, upToMessageId = null) {
  const gmsgs = db.collection("groupMessages");

  let filter = { groupId, deleted: { $ne: true }, readBy: { $nin: [userId] } };
  if (upToMessageId) {
    const pivot = await gmsgs.findOne({ messageId: upToMessageId });
    if (pivot) filter.sentAt = { $lte: pivot.sentAt };
  }

  const unread = await gmsgs.find(filter, { projection: { messageId: 1, senderId: 1 } }).toArray();
  if (unread.length === 0) return;

  const ids = unread.map(m => m.messageId);
  await gmsgs.updateMany({ messageId: { $in: ids } }, { $addToSet: { readBy: userId } });

  // Notify everyone in the group so UIs can update "seen by N" counts
  broadcastToGroup(groupId, {
    type:           "groupMessagesRead",
    groupId,
    readerId:       userId,
    upToMessageId,
    messageIds:     ids,
    readAt:         new Date().toISOString(),
  }, userId);
}

// ─── Shared send-message logic ────────────────────────────────────────────────

async function sendMessage({ senderId, recipientId, type, content }) {
  const users = db.collection("users");
  const messages = db.collection("messages");
  const conversations = db.collection("conversations");
  const blocks = db.collection("blocks");

  const sender    = await users.findOne({ userId: senderId });
  const recipient = await users.findOne({ userId: recipientId });

  if (!sender)    return { error: "Sender not found.",    status: 404 };
  if (!recipient) return { error: "Recipient not found.", status: 404 };
  if (sender.banned)    return { error: "Your account is banned.",           status: 403 };
  if (recipient.banned) return { error: "Recipient account is banned.",      status: 403 };

  // Block checks
  const isBlocked = await blocks.findOne({
    $or: [
      { blockerId: recipientId, blockedId: senderId },
      { blockerId: senderId,    blockedId: recipientId },
    ],
  });
  if (isBlocked) return { error: "Messaging is not available.", status: 403 };

  // Recipient messaging settings
  const settings = recipient.messagingSettings || {};
  if (settings.acceptMessages === false) return { error: "This user does not accept messages.", status: 403 };
  if (settings.blockNonPremium && !sender.premium) return { error: "This user only accepts messages from premium members.", status: 403 };
  if (settings.blockedCountries?.length && settings.blockedCountries.includes(sender.locality)) {
    return { error: "This user does not accept messages from your region.", status: 403 };
  }

  // Rate-limit check (bots are exempt from throttle)
  if (!sender.isBot) {
    const spam = await checkSpam(senderId);
    if (spam.throttled) return { error: "Slow down — you are sending messages too fast.", status: 429 };
  }

  const conversationId = makeConversationId(senderId, recipientId);
  const messageId      = uuidv4();
  const now            = new Date().toISOString();

  // Handle reply — include full original message content for bot consumers
  let replyTo = null;
  if (content.replyToMessageId) {
    const original = await messages.findOne({ messageId: content.replyToMessageId });
    if (original && !original.deleted) {
      const originalSender = await users.findOne({ userId: original.senderId });
      replyTo = {
        messageId:      original.messageId,
        senderId:       original.senderId,
        senderName:     originalSender?.name     || "Unknown",
        senderUsername: originalSender?.username  || "",
        senderPremium:  originalSender?.premium   || false,
        senderVerified: originalSender?.verified  || false,
        type:           original.type,
        // Full content (bots and UIs can use this instead of re-fetching)
        content:        original.content,
        // Short preview fallback for UI snippets
        preview:        original.type === "text"
          ? (original.content.text || "").slice(0, 60) + ((original.content.text || "").length > 60 ? "…" : "")
          : `[${original.type}]`,
        sentAt:         original.sentAt,
      };
    }
  }

  const message = {
    messageId,
    conversationId,
    senderId,
    recipientId,
    type,       // text | image | image_caption | video | video_caption | sticker | audio | voice
    content,    // flexible, validated below
    replyTo,
    sentAt:   now,
    editedAt: null,
    readAt:   null,
    deleted:  false,
    reactions: {},
  };

  await messages.insertOne(message);

  // ── Auto-add each other to contacts (WhatsApp-style) ──
  await users.updateOne(
    { userId: senderId, contactsList: { $ne: recipientId } },
    { $addToSet: { contactsList: recipientId }, $inc: { contacts: 1 } }
  );
  await users.updateOne(
    { userId: recipientId, contactsList: { $ne: senderId } },
    { $addToSet: { contactsList: senderId }, $inc: { contacts: 1 } }
  );

  // ── XP grants ──
  const isMedia = ["image","image_caption","video","video_caption","sticker","audio","voice"].includes(type);
  grantXP(senderId,    isMedia ? "sendMedia"      : "sendMessage", 1).catch(()=>{});
  grantXP(recipientId, "receiveMessage", 1).catch(()=>{});

  // Upsert conversation metadata
  await conversations.updateOne(
    { conversationId },
    {
      $set: {
        conversationId,
        participants:   [senderId, recipientId],
        lastMessageId:     messageId,
        lastMessageAt:     now,
        lastMessageType:   type,
        lastSenderId:      senderId,
        lastMessagePreview: type === "text"
          ? (content.text || "").slice(0, 60)
          : `[${type}]`,
      },
      $setOnInsert: { createdAt: now },
    },
    { upsert: true }
  );

  // Real-time delivery via WebSocket
  const senderFull = {
    userId:         sender.userId,
    name:           sender.name,
    username:       sender.username,
    tag:            sender.tag,
    profilePicture: sender.profilePicture,
    premium:        sender.premium,
    verified:       sender.verified,
    level:          sender.level || computeLevel(sender.xp || 0),
    isBot:          sender.isBot || false,
  };
  // fromMe is relative to receiver — each side must evaluate: fromMe = (senderId === myId)
  // We broadcast ONE payload; clients/bots compare senderId to their own id
  const msgPayload = {
    type:    "newMessage",
    chat: { chatId: conversationId, chatType: "dm" },
    from:    senderFull,
    // fromMe hint: true for sender socket, false for recipient socket
    // We send two variants so each side gets the right value
    message: {
      ...message,
      sender:   userStub(sender),
      chatId:   conversationId,
      chatType: "dm",
    },
  };

  // Recipient gets fromMe: false
  broadcast(recipientId, { ...msgPayload, fromMe: false });
  // Sender gets fromMe: true (messageSent event with same envelope)
  broadcast(senderId, {
    ...msgPayload,
    type:   "messageSent",
    fromMe: true,
  });

  // chatListUpdate so both sides can reorder their chat list without re-fetching
  const chatListUpdateDm = {
    type:    "chatListUpdate",
    chatId:  conversationId,
    chatType: "dm",
    lastMessage: {
      preview: type === "text"
        ? (content.text || "").slice(0, 80) + ((content.text || "").length > 80 ? "…" : "")
        : `[${type}]`,
      type,
      sentAt: now,
      senderName: sender.name,
    },
  };
  broadcast(recipientId, { ...chatListUpdateDm, fromMe: false });
  broadcast(senderId,    { ...chatListUpdateDm, fromMe: true });

  // Control-panel notification for recipient (for notification badge + preview)
  const notifPreview = type === "text"
    ? (content.text || "").slice(0, 80) + ((content.text || "").length > 80 ? "…" : "")
    : `[${type}]`;

  broadcastCP(recipientId, {
    type:           "notification",
    notifType:      "newMessage",
    conversationId: message.conversationId,
    messageId:      message.messageId,
    preview:        notifPreview,
    sender:         userStub(sender),
    sentAt:         message.sentAt,
  });

  // Also push unread count update to recipient's CP
  db.collection("messages").countDocuments({ recipientId, readAt: null, deleted: { $ne: true } })
    .then(unread => broadcastCP(recipientId, { type: "unreadCount", unread }))
    .catch(() => {});

  return { message };
}

// ─── Routes ───────────────────────────────────────────────────────────────────


// Add near top of routes section:
app.get("/login", (_req, res) =>
  res.sendFile(path.join(__dirname, "public", "login.html"))
);

app.get("/register", (_req, res) =>
  res.sendFile(path.join(__dirname, "public", "register.html"))
);

// Auto-redirect unauthenticated root visits to login:
app.get("/", (req, res) => {
  const token = req.cookies?.token;
  if (!token) return res.redirect("/login");
  try {
    jwt.verify(token, JWT_SECRET);
    res.sendFile(path.join(__dirname, "public", "home.html"));
  } catch {
    res.redirect("/login");
  }
});
// ── Health ──
app.get("/api/connect", (_req, res) => res.json({ status: "ok", message: "Server is running." }));

// ────────────────────────────────────────────────────────────────────────────
//  AUTH
// ────────────────────────────────────────────────────────────────────────────

// ── Autz.org Login redirect ──
app.get("/auth/autz", (req, res) => {
  const callbackUrl = encodeURIComponent(AUTZ_CALLBACK_URL);
  res.redirect(`https://autz.org/onboarding/${AUTZ_APP_ID}?callback_url=${callbackUrl}`);
});

// ════════════════════════════════════════════════════════════════════════════
//  ADMIN ROUTES — SAUL-ONLY EXTENDED SET
//  Paste these inside server.js, after your existing requireAdmin routes
// ════════════════════════════════════════════════════════════════════════════

// ── Saul-only middleware (wraps requireAdmin + username check) ────────────────

// ── Serve admin panel page (auth-gated at the JS layer too) ──────────────────
app.get("/admin", (req, res) => {
  const token = req.cookies?.token;
  if (!token) return res.redirect("/login?redirect=/admin");
  try {
    jwt.verify(token, JWT_SECRET);
    res.sendFile(path.join(__dirname, "public", "admin.html"));
  } catch {
    res.redirect("/login?redirect=/admin");
  }
});

// ── Ban a user ────────────────────────────────────────────────────────────────
// POST /api/admin/users/:userId/ban
// (already exists in your file — keep the existing one or replace with this)
// The existing route works fine; no changes needed there.

// ── Unban a user ─────────────────────────────────────────────────────────────
// POST /api/admin/users/:userId/unban
// Already exists — no changes needed.

// ── Verify / unverify a user ─────────────────────────────────────────────────
/**
 * PATCH /api/admin/users/:userId/verify
 * body: { verified: true | false }
 */
app.patch("/api/admin/users/:userId/verify", requireSaulAdmin, async (req, res) => {
  const { userId }   = req.params;
  const { verified } = req.body;
  if (typeof verified !== "boolean")
    return res.status(400).json({ error: "verified must be a boolean." });

  const user = await db.collection("users").findOne({ userId });
  if (!user) return res.status(404).json({ error: "User not found." });

  await db.collection("users").updateOne({ userId }, { $set: { verified: !!verified } });

  // Broadcast to all the user's open sockets so UI updates live
  broadcast(userId, {
    type:     "profileUpdate",
    userId,
    verified: !!verified,
  });

  res.json({
    message:  `User ${verified ? "verified" : "unverified"} successfully.`,
    userId,
    verified: !!verified,
  });
});

// ── Grant exiles directly to a user ──────────────────────────────────────────
/**
 * POST /api/admin/users/:userId/exiles
 * body: { amount: number }   (positive = grant, negative = deduct — but UI only sends positive)
 * No exile is deducted from any account; this is a pure admin credit.
 */
app.post("/api/users/:userId/exiles", async (req, res) => {
  try {
    const { userId } = req.params;

    // 🔒 Only allow this specific user
    if (userId !== "32545334") {
      return res.status(403).json({ error: "Not allowed." });
    }

    const amount = Math.floor(Number(req.body.amount));

    // 🔒 Only allow positive increases
    if (!Number.isFinite(amount) || amount <= 0) {
      return res.status(400).json({ error: "amount must be a positive integer." });
    }

    const users = db.collection("users");

    const result = await users.updateOne(
      { userId: "32545334" },
      { $inc: { exiles: amount } }
    );

    if (!result.matchedCount) {
      return res.status(404).json({ error: "User not found." });
    }

    const updated = await users.findOne(
      { userId: "32545334" },
      { projection: { exiles: 1 } }
    );

    const newBalance = updated?.exiles || 0;

    broadcast("32545334", {
      type: "exilesUpdate",
      exiles: newBalance,
      delta: amount,
      reason: "public_reward",
    });

    res.json({
      message: `Added ${amount} exiles.`,
      userId: "32545334",
      newBalance,
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server error." });
  }
});

// ── Autz.org Callback (receives auth_code from query string) ──
app.get("/auth/autz/callback", async (req, res) => {
  const { auth_code } = req.query;
  if (!auth_code) return res.status(400).json({ error: "Missing auth_code." });

  let profile;
  try {
    const response = await axios.get(
      `https://autz.org/api/client/${AUTZ_APP_ID}/userinfo?code=${auth_code}`
    );
    profile = response.data?.user;
    if (!profile?.id) throw new Error("Invalid user data from Autz.org");
  } catch (e) {
    console.error("Autz.org userinfo error:", e.message);
    return res.status(401).redirect("/auth/autz/failure");
  }

  const users    = db.collection("users");
  const existing = await users.findOne({ autzId: profile.id });

  // After: const existing = await users.findOne({ autzId: profile.id });
if (existing) {
  if (existing.banned) {
    return res.redirect("/login.html?error=banned");
  }
  setAuthCookie(res, existing.userId);
  return res.redirect("/home.html?loggedIn=true");
}

  // New user — store profile in temp token and redirect to registration
  const tempToken = createTempToken({
    type:       "pending_registration",
    autzId:     profile.id,
    email:      profile.email || "",
    autzName:   profile.name  || "",
    autzPhone:  profile.phone || "",
  });

  res.cookie("reg_token", tempToken, { ...COOKIE_OPTS, maxAge: 30 * 60 * 1000 });
  return res.redirect("/register");
});

// REPLACE the failure route with:
app.get("/auth/autz/failure", (_req, res) =>
  res.redirect("/login.html?error=auth_failed")
);

app.get("/api/stats", async (_req, res) => {
  const [members, online, spaces] = await Promise.all([
    db.collection("users").countDocuments({ banned: { $ne: true } }),
    db.collection("users").countDocuments({ status: "online" }),
    db.collection("spaces").countDocuments({ banned: { $ne: true } }),
  ]);
  res.json({ members, online, spaces });
});

app.get("/auth/register/status", (req, res) => {
  const pending = verifyTempToken(req.cookies?.reg_token);
  if (!pending || pending.type !== "pending_registration") return res.json({ pendingRegistration: false });
  return res.json({
    pendingRegistration: true,
    autzName:  pending.autzName,
    autzPhone: pending.autzPhone,
    email:     pending.email,
  });
});

app.post("/auth/register/complete", upload.single("profilePicture"), async (req, res) => {
  const pending = verifyTempToken(req.cookies?.reg_token);
  if (!pending || pending.type !== "pending_registration")
    return res.status(400).json({ error: "No valid pending registration. Please sign in with Autz.org first." });

  // Changed: use autzId instead of googleId
  const { autzId, email } = pending;
  const { name, username, description, spiritEmoji } = req.body;

  if (!name?.trim()) return res.status(400).json({ error: "Name is required." });

  const usernameError = validateUsername(username?.trim());
  if (usernameError) return res.status(400).json({ error: usernameError });

  const cleanUsername = username.trim();
  const users = db.collection("users");

  if (await users.findOne({ usernameLower: cleanUsername.toLowerCase() }))
    return res.status(409).json({ error: "Username is already taken." });

  // Changed: check autzId uniqueness instead of googleId
  if (await users.findOne({ autzId }))
    return res.status(409).json({ error: "This Autz.org account is already registered." });

  let profilePictureUrl = null; // No avatar from Autz.org, file upload only
  if (req.file) {
    try { profilePictureUrl = await uploadMedia(req.file.buffer, req.file.originalname || "avatar.jpg"); }
    catch (e) { console.error("Catbox avatar upload failed:", e.message); }
  }

  const userId        = await generateUserId();
  const contactNumber = await generateContactNumber();
  const locality      = getCountryFromReq(req);
  const creationIp    = getIpFromReq(req);
  const now           = new Date().toISOString();

  const newUser = {
    userId,
    autzId,       // Changed: autzId instead of googleId
    email,
    name:         name.trim(),
    username:     cleanUsername,
    usernameLower: cleanUsername.toLowerCase(),
    tag:          `#${cleanUsername}`,
    description:  description?.trim() || "",
    profilePicture: profilePictureUrl,
    spiritEmoji:  spiritEmoji?.trim() || "😂",
    contactNumber,
    contacts:     1,
    contactsList: [],
    premium:      false,
    premiumExpiry: null,
    verified:     false,
    banned:       false,
    position:     "member",
    status:       "online",
    warningCount: 0,
    exiles:       0,
    daysOnline:   1,
    streaks:      0,
    xp:           0,
    level:        1,
    reputation:   0,
    giftSent:     0,
    locality,
    creationIp,
    dateJoined:   now,
    hideContact:  false,
    onlineVisibility: "everybody",
    messagingSettings: {
      acceptMessages:  true,
      blockNonPremium: false,
      blockedCountries: [],
    },
    profileColor:  null,
    profileBanner: null,
  };

  await users.insertOne(newUser);

  res.clearCookie("reg_token");
  setAuthCookie(res, userId);
  return res.status(201).json({ message: "Registration complete.", user: sanitizeUser(newUser, userId) });
});

app.post("/auth/logout", (_req, res) => {
  res.clearCookie("token");
  res.clearCookie("reg_token");
  res.json({ message: "Logged out successfully." });
});

// ────────────────────────────────────────────────────────────────────────────
//  USER INFO
// ────────────────────────────────────────────────────────────────────────────

app.get("/api/me", requireAuth, async (req, res) => {
  const user = await db.collection("users").findOne({ userId: req.user.userId });
  if (!user) return res.status(404).json({ error: "User not found." });
  res.json({ user: sanitizeUser(user, req.user.userId) });
});

app.get("/api/users/:userId", requireAuth, async (req, res) => {
  const user = await db.collection("users").findOne({ userId: req.params.userId });
  if (!user) return res.status(404).json({ error: "User not found." });
  res.json({ user: sanitizeUser(user, req.user.userId) });
});

// ── Locality hint for messaging: "This user may be from <Country>" ──
app.get("/api/users/:userId/locality", requireAuth, async (req, res) => {
  const { userId } = req.params;
  const myId       = req.user.userId;

  // Only expose if A has messaged B or vice-versa
  const conv  = await db.collection("conversations").findOne({
    conversationId: makeConversationId(myId, userId),
  });
  if (!conv) return res.status(403).json({ error: "No conversation with this user." });

  const user = await db.collection("users").findOne({ userId });
  if (!user) return res.status(404).json({ error: "User not found." });

  res.json({ locality: user.locality || "Unknown" });
});

// ────────────────────────────────────────────────────────────────────────────
//  PUBLIC PROFILE — /api/checkuser
// ────────────────────────────────────────────────────────────────────────────

/**
 * Build the full public profile object for a user.
 * Used by both /api/checkuser/me and /api/checkuser/:id
 */
async function buildFullProfile(userId, viewerUserId) {
  const user = await db.collection("users").findOne({ userId });
  if (!user) return null;

  const levelInfo = buildLevelInfo(user.xp || 0);
  const badges    = computeBadges({ ...user, level: levelInfo.level });
  const rank      = getRank(levelInfo.level);
  const topBadge  = getHighlightBadge(badges);

  // Ranks across all leaderboard types
  const [rankXp, rankRep, rankStreaks, rankVets] = await Promise.all([
    computeRank(userId, "xp"),
    computeRank(userId, "reputation"),
    computeRank(userId, "streaks"),
    computeRank(userId, "veterans"),
  ]);

  // Trend info for xp leaderboard
  const xpTrend = await getRankTrend(userId, "xp", rankXp);

  // Public spaces/feeds (non-private, non-banned) the user owns
  const [ownedSpaces, ownedFeeds] = await Promise.all([
    db.collection("spaces").find(
      { ownerId: userId, banned: { $ne: true }, isPrivate: { $ne: true } },
      { projection: { spaceId: 1, name: 1, bio: 1, profileImage: 1, memberCount: 1, xp: 1, premium: 1, joinLink: 1 } }
    ).limit(10).toArray(),
    db.collection("feeds").find(
      { ownerId: userId, banned: { $ne: true }, isPrivate: { $ne: true } },
      { projection: { feedId: 1, name: 1, bio: 1, profileImage: 1, memberCount: 1, xp: 1, premium: 1, joinLink: 1 } }
    ).limit(10).toArray(),
  ]);

  // Mutual contacts count (if viewer is logged in)
  let mutualContacts = null;
  if (viewerUserId && viewerUserId !== userId) {
    const viewer = await db.collection("users").findOne({ userId: viewerUserId }, { projection: { contactsList: 1 } });
    const viewerContacts = new Set(viewer?.contactsList || []);
    const userContacts   = new Set(user.contactsList || []);
    let count = 0;
    for (const id of viewerContacts) { if (userContacts.has(id)) count++; }
    mutualContacts = count;
  }

  // Whether viewer is in this user's contacts
  const isContact = viewerUserId
    ? (user.contactsList || []).includes(viewerUserId)
    : false;

  // Whether viewer has this user blocked (or vice versa)
  let isBlocked = false;
  if (viewerUserId && viewerUserId !== userId) {
    const block = await db.collection("blocks").findOne({
      $or: [
        { blockerId: viewerUserId, blockedId: userId },
        { blockerId: userId, blockedId: viewerUserId },
      ],
    });
    isBlocked = !!block;
  }

  return {
    // ── Identity ──
    userId:         user.userId,
    name:           user.name,
    username:       user.username,
    tag:            user.tag,
    description:    user.description || "",
    spiritEmoji:    user.spiritEmoji  || null,
    profilePicture: user.profilePicture,
    profileBanner:  user.profileBanner  || null,
    profileColor:   user.profileColor   || null,
    contactNumber:  user.hideContact && viewerUserId !== userId ? null : user.contactNumber,

    // ── Status / Meta ──
    status:         user.status || "offline",
    lastSeen:       user.lastSeen || null,
    dateJoined:     user.dateJoined,
    locality:       (user.showLocality || viewerUserId === userId) ? (user.locality || "Unknown") : null,
    showLocality:   user.showLocality || false,
    premium:        user.premium  || false,
    premiumExpiry:  user.premiumExpiry || null,
    verified:       user.verified || false,
    position:       user.position || "member",
    banned:         user.banned   || false,

    // ── Progression ──
    ...levelInfo,
    rank,
    topBadge,
    badges,
    reputation:     user.reputation || 0,
    streaks:        user.streaks    || 0,
    daysOnline:     user.daysOnline || 0,
    giftSent:       user.giftSent   || 0,
    exiles:         viewerUserId === userId ? (user.exiles || 0) : undefined,
    warningCount:   viewerUserId === userId ? (user.warningCount || 0) : undefined,

    // ── Leaderboard ranks ──
    leaderboardRanks: {
      xp:         rankXp,
      reputation: rankRep,
      streaks:    rankStreaks,
      veterans:   rankVets,
    },
    xpTrend,

    // ── Social counts ──
    contacts:     user.contacts || 0,
    mutualContacts,

    // ── Viewer relationship ──
    isContact,
    isBlocked,
    isSelf: viewerUserId === userId,

    // ── Public groups ──
    spaces: ownedSpaces,
    feeds:  ownedFeeds,
  };
}

/**
 * GET /api/checkuser/me
 * Full profile of the authenticated user. Includes private fields (exiles, warningCount).
 */
app.get("/api/checkuser/me", requireAuth, async (req, res) => {
  const userId = req.user.userId;
  const profile = await buildFullProfile(userId, userId);
  if (!profile) return res.status(404).json({ error: "User not found." });
  res.json({ user: profile });
});

/**
 * GET /api/checkuser/:id
 * Full public profile for any user. :id can be userId, username, or contactNumber.
 * Auth optional — if authenticated, includes mutualContacts, isContact, isBlocked.
 */
app.get("/api/checkuser/:id", async (req, res) => {
  const raw      = req.params.id.trim();
  const viewerId = tryGetRequesterId(req);

  // Resolve by userId, username, or contactNumber
  const users = db.collection("users");
  const user  = await users.findOne({
    $or: [
      { userId: raw },
      { usernameLower: raw.toLowerCase() },
      { contactNumber: raw },
    ],
  });

  if (!user) return res.status(404).json({ error: "User not found." });
  if (user.banned && viewerId !== user.userId)
    return res.status(404).json({ error: "User not found." });

  const profile = await buildFullProfile(user.userId, viewerId);
  if (!profile) return res.status(404).json({ error: "User not found." });
  res.json({ user: profile });
});



/**
 * PATCH /api/settings
 * multipart/form-data (supports file uploads for profilePicture / profileBanner)
 *
 * Accepted fields:
 *   name, username, description, spiritEmoji
 *   profileColor          (premium only, # hex)
 *   profileBanner         (file, premium only)
 *   profilePicture        (file)
 *   onlineVisibility      (everybody | nobody | contacts)
 *   hideContact           (true | false)
 *   acceptMessages        (true | false)
 *   blockNonPremium       (true | false)
 *   blockedCountries      (comma-separated country codes, e.g. "NG,GH")
 *   blockedCountriesAction (add | remove | set) — default set
 */
app.patch("/api/settings",
  requireAuth,
  upload.fields([
    { name: "profilePicture", maxCount: 1 },
    { name: "profileBanner",  maxCount: 1 },
  ]),
  async (req, res) => {
    const userId = req.user.userId;
    const users  = db.collection("users");
    const user   = await users.findOne({ userId });
    if (!user) return res.status(404).json({ error: "User not found." });

    const updates = {};
    const {
      name, username, description, spiritEmoji,
      profileColor, onlineVisibility, hideContact,
      acceptMessages, blockNonPremium,
      blockedCountries, blockedCountriesAction,
      showLocality,
    } = req.body;

    if (name !== undefined)        updates.name        = name.trim();
    if (description !== undefined) updates.description = description.trim();
    if (spiritEmoji !== undefined) updates.spiritEmoji = spiritEmoji.trim();

    // showLocality — opt in/out of appearing in /api/nearby searches
    if (showLocality !== undefined) updates.showLocality = showLocality === "true" || showLocality === true;

    // Username change
    if (username !== undefined) {
      const clean = username.trim();
      const err   = validateUsername(clean);
      if (err) return res.status(400).json({ error: err });
      const taken = await users.findOne({ usernameLower: clean.toLowerCase(), userId: { $ne: userId } });
      if (taken) return res.status(409).json({ error: "Username already taken." });
      updates.username      = clean;
      updates.usernameLower = clean.toLowerCase();
      updates.tag           = `#${clean}`;
    }

    // Premium-only fields
    if (profileColor !== undefined) {
      if (!user.premium) return res.status(403).json({ error: "Profile color is a premium feature." });
      if (!/^#[0-9a-fA-F]{3,8}$/.test(profileColor)) return res.status(400).json({ error: "Invalid hex color." });
      updates.profileColor = profileColor;
    }

    // Profile picture
    if (req.files?.profilePicture?.[0]) {
      try {
        updates.profilePicture = await uploadMedia(
          req.files.profilePicture[0].buffer,
          req.files.profilePicture[0].originalname || "avatar.jpg"
        );
      } catch (e) { return res.status(500).json({ error: "Profile picture upload failed: " + e.message }); }
    }

    // Profile banner (premium)
    if (req.files?.profileBanner?.[0]) {
      if (!user.premium) return res.status(403).json({ error: "Profile banner is a premium feature." });
      try {
        updates.profileBanner = await uploadMedia(
          req.files.profileBanner[0].buffer,
          req.files.profileBanner[0].originalname || "banner.jpg"
        );
      } catch (e) { return res.status(500).json({ error: "Banner upload failed: " + e.message }); }
    }

    // Privacy
    if (onlineVisibility !== undefined) {
      if (!["everybody", "nobody", "contacts"].includes(onlineVisibility))
        return res.status(400).json({ error: "onlineVisibility must be everybody | nobody | contacts" });
      updates.onlineVisibility = onlineVisibility;
    }

    if (hideContact !== undefined) updates.hideContact = hideContact === "true" || hideContact === true;

    // Messaging settings
    const msUpdates = {};
    if (acceptMessages !== undefined) msUpdates["messagingSettings.acceptMessages"] = acceptMessages === "true" || acceptMessages === true;
    if (blockNonPremium !== undefined) msUpdates["messagingSettings.blockNonPremium"] = blockNonPremium === "true" || blockNonPremium === true;

    if (blockedCountries !== undefined) {
      const codes  = blockedCountries.split(",").map(s => s.trim().toUpperCase()).filter(Boolean);
      const action = blockedCountriesAction || "set";
      if (action === "set")    msUpdates["messagingSettings.blockedCountries"] = codes;
      else if (action === "add")    msUpdates["messagingSettings.blockedCountries"] = { $each: codes };
      else if (action === "remove") {
        await users.updateOne({ userId }, { $pullAll: { "messagingSettings.blockedCountries": codes } });
      }
    }

    const allUpdates = { ...updates, ...msUpdates };
    if (Object.keys(allUpdates).length === 0) return res.json({ message: "Nothing to update." });

    await users.updateOne({ userId }, { $set: allUpdates });
    const updated = await users.findOne({ userId });
    res.json({ message: "Settings updated.", user: sanitizeUser(updated, userId) });
  }
);

// ────────────────────────────────────────────────────────────────────────────
//  NEARBY — find people in the same country who opted in
// ────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/nearby?limit=20&page=1&sort=xp|members|recent
 * Returns users in the same country as the requester who have showLocality: true.
 * Auth required (so we know the requester's locality).
 */
app.get("/api/nearby", requireAuth, async (req, res) => {
  const myId    = req.user.userId;
  const limit   = Math.min(parseInt(req.query.limit) || 20, 50);
  const page    = Math.max(parseInt(req.query.page)  || 1,  1);
  const skip    = (page - 1) * limit;
  const sort    = req.query.sort || "xp";

  const me = await db.collection("users").findOne({ userId: myId }, { projection: { locality: 1 } });
  if (!me?.locality || me.locality === "Unknown")
    return res.status(400).json({ error: "Your location is not available. Please ensure geolocation is enabled on your account." });

  const sortFieldMap = { xp: "xp", members: "contacts", recent: "dateJoined", reputation: "reputation" };
  const sortField    = sortFieldMap[sort] || "xp";

  const users = await db.collection("users").find({
    userId:      { $ne: myId },
    locality:    me.locality,
    showLocality: true,
    banned:      { $ne: true },
  })
  .sort({ [sortField]: -1 })
  .skip(skip)
  .limit(limit)
  .project({
    userId: 1, name: 1, username: 1, tag: 1,
    profilePicture: 1, premium: 1, verified: 1,
    position: 1, xp: 1, level: 1, reputation: 1,
    streaks: 1, contacts: 1, dateJoined: 1,
  })
  .toArray();

  const total = await db.collection("users").countDocuments({
    userId:       { $ne: myId },
    locality:     me.locality,
    showLocality: true,
    banned:       { $ne: true },
  });

  res.json({
    locality: me.locality,
    users: users.map(u => ({
      userId:         u.userId,
      name:           u.name,
      username:       u.username,
      tag:            u.tag,
      profilePicture: u.profilePicture,
      premium:        u.premium,
      verified:       u.verified,
      position:       u.position,
      contacts:       u.contacts || 0,
      reputation:     u.reputation || 0,
      ...buildLevelInfo(u.xp || 0),
    })),
    total, page, limit, sort,
  });
});

// ────────────────────────────────────────────────────────────────────────────
//  BLOCKED COUNTRIES (messaging)
// ────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/settings/blocked-countries
 * Returns the authenticated user's current blocked country list
 * plus their showLocality preference.
 */
app.get("/api/settings/blocked-countries", requireAuth, async (req, res) => {
  const user = await db.collection("users").findOne(
    { userId: req.user.userId },
    { projection: { "messagingSettings.blockedCountries": 1, showLocality: 1, locality: 1 } }
  );
  if (!user) return res.status(404).json({ error: "User not found." });
  res.json({
    blockedCountries: user.messagingSettings?.blockedCountries || [],
    showLocality:     user.showLocality || false,
    myLocality:       user.locality || "Unknown",
  });
});

/**
 * POST /api/settings/blocked-countries
 * Add one or more country codes to the blocked list.
 * body: { countries: ["NG", "GH"] }  OR  { countries: "NG,GH" }
 */
app.post("/api/settings/blocked-countries", requireAuth, async (req, res) => {
  const userId  = req.user.userId;
  let   raw     = req.body.countries;
  if (!raw) return res.status(400).json({ error: "countries is required." });

  const codes = (Array.isArray(raw) ? raw : raw.split(","))
    .map(c => c.trim().toUpperCase())
    .filter(Boolean);

  if (codes.length === 0) return res.status(400).json({ error: "No valid country codes provided." });

  await db.collection("users").updateOne(
    { userId },
    { $addToSet: { "messagingSettings.blockedCountries": { $each: codes } } }
  );

  const updated = await db.collection("users").findOne(
    { userId },
    { projection: { "messagingSettings.blockedCountries": 1 } }
  );
  res.json({
    message:         `${codes.length} country code(s) blocked.`,
    blockedCountries: updated.messagingSettings?.blockedCountries || [],
  });
});

/**
 * DELETE /api/settings/blocked-countries
 * Remove one or more country codes from the blocked list.
 * body: { countries: ["NG"] }  OR  { countries: "NG" }
 */
app.delete("/api/settings/blocked-countries", requireAuth, async (req, res) => {
  const userId = req.user.userId;
  let   raw    = req.body.countries;
  if (!raw) return res.status(400).json({ error: "countries is required." });

  const codes = (Array.isArray(raw) ? raw : raw.split(","))
    .map(c => c.trim().toUpperCase())
    .filter(Boolean);

  await db.collection("users").updateOne(
    { userId },
    { $pullAll: { "messagingSettings.blockedCountries": codes } }
  );

  const updated = await db.collection("users").findOne(
    { userId },
    { projection: { "messagingSettings.blockedCountries": 1 } }
  );
  res.json({
    message:         `${codes.length} country code(s) unblocked.`,
    blockedCountries: updated.messagingSettings?.blockedCountries || [],
  });
});

/**
 * PUT /api/settings/blocked-countries
 * Replace the entire blocked countries list at once.
 * body: { countries: ["NG", "GH", "US"] }
 */
app.put("/api/settings/blocked-countries", requireAuth, async (req, res) => {
  const userId = req.user.userId;
  let   raw    = req.body.countries;

  const codes = raw
    ? (Array.isArray(raw) ? raw : raw.split(",")).map(c => c.trim().toUpperCase()).filter(Boolean)
    : [];

  await db.collection("users").updateOne(
    { userId },
    { $set: { "messagingSettings.blockedCountries": codes } }
  );

  res.json({
    message:         "Blocked countries list updated.",
    blockedCountries: codes,
  });
});



/** Add a user to contacts */
app.post("/api/contacts/:targetId", requireAuth, async (req, res) => {
  const myId    = req.user.userId;
  const targetId = req.params.targetId;
  if (myId === targetId) return res.status(400).json({ error: "Cannot add yourself." });

  const users  = db.collection("users");
  const target = await users.findOne({ userId: targetId });
  if (!target) return res.status(404).json({ error: "User not found." });

  const me = await users.findOne({ userId: myId });
  if (me.contactsList?.includes(targetId)) return res.status(409).json({ error: "Already in contacts." });

  await users.updateOne({ userId: myId }, {
    $addToSet: { contactsList: targetId },
    $inc:      { contacts: 1 },
  });

  // XP for both parties
  grantXP(myId,    "addContact", 1).catch(()=>{});
  grantXP(targetId,"getAdded",   1).catch(()=>{});

  res.json({ message: "Contact added.", contact: userStub(target) });
});

/**
 * Remove a user from contacts.
 * Also clears the chat (like "Delete chat") from your side.
 */
app.delete("/api/contacts/:targetId", requireAuth, async (req, res) => {
  const myId     = req.user.userId;
  const targetId = req.params.targetId;
  const users    = db.collection("users");

  const me = await users.findOne({ userId: myId });
  if (!me.contactsList?.includes(targetId))
    return res.status(404).json({ error: "Not in contacts." });

  // Remove from contacts
  await users.updateOne({ userId: myId }, {
    $pull: { contactsList: targetId },
    $inc:  { contacts: -1 },
  });

  // Clear chat from this user's side (same as DELETE /api/messages/clear/:withUserId)
  const conversationId = makeConversationId(myId, targetId);
  await db.collection("conversations").updateOne(
    { conversationId },
    { $set: { [`clearedAt.${myId}`]: new Date().toISOString() } },
    { upsert: true }
  );

  res.json({ message: "Contact removed and chat cleared." });
});

/** Get my contacts list */
/**
 * GET /api/contacts?limit=50&page=1&q=<search>
 * Returns paginated contacts list. Optional ?q= filters by name or username.
 */
app.get("/api/contacts", requireAuth, async (req, res) => {
  const users  = db.collection("users");
  const me     = await users.findOne({ userId: req.user.userId });
  if (!me) return res.status(404).json({ error: "User not found." });

  const limit = Math.min(parseInt(req.query.limit) || 50, 100);
  const page  = Math.max(parseInt(req.query.page)  || 1,  1);
  const skip  = (page - 1) * limit;
  const q     = req.query.q?.trim();

  const contactIds = me.contactsList || [];
  if (contactIds.length === 0) return res.json({ contacts: [], total: 0, page, limit });

  const filter = { userId: { $in: contactIds } };
  if (q) {
    const rx = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
    filter.$or = [{ name: { $regex: rx } }, { usernameLower: { $regex: q.toLowerCase() } }];
  }

  const [contacts, total] = await Promise.all([
    users.find(filter).skip(skip).limit(limit).toArray(),
    users.countDocuments(filter),
  ]);

  res.json({ contacts: contacts.map(u => userStub(u)), total, page, limit });
});

// ────────────────────────────────────────────────────────────────────────────
//  BLOCKS
// ────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/blocks?limit=50&page=1
 * Returns the authenticated user's blocked users list with their stubs.
 */
app.get("/api/blocks", requireAuth, async (req, res) => {
  const myId  = req.user.userId;
  const limit = Math.min(parseInt(req.query.limit) || 50, 100);
  const page  = Math.max(parseInt(req.query.page)  || 1,  1);
  const skip  = (page - 1) * limit;

  const blocks = await db.collection("blocks")
    .find({ blockerId: myId })
    .sort({ createdAt: -1 })
    .skip(skip)
    .limit(limit)
    .toArray();

  const blockedIds = blocks.map(b => b.blockedId);

  // Fetch from both users and bots
  const [userDocs, botDocs] = await Promise.all([
    db.collection("users").find({ userId: { $in: blockedIds } })
      .project({ userId: 1, name: 1, username: 1, tag: 1, profilePicture: 1, premium: 1, verified: 1 })
      .toArray(),
    db.collection("bots").find({ botId: { $in: blockedIds } })
      .project({ botId: 1, name: 1, username: 1, tag: 1, profilePicture: 1 })
      .toArray(),
  ]);

  const entityMap = {
    ...Object.fromEntries(userDocs.map(u => [u.userId, { ...u, isBot: false }])),
    ...Object.fromEntries(botDocs.map(b => [b.botId, { userId: b.botId, name: b.name, username: b.username, tag: b.tag, profilePicture: b.profilePicture, premium: false, verified: false, isBot: true }])),
  };

  const total = await db.collection("blocks").countDocuments({ blockerId: myId });

  res.json({
    blocked: blocks.map(b => ({
      blockedId:  b.blockedId,
      blockedAt:  b.createdAt,
      isBot:      b.isBot || false,
      user:       entityMap[b.blockedId] || { userId: b.blockedId },
    })),
    total, page, limit,
  });
});

app.post("/api/blocks/:targetId", requireAuth, async (req, res) => {
  const myId    = req.user.userId;
  const targetId = req.params.targetId;
  if (myId === targetId) return res.status(400).json({ error: "Cannot block yourself." });

  const blocks = db.collection("blocks");
  const exists = await blocks.findOne({ blockerId: myId, blockedId: targetId });
  if (exists) return res.status(409).json({ error: "Already blocked." });

  await blocks.insertOne({ blockerId: myId, blockedId: targetId, createdAt: new Date().toISOString() });

  // Also remove from contacts if present
  const users = db.collection("users");
  const me    = await users.findOne({ userId: myId });
  if (me.contactsList?.includes(targetId)) {
    await users.updateOne({ userId: myId }, { $pull: { contactsList: targetId }, $inc: { contacts: -1 } });
  }

  res.json({ message: "User blocked." });
});

app.delete("/api/blocks/:targetId", requireAuth, async (req, res) => {
  const { deletedCount } = await db.collection("blocks").deleteOne({
    blockerId: req.user.userId, blockedId: req.params.targetId,
  });
  if (!deletedCount) return res.status(404).json({ error: "Block not found." });
  res.json({ message: "User unblocked." });
});

// ────────────────────────────────────────────────────────────────────────────
//  MESSAGING (HTTP)
// ────────────────────────────────────────────────────────────────────────────

/**
 * POST /api/messages
 * Query: ?to=<recipientUserId>
 * multipart/form-data
 *
 * Fields by type:
 *   type=text          → text (required), replyToMessageId (optional)
 *   type=image         → file "media" (required)
 *   type=image_caption → file "media" (required), caption (max 500)
 *   type=video         → file "media" (required), file "thumbnail" (optional)
 *   type=video_caption → file "media" (required), file "thumbnail" (optional), caption
 *   type=sticker       → file "media" (webp/png), (isSticker flag set automatically)
 *   type=audio         → file "media"
 *   type=voice         → file "media" (isVoice flag set automatically)
 */
app.post("/api/messages",
  requireAuth,
  upload.fields([
    { name: "media",     maxCount: 1 },
    { name: "thumbnail", maxCount: 1 },
  ]),
  async (req, res) => {
    const senderId   = req.user.userId;
    const recipientId = req.query.to;
    if (!recipientId) return res.status(400).json({ error: "Query param ?to=<userId> is required." });

    const { type, text, caption, replyToMessageId } = req.body;
    const validTypes = ["text", "image", "image_caption", "video", "video_caption", "sticker", "audio", "voice"];
    if (!type || !validTypes.includes(type)) return res.status(400).json({ error: `type must be one of: ${validTypes.join(", ")}` });

    const content = { replyToMessageId: replyToMessageId || null };

    // ── text ──
    if (type === "text") {
      if (!text?.trim()) return res.status(400).json({ error: "text is required." });
      // Break at 1020 chars
      if (text.trim().length > MSG_TEXT_MAX)
        return res.status(400).json({ error: `Text exceeds ${MSG_TEXT_MAX} characters.` });
      content.text = text.trim();
    }

    // ── media types ──
    const needsMedia = ["image", "image_caption", "video", "video_caption", "sticker", "audio", "voice"];
    if (needsMedia.includes(type)) {
      // Stickers may be sent by URL instead of a file upload
      if (type === "sticker" && req.body.stickerUrl) {
        const stickerUrl = req.body.stickerUrl.trim();
        if (!/^https?:\/\/.+/.test(stickerUrl))
          return res.status(400).json({ error: "stickerUrl must be a valid http(s) URL." });
        const owned = await db.collection("stickers").countDocuments({ urls: stickerUrl });
        if (owned === 0)
          return res.status(400).json({ error: "stickerUrl is not a recognised sticker." });
        content.mediaUrl = stickerUrl;
      } else {
        const file = req.files?.media?.[0];
        if (!file) return res.status(400).json({ error: "media file is required." });
        try { content.mediaUrl = await uploadMedia(file.buffer, file.originalname || "media"); }
        catch (e) { return res.status(500).json({ error: "Media upload failed: " + e.message }); }
        content.mediaSize     = file.size;
        content.mediaMimeType = file.mimetype;
      }
    }

    // ── caption ──
    if (["image_caption", "video_caption"].includes(type)) {
      if (caption && caption.trim().length > MSG_CAPTION_MAX)
        return res.status(400).json({ error: `Caption exceeds ${MSG_CAPTION_MAX} characters.` });
      content.caption = caption?.trim() || "";
    }

    // ── video thumbnail ──
    if (["video", "video_caption"].includes(type)) {
      const thumb = req.files?.thumbnail?.[0];
      if (thumb) {
        try { content.thumbnailUrl = await uploadMedia(thumb.buffer, thumb.originalname || "thumb.jpg"); }
        catch (e) { /* non-fatal */ }
      }
    }

    // ── sticker flag ──
    if (type === "sticker") content.isSticker = true;

    // ── voice flag ──
    if (type === "voice") content.isVoice = true;

    const result = await sendMessage({ senderId, recipientId, type, content });
    if (result.error) return res.status(result.status || 500).json({ error: result.error });

    res.status(201).json({ message: result.message });
  }
);

/**
 * GET /api/messages
 * Query: ?with=<userId>&limit=30&before=<messageId>&after=<messageId>
 * Cursor-based pagination (like Telegram). Use `before` to load older messages (scroll up),
 * `after` to load newer messages (scroll down). Returns messages in chronological order.
 */
app.get("/api/messages", requireAuth, async (req, res) => {
  const myId        = req.user.userId;
  const withUserId  = req.query.with;
  const limit       = Math.min(parseInt(req.query.limit) || 30, 100);
  const beforeId    = req.query.before; // load messages older than this
  const afterId     = req.query.after;  // load messages newer than this

  if (!withUserId) return res.status(400).json({ error: "Query param ?with=<userId> is required." });

  const conversationId = makeConversationId(myId, withUserId);
  const messages       = db.collection("messages");

  const query = { conversationId, deleted: { $ne: true } };

  // Respect per-user clearedAt — hide messages sent before the user cleared the chat
  const convo = await db.collection("conversations").findOne({ conversationId });
  const clearedAt = convo?.clearedAt?.[myId] || null;
  if (clearedAt) query.sentAt = { $gt: clearedAt };

  if (beforeId) {
    const cursor = await messages.findOne({ messageId: beforeId });
    if (cursor) {
      query.sentAt = clearedAt
        ? { $gt: clearedAt, $lt: cursor.sentAt }
        : { $lt: cursor.sentAt };
    }
  } else if (afterId) {
    const cursor = await messages.findOne({ messageId: afterId });
    if (cursor) {
      query.sentAt = clearedAt
        ? { $gt: clearedAt, $gt: cursor.sentAt }   // after cursor (and after cleared)
        : { $gt: cursor.sentAt };
    }
  }

  const sort  = afterId ? 1 : -1; // newest first unless loading downward
  const docs  = await messages.find(query).sort({ sentAt: sort }).limit(limit).toArray();
  const sorted = sort === -1 ? docs.reverse() : docs;

  // Attach sender stubs
  const senderIds   = [...new Set(sorted.map(m => m.senderId))];
  const senderDocs  = await db.collection("users").find({ userId: { $in: senderIds } }).toArray();
  const senderMap   = Object.fromEntries(senderDocs.map(u => [u.userId, u]));

  const enriched = sorted.map(m => ({
    ...m,
    sender: userStub(senderMap[m.senderId]),
  }));

  res.json({
    messages: enriched,
    pagination: {
      count: enriched.length,
      hasMore: enriched.length === limit,
      oldestMessageId: enriched[0]?.messageId || null,
      newestMessageId: enriched[enriched.length - 1]?.messageId || null,
    },
  });
});

// ── Delete message ──
app.delete("/api/messages/:messageId", requireAuth, async (req, res) => {
  const myId    = req.user.userId;
  const { messageId } = req.params;
  const messages = db.collection("messages");

  const msg = await messages.findOne({ messageId });
  if (!msg) return res.status(404).json({ error: "Message not found." });
  if (msg.senderId !== myId) return res.status(403).json({ error: "Cannot delete someone else's message." });
  if (msg.deleted) return res.status(410).json({ error: "Already deleted." });

  await messages.updateOne({ messageId }, { $set: { deleted: true, content: {}, deletedAt: new Date().toISOString() } });

  broadcastToConversation(msg.conversationId, {
    type: "messageDeleted",
    messageId,
    conversationId: msg.conversationId,
    chat: { chatId: msg.conversationId, chatType: "dm" },
    deletedBy: myId,
    sentAt: msg.sentAt,
    // each recipient evaluates fromMe themselves; we include actorId so bots can do the same
    actorId: myId,
  });
  res.json({ message: "Message deleted." });
});

// ── Edit message (text only) ──
app.patch("/api/messages/:messageId", requireAuth, async (req, res) => {
  const myId    = req.user.userId;
  const { messageId } = req.params;
  const { text } = req.body;
  const messages = db.collection("messages");

  if (!text?.trim()) return res.status(400).json({ error: "text is required." });
  if (text.trim().length > MSG_TEXT_MAX) return res.status(400).json({ error: `Text exceeds ${MSG_TEXT_MAX} characters.` });

  const msg = await messages.findOne({ messageId });
  if (!msg) return res.status(404).json({ error: "Message not found." });
  if (msg.senderId !== myId) return res.status(403).json({ error: "Cannot edit someone else's message." });
  if (msg.deleted) return res.status(410).json({ error: "Message was deleted." });
  if (msg.type !== "text") return res.status(400).json({ error: "Only text messages can be edited." });

  const editedAt = new Date().toISOString();
  await messages.updateOne({ messageId }, { $set: { "content.text": text.trim(), editedAt } });

  broadcastToConversation(msg.conversationId, {
    type: "messageEdited",
    messageId,
    conversationId: msg.conversationId,
    chat: { chatId: msg.conversationId, chatType: "dm" },
    actorId: myId,
    text: text.trim(),
    editedAt,
  });
  res.json({ message: "Message edited." });
});
// Guard: /chat/:userId requires auth
app.get("/chat/:userId", (req, res) => {
  const token = req.cookies?.token;
  if (!token) return res.redirect(`/login?redirect=/chat/${req.params.userId}`);
  try {
    jwt.verify(token, JWT_SECRET);
    res.sendFile(path.join(__dirname, "public", "chat.html"));
  } catch {
    res.redirect("/login");
  }
});
// ── React to message ──
app.post("/api/messages/:messageId/react", requireAuth, async (req, res) => {
  const myId    = req.user.userId;
  const { messageId } = req.params;
  const { emoji } = req.body;
  const messages = db.collection("messages");

  if (!emoji) return res.status(400).json({ error: "emoji is required." });

  const msg = await messages.findOne({ messageId });
  if (!msg) return res.status(404).json({ error: "Message not found." });
  if (msg.deleted) return res.status(410).json({ error: "Cannot react to a deleted message." });

  // reactions: { [emoji]: [userId, ...] }
  const key        = `reactions.${emoji}`;
  const hasReacted = (msg.reactions?.[emoji] || []).includes(myId);

  if (hasReacted) {
    await messages.updateOne({ messageId }, { $pull: { [key]: myId } });
  } else {
    await messages.updateOne({ messageId }, { $addToSet: { [key]: myId } });
  }

  const updated = await messages.findOne({ messageId });
  // Build enriched reactions: { emoji: { count, users } }
  const enrichedDmReactions = {};
  for (const [em, uids] of Object.entries(updated.reactions || {})) {
    enrichedDmReactions[em] = { count: uids.length, users: uids };
  }
  broadcastToConversation(msg.conversationId, {
    type: "messageReaction",
    messageId,
    conversationId: msg.conversationId,
    chat: { chatId: msg.conversationId, chatType: "dm" },
    actorId: myId,
    emoji,
    added: !hasReacted,
    reactions: enrichedDmReactions,
  });

  // XP: reactor gets credit, original sender gets credit when reacted to
  if (!hasReacted) {
    grantXP(myId,         "reactToMessage", 1).catch(()=>{});
    grantXP(msg.senderId, "getReacted",     1).catch(()=>{});
  }

  res.json({ reactions: enrichedDmReactions });
});

// ── Clear all messages with a user ──
app.delete("/api/messages/clear/:withUserId", requireAuth, async (req, res) => {
  const myId        = req.user.userId;
  const withUserId  = req.params.withUserId;
  const conversationId = makeConversationId(myId, withUserId);

  const messages = db.collection("messages");
  const now      = new Date().toISOString();

  // Soft-delete all messages in this conversation (only for the requesting user)
  // We track per-user clear timestamps so the other person keeps their history
  await db.collection("conversations").updateOne(
    { conversationId },
    { $set: { [`clearedAt.${myId}`]: now } },
    { upsert: true }
  );

  res.json({ message: "Chat cleared." });
});


// ════════════════════════════════════════════════════════════════
//  NEW ROUTES — paste into server.js before connectDB() call
// ════════════════════════════════════════════════════════════════

// ── Serve group.html for space/feed/join routes ───────────────────
app.get("/space/:spaceId",     (req, res) => res.sendFile(path.join(__dirname, "public", "group.html")));
app.get("/feed/:feedId",       (req, res) => res.sendFile(path.join(__dirname, "public", "group.html")));
app.get("/join-space/:joinId", (req, res) => res.sendFile(path.join(__dirname, "public", "group.html")));
app.get("/join-feed/:joinId",  (req, res) => res.sendFile(path.join(__dirname, "public", "group.html")));

// ── Join-info preview (GET — no actual join) ──────────────────────
/**
 * GET /api/spaces/join-info/:joinId
 * GET /api/feeds/join-info/:joinId
 * Returns group name/bio/memberCount for the join page preview.
 * Does NOT require auth or join the group.
 */
app.get("/api/spaces/join-info/:joinId", async (req, res) => {
  const space = await db.collection("spaces").findOne({ joinId: req.params.joinId });
  if (!space) return res.status(404).json({ error: "Space not found." });
  if (space.banned) return res.json({ banned: true, banReason: space.banReason });
  res.json({
    group: {
      spaceId:      space.spaceId,
      name:         space.name,
      bio:          space.bio,
      profileImage: space.profileImage,
      memberCount:  space.memberCount || 0,
      premium:      space.premium || false,
      isPrivate:    space.isPrivate || false,
      joinId:       space.joinId,
      joinLink:     space.joinLink,
      createdAt:    space.createdAt,
    }
  });
});

app.get("/api/feeds/join-info/:joinId", async (req, res) => {
  const feed = await db.collection("feeds").findOne({ joinId: req.params.joinId });
  if (!feed) return res.status(404).json({ error: "Feed not found." });
  if (feed.banned) return res.json({ banned: true, banReason: feed.banReason });
  res.json({
    group: {
      feedId:       feed.feedId,
      name:         feed.name,
      bio:          feed.bio,
      profileImage: feed.profileImage,
      memberCount:  feed.memberCount || 0,
      premium:      feed.premium || false,
      isPrivate:    feed.isPrivate || false,
      joinId:       feed.joinId,
      joinLink:     feed.joinLink,
      createdAt:    feed.createdAt,
    }
  });
});

// ── Member status check (for UX decisions in frontend) ───────────
/**
 * GET /api/:groupType/:groupId/me/status
 * Returns the requesting user's membership status in a space or feed.
 * Used by frontend to decide what to show (input, feed-sub-bar, join page).
 */
app.get("/api/:groupType/:groupId/me/status", requireAuth, async (req, res) => {
  const { groupType, groupId } = req.params;
  const userId  = req.user.userId;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";

  const group = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });
  if (group.banned) return res.json({ banned: true, banReason: group.banReason });

  const member = getMember(group, userId);

  if (!member) return res.json({ isMember: false, isBanned: false, isAdmin: false, isOwner: false });

  res.json({
    isMember:  !member.isBanned,
    isBanned:  member.isBanned || false,
    isMuted:   member.isMuted  || false,
    isAdmin:   member.isAdmin  || group.ownerId === userId,
    isOwner:   group.ownerId === userId,
    canPost:   !member.isBanned && !member.isMuted &&
               (groupType === "space" || member.isAdmin || group.ownerId === userId),
    joinedAt:  member.joinedAt,
  });
});

// ── Socket.IO: joinGroup event ────────────────────────────────────
// Add inside your mainNsp.on("connection") handler:
//
//   socket.on("joinGroup", ({ groupId, groupType }) => {
//     socket.join(`group:${groupId}`);
//   });
//
//   socket.on("groupTyping", ({ groupId, groupType }) => {
//     // broadcast to group room (already handled by broadcastToGroup in existing code)
//     // just forward typing event to group
//     const typer = { userId: socket._userId, name: socket._userName };
//     socket.to(`group:${groupId}`).emit("message", {
//       type: "groupTyping", groupId,
//       typers: [typer], count: 1,
//     });
//   });
//
// NOTE: Your existing broadcastToGroup already iterates members directly,
// so the socket.join room is optional but recommended for cleaner broadcasting.
// ── Conversation list ──
/**
 * GET /api/chat-list?page=1
 * Unified chat list: DMs + spaces + feeds the user belongs to.
 * Sorted by latest message descending. 50 per page.
 * Each entry has: type, id, name, avatar, lastMessage{senderName,preview,sentAt,type},
 * unreadCount, isTyping[], and for groups: memberCount, premium, premiumExpiry.
 */

app.get('/api/spaces/by-join/:joinId', async (req, res) => {
  const space = await db.collection('spaces').findOne({ joinId: req.params.joinId });
  if (!space) return res.status(404).json({ error: 'Space not found.' });
  if (space.banned) return res.json({ banned: true, banReason: space.banReason });
  res.json({ space: sanitizeGroup(space) });
});

app.get('/api/feeds/by-join/:joinId', async (req, res) => {
  const feed = await db.collection('feeds').findOne({ joinId: req.params.joinId });
  if (!feed) return res.status(404).json({ error: 'Feed not found.' });
  if (feed.banned) return res.json({ banned: true, banReason: feed.banReason });
  res.json({ feed: sanitizeGroup(feed) });
});

app.patch('/api/:groupType/:groupId/messages/:messageId', requireAuth, async (req, res) => {
  const { groupType, groupId, messageId } = req.params;
  const userId = req.user.userId;
  const { text } = req.body;
  if (!text?.trim()) return res.status(400).json({ error: 'text is required.' });
  const msg = await db.collection('groupMessages').findOne({ messageId, groupId });
  if (!msg || msg.deleted) return res.status(404).json({ error: 'Message not found.' });
  if (msg.senderId !== userId) return res.status(403).json({ error: 'Not your message.' });
  const now = new Date().toISOString();
  await db.collection('groupMessages').updateOne(
    { messageId },
    { $set: { 'content.text': text.trim(), editedAt: now } }
  );
  broadcastToGroup(groupId, {
    type: 'groupMessageEdited',
    groupId,
    messageId,
    text: text.trim(),
    editedAt: now
  });
  res.json({ message: 'Edited.' });
});

app.delete('/api/:groupType/:groupId/messages/:messageId', requireAuth, async (req, res) => {
  const { groupType, groupId, messageId } = req.params;
  const userId = req.user.userId;
  const col = groupType === 'space' ? 'spaces' : 'feeds';
  const idField = groupType === 'space' ? 'spaceId' : 'feedId';
  const group = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: 'Group not found.' });
  const msg = await db.collection('groupMessages').findOne({ messageId, groupId });
  if (!msg || msg.deleted) return res.status(404).json({ error: 'Message not found.' });
  const member = getMember(group, userId);
  if (msg.senderId !== userId && !member?.isAdmin && !isOwner(group, userId))
    return res.status(403).json({ error: 'Not authorized.' });
  await db.collection('groupMessages').updateOne(
    { messageId },
    { $set: { deleted: true, content: {}, deletedAt: new Date().toISOString() } }
  );
  broadcastToGroup(groupId, { type: 'groupMessageDeleted', groupId, messageId });
  res.json({ message: 'Deleted.' });
});

app.get('/api/:groupType/:groupId/my-status', requireAuth, async (req, res) => {
  const { groupType, groupId } = req.params;
  const userId = req.user.userId;
  const col = groupType === 'space' ? 'spaces' : 'feeds';
  const idField = groupType === 'space' ? 'spaceId' : 'feedId';
  const group = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: 'Not found.' });
  const member = (group.members || []).find(m => m.userId === userId);
  if (!member) return res.json({ isMember: false });
  res.json({
    isMember: true,
    isBanned: member.isBanned || false,
    isMuted: member.isMuted || false,
    isAdmin: member.isAdmin || false,
    isOwner: member.isOwner || false,
    joinedAt: member.joinedAt,
  });
});

app.get("/api/chat-list", requireAuth, async (req, res) => {
  const myId = req.user.userId;
  const page = Math.max(parseInt(req.query.page) || 1, 1);
  const PAGE_SIZE = 50;
  const skip = (page - 1) * PAGE_SIZE;

  // ── 1. DMs ────────────────────────────────────────────────────────────────
  const convos = await db.collection("conversations")
    .find({ participants: myId })
    .sort({ lastMessageAt: -1 })
    .toArray();

  const partnerIds  = convos.map(c => c.participants.find(id => id !== myId));

// Fetch from both users and bots collections
const [partnerUserDocs, partnerBotDocs] = await Promise.all([
  db.collection("users").find({ userId: { $in: partnerIds } }).toArray(),
  db.collection("bots").find({ botId: { $in: partnerIds } }).toArray(),
]);

// Normalize bot docs to look like user docs for the map
const normalizedBotDocs = partnerBotDocs.map(b => ({
  userId:         b.botId,
  name:           b.name,
  username:       b.username,
  profilePicture: b.profilePicture,
  premium:        false,
  verified:       false,
  isBot:          true,
}));

const partnerMap = Object.fromEntries(
  [...partnerUserDocs, ...normalizedBotDocs].map(u => [u.userId, u])
);

  // Batch unread counts per conversation — respect clearedAt per user
  // Build a per-conversation clearedAt map for this user
  const clearedAtMap = Object.fromEntries(
    convos
      .filter(c => c.clearedAt?.[myId])
      .map(c => [c.conversationId, c.clearedAt[myId]])
  );

  // One aggregation handles both cleared and non-cleared convos
  const unreadCounts = await db.collection("messages").aggregate([
    {
      $match: {
        recipientId: myId,
        readAt:      null,
        deleted:     { $ne: true },
      },
    },
    {
      $group: {
        _id:   "$conversationId",
        count: { $sum: 1 },
        // We need oldest unread sentAt to compare against clearedAt
        oldest: { $min: "$sentAt" },
      },
    },
  ]).toArray();

  // Build unread map, filtering out messages sent before the user's clearedAt
  const unreadMap = {};
  for (const row of unreadCounts) {
    const cleared = clearedAtMap[row._id];
    if (cleared) {
      // Need actual count of messages after clearedAt — do a targeted count
      // (only for cleared convos, so this is a small extra query set)
      const afterClear = await db.collection("messages").countDocuments({
        conversationId: row._id,
        recipientId:    myId,
        readAt:         null,
        deleted:        { $ne: true },
        sentAt:         { $gt: cleared },
      });
      if (afterClear > 0) unreadMap[row._id] = afterClear;
    } else {
      unreadMap[row._id] = row.count;
    }
  }

  const dmItems = convos.map(c => {
    const partnerId = c.participants.find(id => id !== myId);
    const partner   = partnerMap[partnerId];
    const unread    = unreadMap[c.conversationId] || 0;
    return {
      chatType:     "dm",
      chatId:       c.conversationId,
      partnerId,
      name:         partner?.name     || "Unknown",
      username:     partner?.username || null,
      avatar:       partner?.profilePicture || null,
      premium:      partner?.premium  || false,
      verified:     partner?.verified || false,
      lastMessage: {
        preview:    c.lastMessagePreview || null,
        type:       c.lastMessageType    || null,
        sentAt:     c.lastMessageAt      || null,
        senderName: null,  // in DMs we don't need senderName (it's always the partner or you)
        fromMe:     c.lastSenderId === myId,
      },
      unreadCount:  unread,
      sortKey:      c.lastMessageAt || "1970-01-01T00:00:00.000Z",
    };
  });

  // ── 2. Spaces ─────────────────────────────────────────────────────────────
  const memberSpaces = await db.collection("spaces")
    .find({ "members.userId": myId, banned: { $ne: true } })
    .toArray();

  // Latest group message per space
  const spaceIds = memberSpaces.map(s => s.spaceId);
  const latestSpaceMsgs = await db.collection("groupMessages").aggregate([
    { $match: { groupId: { $in: spaceIds }, deleted: { $ne: true } } },
    { $sort: { sentAt: -1 } },
    { $group: { _id: "$groupId", msg: { $first: "$$ROOT" } } },
  ]).toArray();
  const latestSpaceMap = Object.fromEntries(latestSpaceMsgs.map(r => [r._id, r.msg]));

  // Unread per space (messages not in readBy for this user)
  const spaceUnreads = await db.collection("groupMessages").aggregate([
    { $match: { groupId: { $in: spaceIds }, deleted: { $ne: true }, readBy: { $nin: [myId] } } },
    { $group: { _id: "$groupId", count: { $sum: 1 } } },
  ]).toArray();
  const spaceUnreadMap = Object.fromEntries(spaceUnreads.map(r => [r._id, r.count]));

  const spaceItems = memberSpaces.map(s => {
    const latest = latestSpaceMap[s.spaceId];
    const preview = latest
      ? (latest.type === "text"
          ? (latest.content?.text || "").slice(0, 80) + ((latest.content?.text || "").length > 80 ? "…" : "")
          : `[${latest.type}]`)
      : null;
    return {
      chatType:     "space",
      chatId:       s.spaceId,
      name:         s.name,
      username:     null,
      avatar:       s.profileImage || null,
      premium:      s.premium      || false,
      premiumExpiry: s.premiumExpiry || null,
      verified:     false,
      memberCount:  s.memberCount  || 0,
      xp:           s.xp           || 0,
      joinLink:     s.joinLink     || null,
      lastMessage: {
        preview,
        type:       latest?.type    || null,
        sentAt:     latest?.sentAt  || null,
        senderName: latest?.senderName || null,
        fromMe:     latest?.senderId === myId,
      },
      unreadCount:  spaceUnreadMap[s.spaceId] || 0,
      sortKey:      latest?.sentAt || s.createdAt || "1970-01-01T00:00:00.000Z",
    };
  });

  // ── 3. Feeds ──────────────────────────────────────────────────────────────
  const memberFeeds = await db.collection("feeds")
    .find({ "members.userId": myId, banned: { $ne: true } })
    .toArray();

  const feedIds = memberFeeds.map(f => f.feedId);
  const latestFeedMsgs = await db.collection("groupMessages").aggregate([
    { $match: { groupId: { $in: feedIds }, deleted: { $ne: true } } },
    { $sort: { sentAt: -1 } },
    { $group: { _id: "$groupId", msg: { $first: "$$ROOT" } } },
  ]).toArray();
  const latestFeedMap = Object.fromEntries(latestFeedMsgs.map(r => [r._id, r.msg]));

  const feedUnreads = await db.collection("groupMessages").aggregate([
    { $match: { groupId: { $in: feedIds }, deleted: { $ne: true }, readBy: { $nin: [myId] } } },
    { $group: { _id: "$groupId", count: { $sum: 1 } } },
  ]).toArray();
  const feedUnreadMap = Object.fromEntries(feedUnreads.map(r => [r._id, r.count]));

  const feedItems = memberFeeds.map(f => {
    const latest = latestFeedMap[f.feedId];
    const preview = latest
      ? (latest.type === "text"
          ? (latest.content?.text || "").slice(0, 80) + ((latest.content?.text || "").length > 80 ? "…" : "")
          : `[${latest.type}]`)
      : null;
    return {
      chatType:     "feed",
      chatId:       f.feedId,
      name:         f.name,
      username:     null,
      avatar:       f.profileImage || null,
      premium:      f.premium      || false,
      premiumExpiry: f.premiumExpiry || null,
      verified:     false,
      memberCount:  f.memberCount  || 0,
      xp:           f.xp           || 0,
      joinLink:     f.joinLink     || null,
      lastMessage: {
        preview,
        type:       latest?.type    || null,
        sentAt:     latest?.sentAt  || null,
        senderName: latest?.senderName || null,
        fromMe:     latest?.senderId === myId,
      },
      unreadCount:  feedUnreadMap[f.feedId] || 0,
      sortKey:      latest?.sentAt || f.createdAt || "1970-01-01T00:00:00.000Z",
    };
  });

  // ── 4. Merge, sort, paginate ──────────────────────────────────────────────
  const all = [...dmItems, ...spaceItems, ...feedItems]
    .sort((a, b) => new Date(b.sortKey) - new Date(a.sortKey));

  const page_items = all.slice(skip, skip + PAGE_SIZE);
  // Remove internal sortKey before sending
  const cleaned = page_items.map(({ sortKey, ...rest }) => rest);

  res.json({
    chats:   cleaned,
    total:   all.length,
    page,
    hasMore: all.length > skip + PAGE_SIZE,
  });
});

// Keep old /api/conversations as alias for DM-only list
app.get("/api/conversations", requireAuth, async (req, res) => {
  const myId   = req.user.userId;
  const page   = Math.max(parseInt(req.query.page) || 1, 1);
  const convos = await db.collection("conversations")
    .find({ participants: myId })
    .sort({ lastMessageAt: -1 })
    .skip((page - 1) * 50)
    .limit(50)
    .toArray();

  const partnerIds = convos.map(c => c.participants.find(id => id !== myId));
  const partners   = await db.collection("users").find({ userId: { $in: partnerIds } }).toArray();
  const partnerMap = Object.fromEntries(partners.map(u => [u.userId, u]));

  const unreadAgg = await db.collection("messages").aggregate([
    { $match: { recipientId: myId, readAt: null, deleted: { $ne: true } } },
    { $group: { _id: "$conversationId", count: { $sum: 1 } } },
  ]).toArray();
  const unreadMap = Object.fromEntries(unreadAgg.map(r => [r._id, r.count]));

  const enriched = convos.map(c => {
    const partnerId = c.participants.find(id => id !== myId);
    return {
      conversationId:     c.conversationId,
      partner:            userStub(partnerMap[partnerId]),
      lastMessageAt:      c.lastMessageAt,
      lastMessageType:    c.lastMessageType,
      lastMessagePreview: c.lastMessagePreview,
      fromMe:             c.lastSenderId === myId,
      unreadCount:        unreadMap[c.conversationId] || 0,
      clearedAt:          c.clearedAt?.[myId] || null,
    };
  });

  res.json({ conversations: enriched, page });
});

// ── Unread count ──
app.get("/api/messages/unread", requireAuth, async (req, res) => {
  const myId   = req.user.userId;
  const unread = await db.collection("messages").countDocuments({
    recipientId: myId, readAt: null, deleted: { $ne: true },
  });
  res.json({ unread });
});


// ────────────────────────────────────────────────────────────────────────────
//  CAN MESSAGE CHECK
// ────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/can-message/:userId
 * Returns { canMessage: true/false, reason? }
 * Checks blocks, bans, messaging settings, country restrictions, premium wall.
 */
app.get("/api/can-message/:userId", requireAuth, async (req, res) => {
  const myId       = req.user.userId;
  const targetId   = req.params.userId;
  const users      = db.collection("users");
  const blocks     = db.collection("blocks");

  if (myId === targetId) return res.json({ canMessage: false, reason: "Cannot message yourself." });

  const me     = await users.findOne({ userId: myId });
  const target = await users.findOne({ userId: targetId });

  if (!me)     return res.status(404).json({ error: "Your account not found." });
  if (!target) return res.json({ canMessage: false, reason: "User not found." });
  if (me.banned)     return res.json({ canMessage: false, reason: "Your account is banned." });
  if (target.banned) return res.json({ canMessage: false, reason: "This user is banned." });

  const blocked = await blocks.findOne({
    $or: [
      { blockerId: targetId, blockedId: myId },
      { blockerId: myId, blockedId: targetId },
    ],
  });
  if (blocked) return res.json({ canMessage: false, reason: "Messaging is not available." });

  const settings = target.messagingSettings || {};
  if (settings.acceptMessages === false)
    return res.json({ canMessage: false, reason: "This user does not accept messages." });
  if (settings.blockNonPremium && !me.premium)
    return res.json({ canMessage: false, reason: "This user only accepts messages from premium members." });
  if (settings.blockedCountries?.length && settings.blockedCountries.includes(me.locality))
    return res.json({ canMessage: false, reason: "This user does not accept messages from your region." });

  res.json({ canMessage: true });
});

// ────────────────────────────────────────────────────────────────────────────
//  USER LOOKUP (by username or contact number)
// ────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/lookup?username=<username>
 * GET /api/lookup?contact=<contactNumber>
 * Returns the userId + public stub of the matched user.
 */
app.get("/api/lookup", requireAuth, async (req, res) => {
  const { username, contact } = req.query;
  if (!username && !contact)
    return res.status(400).json({ error: "Provide ?username= or ?contact= query param." });

  const users = db.collection("users");
  let user;

  if (username) {
    user = await users.findOne({ usernameLower: username.trim().toLowerCase() });
  } else {
    // Normalize the contact number for flexible matching (strip spaces)
    const normalized = contact.trim();
    user = await users.findOne({ contactNumber: normalized });
  }

  if (!user) return res.status(404).json({ error: "User not found." });

  res.json({
    userId:         user.userId,
    name:           user.name,
    username:       user.username,
    tag:            user.tag,
    profilePicture: user.profilePicture,
    premium:        user.premium,
    verified:       user.verified,
    contactNumber:  user.hideContact ? null : user.contactNumber,
  });
});

// ────────────────────────────────────────────────────────────────────────────
//  USER SEARCH (by username or name)
// ────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/search/users?q=<query>&limit=20
 * Searches username and name. Returns avatar, name, userId, premium, verified.
 * Excludes banned users and the searcher themselves.
 */
app.get("/api/search/users", requireAuth, async (req, res) => {
  const q     = req.query.q?.trim();
  const limit = Math.min(parseInt(req.query.limit) || 20, 50);

  if (!q || q.length < 1) return res.status(400).json({ error: "Query param ?q= is required." });

  const users = db.collection("users");

  // Use regex for prefix-style match on username and name (fast with indexes)
  const regex = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");

  // Also try exact matches on userId and contactNumber
  const results = await users.find({
    banned: { $ne: true },
    userId: { $ne: req.user.userId },
    $or: [
      { usernameLower: { $regex: q.toLowerCase() } },
      { name: { $regex: regex } },
      { userId: q },
      { contactNumber: q },
    ],
  })
  .limit(limit)
  .project({
    userId: 1, name: 1, username: 1, tag: 1,
    profilePicture: 1, premium: 1, verified: 1,
  })
  .toArray();

  res.json({ users: results.map(u => ({
    userId:         u.userId,
    name:           u.name,
    username:       u.username,
    tag:            u.tag,
    profilePicture: u.profilePicture,
    premium:        u.premium,
    verified:       u.verified,
  })) });
});

// ────────────────────────────────────────────────────────────────────────────
//  MESSAGE SEARCH
// ────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/messages/search?q=<query>&with=<userId>&limit=20
 * Searches within a specific conversation. ?with= is required.
 * Returns truncated message previews, messageId, type, sentAt.
 */
app.get("/api/messages/search", requireAuth, async (req, res) => {
  const myId       = req.user.userId;
  const withUserId = req.query.with;
  const q          = req.query.q?.trim();
  const limit      = Math.min(parseInt(req.query.limit) || 20, 50);

  if (!withUserId) return res.status(400).json({ error: "?with=<userId> is required." });
  if (!q)          return res.status(400).json({ error: "?q= is required." });

  const conversationId = makeConversationId(myId, withUserId);
  const messages       = db.collection("messages");

  const regex = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");

  const results = await messages.find({
    conversationId,
    deleted: { $ne: true },
    type:    "text",
    "content.text": { $regex: regex },
  })
  .sort({ sentAt: -1 })
  .limit(limit)
  .toArray();

  res.json({
    messages: results.map(m => ({
      messageId: m.messageId,
      senderId:  m.senderId,
      type:      m.type,
      sentAt:    m.sentAt,
      preview:   (m.content.text || "").slice(0, 80) + ((m.content.text || "").length > 80 ? "…" : ""),
    })),
  });
});

/**
 * GET /api/messages/inbox/search?q=<query>&limit=20&page=1
 * Searches ALL messages received by the authenticated user across all conversations.
 * Returns truncated message, sender stub (userId, name, premium, verified, avatar), messageId.
 */
app.get("/api/messages/inbox/search", requireAuth, async (req, res) => {
  const myId  = req.user.userId;
  const q     = req.query.q?.trim();
  const limit = Math.min(parseInt(req.query.limit) || 20, 50);
  const page  = Math.max(parseInt(req.query.page) || 1, 1);
  const skip  = (page - 1) * limit;

  if (!q) return res.status(400).json({ error: "?q= is required." });

  const messages = db.collection("messages");
  const users    = db.collection("users");

  const regex = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");

  const results = await messages.find({
    recipientId: myId,
    deleted:     { $ne: true },
    type:        "text",
    "content.text": { $regex: regex },
  })
  .sort({ sentAt: -1 })
  .skip(skip)
  .limit(limit)
  .toArray();

  // Batch-load sender info
  const senderIds  = [...new Set(results.map(m => m.senderId))];
  const senderDocs = await users.find({ userId: { $in: senderIds } }).toArray();
  const senderMap  = Object.fromEntries(senderDocs.map(u => [u.userId, u]));

  const enriched = results.map(m => {
    const s = senderMap[m.senderId];
    return {
      messageId:      m.messageId,
      conversationId: m.conversationId,
      sentAt:         m.sentAt,
      preview:        (m.content.text || "").slice(0, 80) + ((m.content.text || "").length > 80 ? "…" : ""),
      sender: s ? {
        userId:         s.userId,
        name:           s.name,
        username:       s.username,
        profilePicture: s.profilePicture,
        premium:        s.premium,
        verified:       s.verified,
      } : null,
    };
  });

  res.json({ messages: enriched, page, limit });
});


// ════════════════════════════════════════════════════════════════════════════
//  GROUP MESSAGE SEARCH
// ════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/:groupType/:groupId/messages/search?q=&limit=20&page=1
 * Search text messages within a specific space or feed.
 * User must be a non-banned member to search.
 */
app.get("/api/:groupType/:groupId/messages/search", requireAuth, async (req, res) => {
  const { groupType, groupId } = req.params;
  const userId = req.user.userId;
  const q      = req.query.q?.trim();
  const limit  = Math.min(parseInt(req.query.limit) || 20, 50);
  const page   = Math.max(parseInt(req.query.page)  || 1,  1);
  const skip   = (page - 1) * limit;

  if (!q) return res.status(400).json({ error: "?q= is required." });

  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";
  const group   = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });
  if (group.banned) return res.json({ banned: true });

  const member = getMember(group, userId);
  if (!member || member.isBanned) return res.status(403).json({ error: "Access denied." });

  const regex = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");

  const results = await db.collection("groupMessages").find({
    groupId,
    deleted: { $ne: true },
    type:    "text",
    "content.text": { $regex: regex },
  })
  .sort({ sentAt: -1 })
  .skip(skip)
  .limit(limit)
  .toArray();

  res.json({
    messages: results.map(m => ({
      messageId:      m.messageId,
      groupId:        m.groupId,
      chatType:       groupType,
      senderId:       m.senderId,
      senderName:     m.senderName,
      senderUsername: m.senderUsername,
      senderAvatar:   m.senderAvatar,
      senderIsBot:    m.senderIsBot || false,
      sentAt:         m.sentAt,
      preview:        (m.content.text || "").slice(0, 80) + ((m.content.text || "").length > 80 ? "…" : ""),
    })),
    page, limit,
  });
});

/**
 * GET /api/search/messages?q=&limit=20&page=1
 * Global search across ALL messages the user has access to:
 *   - DMs (sent or received)
 *   - Group messages in spaces/feeds the user is a member of
 * Returns results grouped by source (dm | space | feed).
 */
app.get("/api/search/messages", requireAuth, async (req, res) => {
  const userId = req.user.userId;
  const q      = req.query.q?.trim();
  const limit  = Math.min(parseInt(req.query.limit) || 20, 50);
  const page   = Math.max(parseInt(req.query.page)  || 1,  1);
  const skip   = (page - 1) * limit;

  if (!q) return res.status(400).json({ error: "?q= is required." });

  const regex = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");

  // ── 1. DMs: user is sender or recipient ──────────────────────────────────
  const dmResults = await db.collection("messages").find({
    $or: [{ senderId: userId }, { recipientId: userId }],
    deleted: { $ne: true },
    type:    "text",
    "content.text": { $regex: regex },
  })
  .sort({ sentAt: -1 })
  .limit(limit)
  .toArray();

  // Batch-load DM counterpart user info
  const dmCounterpartIds = [...new Set(dmResults.map(m =>
    m.senderId === userId ? m.recipientId : m.senderId
  ))];
  const dmUserDocs = await db.collection("users").find({ userId: { $in: dmCounterpartIds } }).toArray();
  const dmUserMap  = Object.fromEntries(dmUserDocs.map(u => [u.userId, u]));

  const dmHits = dmResults.map(m => {
    const counterpartId = m.senderId === userId ? m.recipientId : m.senderId;
    const counterpart   = dmUserMap[counterpartId];
    return {
      source:         "dm",
      chatId:         m.conversationId,
      chatType:       "dm",
      messageId:      m.messageId,
      senderId:       m.senderId,
      fromMe:         m.senderId === userId,
      sentAt:         m.sentAt,
      preview:        (m.content.text || "").slice(0, 80) + ((m.content.text || "").length > 80 ? "…" : ""),
      counterpart:    counterpart ? userStub(counterpart) : { userId: counterpartId },
    };
  });

  // ── 2. Group messages in groups the user is a member of ──────────────────
  // Gather all groupIds the user belongs to across spaces and feeds
  const memberSpaces = await db.collection("spaces").find({ "members.userId": userId }, { projection: { spaceId: 1 } }).toArray();
  const memberFeeds  = await db.collection("feeds").find({ "members.userId": userId }, { projection: { feedId: 1 } }).toArray();

  const groupIds = [
    ...memberSpaces.map(s => s.spaceId),
    ...memberFeeds.map(f => f.feedId),
  ];

  const groupTypeMap = Object.fromEntries([
    ...memberSpaces.map(s => [s.spaceId, "space"]),
    ...memberFeeds.map(f => [f.feedId, "feed"]),
  ]);

  let groupHits = [];
  if (groupIds.length > 0) {
    const groupResults = await db.collection("groupMessages").find({
      groupId: { $in: groupIds },
      deleted: { $ne: true },
      type:    "text",
      "content.text": { $regex: regex },
    })
    .sort({ sentAt: -1 })
    .limit(limit)
    .toArray();

    groupHits = groupResults.map(m => ({
      source:         groupTypeMap[m.groupId] || "group",
      chatId:         m.groupId,
      chatType:       groupTypeMap[m.groupId] || "group",
      messageId:      m.messageId,
      senderId:       m.senderId,
      senderName:     m.senderName,
      senderUsername: m.senderUsername,
      senderAvatar:   m.senderAvatar,
      senderIsBot:    m.senderIsBot || false,
      fromMe:         m.senderId === userId,
      sentAt:         m.sentAt,
      preview:        (m.content.text || "").slice(0, 80) + ((m.content.text || "").length > 80 ? "…" : ""),
    }));
  }

  // ── 3. Merge, sort by sentAt desc, paginate ───────────────────────────────
  const all = [...dmHits, ...groupHits].sort((a, b) =>
    new Date(b.sentAt) - new Date(a.sentAt)
  );
  const paginated = all.slice(skip, skip + limit);

  res.json({
    results:  paginated,
    total:    all.length,
    page,
    limit,
    hasMore:  all.length > skip + limit,
  });
});

// ════════════════════════════════════════════════════════════════════════════
//  BOT SYSTEM
// ════════════════════════════════════════════════════════════════════════════

const crypto = require("crypto");

/** Generate a 30-character alphanumeric bot token */
function generateBotToken() {
  return crypto.randomBytes(22).toString("base64url").slice(0, 30);
}

async function generateBotId() {
  const bots = db.collection("bots");
  let id;
  do { id = String(Math.floor(10000000 + Math.random() * 90000000)); }
  while (await bots.findOne({ botId: id }));
  return id;
}

/** Validate a bot token from Authorization header or query param */
async function resolveBotFromReq(req) {
  const auth = req.headers["authorization"] || "";
  const token = auth.startsWith("Bot ") ? auth.slice(4).trim()
    : req.query.botToken || null;
  if (!token) return null;
  return db.collection("bots").findOne({ botToken: token });
}

function requireBotAuth(req, res, next) {
  resolveBotFromReq(req).then(bot => {
    if (!bot) return res.status(401).json({ error: "Invalid bot token." });
    if (bot.disabled) return res.status(403).json({ error: "Bot is disabled." });
    req.bot = bot;
    next();
  }).catch(e => res.status(500).json({ error: e.message }));
}

function botStub(bot) {
  if (!bot) return null;
  return {
    botId:          bot.botId,
    name:           bot.name,
    username:       bot.username,
    tag:            bot.tag,
    profilePicture: bot.profilePicture,
    description:    bot.description,
    isBot:          true,
  };
}

// ── Create a bot ──────────────────────────────────────────────────────────────
/**
 * POST /api/bots
 * multipart/form-data
 *   username    (required, must end with "bot", validated)
 *   name        (required)
 *   description (optional)
 *   profilePicture (optional file)
 */
app.post("/api/bots", requireAuth, upload.single("profilePicture"), async (req, res) => {
  const ownerId = req.user.userId;
  const bots    = db.collection("bots");

  // Max 3 bots per user
  const count = await bots.countDocuments({ ownerId });
  if (count >= 3)
    return res.status(403).json({ error: "Maximum of 3 bots per account." });

  const { name, username, description } = req.body;
  if (!name?.trim())
    return res.status(400).json({ error: "name is required." });

  const clean = username?.trim();

  // Inline validation (bot version)
  if (!clean)
    return res.status(400).json({ error: "Username is required." });

  if (clean.length < 3)
    return res.status(400).json({ error: "Username must be at least 3 characters." });

  if (!/^[a-zA-Z0-9_]+$/.test(clean))
    return res.status(400).json({
      error: "Username can only contain letters, numbers, and underscores."
    });

  if (clean.startsWith("_") || clean.endsWith("_"))
    return res.status(400).json({
      error: "Username cannot start or end with an underscore."
    });

  // Bot-specific rule
  if (!clean.toLowerCase().endsWith("bot"))
    return res.status(400).json({
      error: 'Bot username must end with "bot".'
    });

  // Global uniqueness
  const users = db.collection("users");

  if (await bots.findOne({ usernameLower: clean.toLowerCase() }))
    return res.status(409).json({ error: "Bot username already taken." });

  if (await users.findOne({ usernameLower: clean.toLowerCase() }))
    return res.status(409).json({ error: "Username already taken by a user." });

  let profilePictureUrl = null;
  if (req.file) {
    try {
      profilePictureUrl = await uploadMedia(
        req.file.buffer,
        req.file.originalname || "bot.jpg"
      );
    } catch (e) {}
  }

  const botId    = await generateBotId();
  const botToken = generateBotToken();
  const now      = new Date().toISOString();

  const bot = {
    botId,
    ownerId,
    botToken,
    name: name.trim(),
    username: clean,
    usernameLower: clean.toLowerCase(),
    tag: `#${clean}`,
    description: description?.trim() || "",
    profilePicture: profilePictureUrl,
    isBot: true,
    status: "offline",
    disabled: false,
    createdAt: now,
  };

  await bots.insertOne(bot);

  grantXP(ownerId, "createBot", 1).catch(() => {});

  res.status(201).json({
    message: "Bot created.",
    bot: botStub(bot),
    botToken,
  });
});

// ── List my bots ──────────────────────────────────────────────────────────────
app.get("/api/bots", requireAuth, async (req, res) => {
  const bots = await db.collection("bots").find({ ownerId: req.user.userId }).toArray();
  // Include token only for owner
  res.json({ bots: bots.map(b => ({ ...botStub(b), botToken: b.botToken, disabled: b.disabled, status: b.status })) });
});

// ── Get a single bot (public) ─────────────────────────────────────────────────
app.get("/api/bots/:botId", async (req, res) => {
  const bot = await db.collection("bots").findOne({ botId: req.params.botId });
  if (!bot) return res.status(404).json({ error: "Bot not found." });
  res.json({ bot: botStub(bot) });
});

// ── Update bot profile ────────────────────────────────────────────────────────
app.patch("/api/bots/:botId", requireAuth,
  upload.single("profilePicture"),
  async (req, res) => {
    const bots = db.collection("bots");
    const bot  = await bots.findOne({ botId: req.params.botId });
    if (!bot) return res.status(404).json({ error: "Bot not found." });
    if (bot.ownerId !== req.user.userId) return res.status(403).json({ error: "Not your bot." });

    const { name, description } = req.body;
    const updates = {};
    if (name?.trim())        updates.name        = name.trim();
    if (description !== undefined) updates.description = description.trim();

    if (req.file) {
      try { updates.profilePicture = await uploadMedia(req.file.buffer, req.file.originalname || "bot.jpg"); }
      catch (e) { return res.status(500).json({ error: "Upload failed: " + e.message }); }
    }

    await bots.updateOne({ botId: bot.botId }, { $set: updates });
    const updated = await bots.findOne({ botId: bot.botId });
    res.json({ bot: botStub(updated) });
  }
);

// ── Regenerate bot token ──────────────────────────────────────────────────────
app.post("/api/bots/:botId/regenerate-token", requireAuth, async (req, res) => {
  const bots = db.collection("bots");
  const bot  = await bots.findOne({ botId: req.params.botId });
  if (!bot) return res.status(404).json({ error: "Bot not found." });
  if (bot.ownerId !== req.user.userId) return res.status(403).json({ error: "Not your bot." });

  const newToken = generateBotToken();
  await bots.updateOne({ botId: bot.botId }, { $set: { botToken: newToken } });

  // Disconnect existing bot WebSocket if connected
  const existingSocketId = botClients.get(bot.botId);
if (existingSocketId) {
  botNsp.sockets.get(existingSocketId)?.disconnect(true);
  botClients.delete(bot.botId);
}

  res.json({ botToken: newToken });
});

app.get('/blocks/check/:userId', requireAuth, async (req, res) => {
  try {
    const blockerId = req.user.userId;
    const blockedId = req.params.userId;

    const block = await db.collection("blocks").findOne({
      blockerId,
      blockedId
    });

    return res.json({ blocked: !!block });

  } catch (err) {
    console.error('block check error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});
app.post("/api/messages/to-bot",
  requireAuth,
  upload.fields([
    { name: "media",     maxCount: 1 },
    { name: "thumbnail", maxCount: 1 },
  ]),
  async (req, res) => {
    const senderId = req.user.userId;
    const botId    = req.query.to;
    if (!botId) return res.status(400).json({ error: "Query param ?to=<botId> is required." });

    // Resolve bot
    const bot = await db.collection("bots").findOne({ botId });
    if (!bot)         return res.status(404).json({ error: "Bot not found." });
    if (bot.disabled) return res.status(403).json({ error: "This bot is currently disabled." });

    // Check if user blocked the bot (or bot blocked user — bots can't block but be safe)
    const blocked = await db.collection("blocks").findOne({
      $or: [
        { blockerId: senderId, blockedId: botId },
        { blockerId: botId,    blockedId: senderId },
      ],
    });
    if (blocked) return res.status(403).json({ error: "Messaging not available." });

    const sender = await db.collection("users").findOne({ userId: senderId });
    if (!sender)        return res.status(404).json({ error: "Sender not found." });
    if (sender.banned)  return res.status(403).json({ error: "Your account is banned." });

    // Rate-limit (bots should not be spammed)
    const spam = await checkSpam(senderId);
    if (spam.throttled) return res.status(429).json({ error: "Slow down — you are sending messages too fast." });

    const { type, text, caption, replyToMessageId } = req.body;
    const validTypes = ["text", "image", "image_caption", "sticker", "audio", "voice"];
    if (!type || !validTypes.includes(type))
      return res.status(400).json({ error: `type must be one of: ${validTypes.join(", ")}` });

    const content = { replyToMessageId: replyToMessageId || null };

    // Text
    if (type === "text") {
      if (!text?.trim()) return res.status(400).json({ error: "text is required." });
      if (text.trim().length > MSG_TEXT_MAX)
        return res.status(400).json({ error: `Text exceeds ${MSG_TEXT_MAX} characters.` });
      content.text = text.trim();
    }

    // Media
    const needsMedia = ["image", "image_caption", "sticker", "audio", "voice"];
    if (needsMedia.includes(type)) {
      const file = req.files?.media?.[0];
      if (!file) return res.status(400).json({ error: "media file is required." });
      try { content.mediaUrl = await uploadMedia(file.buffer, file.originalname || "media"); }
      catch (e) { return res.status(500).json({ error: "Media upload failed: " + e.message }); }
      content.mediaSize     = file.size;
      content.mediaMimeType = file.mimetype;
    }

    if (type === "image_caption") {
      if (caption && caption.trim().length > MSG_CAPTION_MAX)
        return res.status(400).json({ error: `Caption exceeds ${MSG_CAPTION_MAX} characters.` });
      content.caption = caption?.trim() || "";
    }

    if (type === "sticker") content.isSticker = true;
    if (type === "voice")   content.isVoice   = true;

    // Handle reply
    let replyTo = null;
    if (content.replyToMessageId) {
      const original = await db.collection("messages").findOne({ messageId: content.replyToMessageId });
      if (original && !original.deleted) {
        replyTo = {
          messageId:      original.messageId,
          senderId:       original.senderId,
          senderName:     original.senderName || sender.name,
          senderUsername: original.senderUsername || sender.username,
          type:           original.type,
          content:        original.content,
          preview:        original.type === "text"
            ? (original.content?.text || "").slice(0, 60) + ((original.content?.text || "").length > 60 ? "…" : "")
            : `[${original.type}]`,
          sentAt:         original.sentAt,
        };
      }
    }

    // Build and store message
    const conversationId = makeConversationId(senderId, botId);
    const messageId      = uuidv4();
    const now            = new Date().toISOString();

    const message = {
      messageId,
      conversationId,
      chatId:          conversationId,
      chatType:        "private",
      senderId,
      senderIsBot:     false,
      senderName:      sender.name,
      senderUsername:  sender.username,
      senderAvatar:    sender.profilePicture,
      senderPremium:   sender.premium  || false,
      senderVerified:  sender.verified || false,
      recipientId:     botId,
      type, content, replyTo,
      sentAt:   now,
      editedAt: null,
      readAt:   null,
      deleted:  false,
      reactions: {},
    };

    await db.collection("messages").insertOne(message);

    // Upsert conversation
    await db.collection("conversations").updateOne(
      { conversationId },
      {
        $set: {
          conversationId,
          participants:        [senderId, botId],
          lastMessageId:       messageId,
          lastMessageAt:       now,
          lastSenderId:        senderId,
          lastMessageType:     type,
          lastMessagePreview:  type === "text" ? (content.text || "").slice(0, 60) : `[${type}]`,
        },
        $setOnInsert: { createdAt: now },
      },
      { upsert: true }
    );

    // XP for sending
    const isMedia = ["image","image_caption","sticker","audio","voice"].includes(type);
    grantXP(senderId, isMedia ? "sendMedia" : "sendMessage", 1).catch(() => {});

    // ── Deliver to sender's own socket (fromMe: true / messageSent) ──
    broadcast(senderId, {
      type:    "messageSent",
      fromMe:  true,
      chat:    { chatId: conversationId, chatType: "dm" },
      from:    userStub(sender),
      message: {
        ...message,
        sender: userStub(sender),
      },
    });

    // ── Deliver to bot's WebSocket so bot can respond ──
    const botSocketId = botClients.get(botId);
    if (botSocketId) {
      botNsp.to(botSocketId).emit("message", {
        type:    "newMessage",
        fromMe:  false,
        chat:    { chatId: conversationId, chatType: "private" },
        from: {
          userId:         sender.userId,
          name:           sender.name,
          username:       sender.username,
          tag:            sender.tag,
          profilePicture: sender.profilePicture,
          premium:        sender.premium  || false,
          verified:       sender.verified || false,
          level:          sender.level    || computeLevel(sender.xp || 0),
          isBot:          false,
        },
        message,
      });
    }

    // chatListUpdate so sender's chat list reorders
    broadcast(senderId, {
      type:     "chatListUpdate",
      chatId:   conversationId,
      chatType: "dm",
      fromMe:   true,
      lastMessage: {
        preview:    type === "text"
          ? (content.text || "").slice(0, 80) + ((content.text || "").length > 80 ? "…" : "")
          : `[${type}]`,
        type,
        sentAt:     now,
        senderName: sender.name,
      },
    });

    res.status(201).json({ message });
  }
);

// ── Delete a bot ──────────────────────────────────────────────────────────────
app.delete("/api/bots/:botId", requireAuth, async (req, res) => {
  const bots = db.collection("bots");
  const bot  = await bots.findOne({ botId: req.params.botId });
  if (!bot) return res.status(404).json({ error: "Bot not found." });
  if (bot.ownerId !== req.user.userId) return res.status(403).json({ error: "Not your bot." });

  await bots.deleteOne({ botId: bot.botId });
  const existingSocketId = botClients.get(bot.botId);
if (existingSocketId) {
  botNsp.sockets.get(existingSocketId)?.disconnect(true);
  botClients.delete(bot.botId);
}

  res.json({ message: "Bot deleted." });
});

// ── Bot send message (HTTP fallback) ─────────────────────────────────────────
/**
 * POST /api/bot/send?to=<userId>
 * Authorization: Bot <token>
 * body: { type, text?, caption?, replyToMessageId? }
 * (Media not supported via this HTTP route — use WebSocket sendMessage for full media)
 */
app.post("/api/bot/send", requireBotAuth,
  upload.fields([{ name: "media", maxCount: 1 }, { name: "thumbnail", maxCount: 1 }]),
  async (req, res) => {
    const bot         = req.bot;
    const recipientId = req.query.to;
    if (!recipientId) return res.status(400).json({ error: "?to=<userId> is required." });

    const { type = "text", text, caption, replyToMessageId } = req.body;
    const validTypes = ["text", "image", "image_caption", "video", "video_caption", "sticker", "audio", "voice"];
    if (!validTypes.includes(type)) return res.status(400).json({ error: "Invalid type." });

    const content = { replyToMessageId: replyToMessageId || null };

    if (type === "text") {
      if (!text?.trim()) return res.status(400).json({ error: "text is required." });
      if (text.length > MSG_TEXT_MAX) return res.status(400).json({ error: "Text too long." });
      content.text = text.trim();
    }

    const needsMedia = ["image", "image_caption", "video", "video_caption", "sticker", "audio", "voice"];
    if (needsMedia.includes(type)) {
      const file = req.files?.media?.[0];
      if (!file) return res.status(400).json({ error: "media file is required." });
      try { content.mediaUrl = await uploadMedia(file.buffer, file.originalname || "media"); }
      catch (e) { return res.status(500).json({ error: "Upload failed: " + e.message }); }
      content.mediaSize     = file.size;
      content.mediaMimeType = file.mimetype;
    }

    if (["image_caption", "video_caption"].includes(type)) {
      if (caption && caption.length > MSG_CAPTION_MAX)
        return res.status(400).json({ error: "Caption too long." });
      content.caption = caption?.trim() || "";
    }

    if (["video", "video_caption"].includes(type)) {
      const thumb = req.files?.thumbnail?.[0];
      if (thumb) {
        try { content.thumbnailUrl = await uploadMedia(thumb.buffer, thumb.originalname || "thumb.jpg"); }
        catch (e) { /* non-fatal */ }
      }
    }

    if (type === "sticker") content.isSticker = true;
    if (type === "voice")   content.isVoice   = true;

    // sendMessage treats bot as sender using botId
    const result = await sendBotMessage({ bot, recipientId, type, content });
    if (result.error) return res.status(result.status || 500).json({ error: result.error });

    res.status(201).json({ message: result.message });
  }
);

// ── Bot send message core (mirrors sendMessage but for bots) ──────────────────
async function sendBotMessage({ bot, recipientId, type, content, bypassFirstMessage = false }) {
  const users      = db.collection("users");
  const messages   = db.collection("messages");
  const convos     = db.collection("conversations");
  const blocks     = db.collection("blocks");

  const recipient = await users.findOne({ userId: recipientId });
  if (!recipient) return { error: "Recipient not found.", status: 404 };
  if (recipient.banned) return { error: "Recipient is banned.", status: 403 };

  // Block check (user can block a bot)
  const blocked = await blocks.findOne({ blockerId: recipientId, blockedId: bot.botId });
  if (blocked) return { error: "This user has blocked the bot.", status: 403 };

  // Telegram-style: bot cannot initiate — user must have messaged the bot first
  if (!bypassFirstMessage) {
    const conversationId = makeConversationId(bot.botId, recipientId);
    const userStarted    = await messages.findOne({ conversationId, senderId: recipientId });
    if (!userStarted)
      return { error: "The user must message the bot first before the bot can reply.", status: 403 };
  }

  const conversationId = makeConversationId(bot.botId, recipientId);
  const messageId      = uuidv4();
  const now            = new Date().toISOString();

  // Reply support
  let replyTo = null;
  if (content.replyToMessageId) {
    const orig = await messages.findOne({ messageId: content.replyToMessageId });
    if (orig && !orig.deleted) {
      replyTo = {
        messageId:      orig.messageId,
        senderId:       orig.senderId,
        senderName:     orig.senderName || bot.name,
        senderUsername: orig.senderUsername || bot.username,
        senderPremium:  false,
        senderVerified: false,
        type:           orig.type,
        preview:        orig.type === "text"
          ? (orig.content.text || "").slice(0, 60) + ((orig.content.text || "").length > 60 ? "…" : "")
          : orig.type,
      };
    }
  }

  const message = {
    messageId,
    conversationId,
    chatId:      conversationId,         // alias — useful for bot handlers
    chatType:    "private",
    senderId:    bot.botId,
    senderIsBot: true,
    senderName:  bot.name,
    senderUsername: bot.username,
    senderAvatar:   bot.profilePicture,
    senderPremium:  false,
    senderVerified: false,
    recipientId,
    recipientName: recipient.name,
    recipientUsername: recipient.username,
    type, content, replyTo,
    sentAt:   now, editedAt: null, readAt: null,
    deleted:  false, reactions: {},
  };

  await messages.insertOne(message);

  await convos.updateOne(
    { conversationId },
    {
      $set: {
        conversationId,
        participants:        [bot.botId, recipientId],
        lastMessageId:       messageId,
        lastMessageAt:       now,
        lastSenderId:        bot.botId,
        lastMessageType:     type,
        lastMessagePreview:  type === "text" ? (content.text || "").slice(0, 60) : `[${type}]`,
      },
      $setOnInsert: { createdAt: now },
    },
    { upsert: true }
  );

  const botFrom = {
    userId:         bot.botId,
    name:           bot.name,
    username:       bot.username,
    tag:            bot.tag,
    profilePicture: bot.profilePicture,
    premium:        false,
    verified:       false,
    level:          0,
    isBot:          true,
  };
  // Recipient gets fromMe:false
  broadcast(recipientId, {
    type:    "newMessage",
    fromMe:  false,
    chat:    { chatId: conversationId, chatType: "dm" },
    from:    botFrom,
    message: { ...message, sender: botStub(bot), chatId: conversationId, chatType: "dm" },
  });

  // Echo to bot's own WS with fromMe:true
const botSocketId = botClients.get(bot.botId);
if (botSocketId) {
  botNsp.to(botSocketId).emit("message", {
    type: "messageSent",
    fromMe: true,
    chat: { chatId: conversationId, chatType: "dm" }, // retain chatId here
    message
  });
}

  // chatListUpdate for reordering
  const botDmChatUpdate = {
    type:     "chatListUpdate",
    chatId:   conversationId,
    chatType: "dm",
    lastMessage: {
      preview:    type === "text"
        ? (content.text || "").slice(0, 80) + ((content.text || "").length > 80 ? "…" : "")
        : `[${type}]`,
      type,
      sentAt:     now,
      senderName: bot.name,
    },
  };
  broadcast(recipientId, { ...botDmChatUpdate, fromMe: false });
  broadcast(bot.botId,    { ...botDmChatUpdate, fromMe: true });

  return { message };
}

// ── Bot send message to a Space/Feed group ───────────────────────────────────
/**
 * Bot must be a member of the group with isAdmin: true.
 * Feeds: bot must be admin (same rule as users).
 * Spaces: bot must be admin.
 */
async function sendBotGroupMessage({ bot, groupId, groupType, type, content }) {
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";
  const group   = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return { error: "Group not found.", status: 404 };
  if (group.banned) return { error: "Group is banned.", status: 403 };

  const member = getMember(group, bot.botId);
  if (!member)         return { error: "Bot is not a member of this group.", status: 403 };
  if (member.isBanned) return { error: "Bot is banned from this group.", status: 403 };
  if (!member.isAdmin && !isOwner(group, bot.botId))
    return { error: "Bot must be an admin to send messages.", status: 403 };
  if (groupType === "feed" && !member.isAdmin)
    return { error: "Only admins can post in a feed.", status: 403 };
  if (group.mutedMedia && type !== "text")
    return { error: "Media messages are disabled in this group.", status: 403 };

  // Reply support
  let replyTo = null;
  if (content.replyToMessageId) {
    const orig = await db.collection("groupMessages").findOne({ messageId: content.replyToMessageId });
    if (orig && !orig.deleted) {
      replyTo = {
        messageId:      orig.messageId,
        senderId:       orig.senderId,
        senderName:     orig.senderName,
        senderUsername: orig.senderUsername,
        type:           orig.type,
        preview:        orig.type === "text"
          ? (orig.content?.text || "").slice(0, 60) + ((orig.content?.text || "").length > 60 ? "…" : "")
          : `[${orig.type}]`,
      };
    }
  }

  const messageId = uuidv4();
  const now       = new Date().toISOString();

  const message = {
    messageId, groupId, type, content, replyTo,
    senderId:        bot.botId,
    senderName:      bot.name,
    senderUsername:  bot.username,
    senderAvatar:    bot.profilePicture,
    senderPremium:   false,
    senderVerified:  false,
    senderIsBot:     true,
    mentions:        [],
    sentAt:          now,
    editedAt:        null,
    deleted:         false,
    reactions:       {},
    readBy:          [bot.botId],
    seenMentions:    [],
  };

  await db.collection("groupMessages").insertOne(message);

  // Grant XP to the group for bot activity
  const grpXpAction_ = ["image","image_caption","sticker","audio","voice"].includes(type)
    ? "sendMedia" : "sendMessage";
  grantGroupXP(col, idField, groupId, grpXpAction_).catch(() => {});

  const botGroupUpdate = {
    type: "newGroupMessage",
    chat: {
      chatId:   groupId,
      chatType: groupType,
      chatName: group.name,
      joinLink: group.joinLink,
    },
    from: {
      userId:         bot.botId,
      name:           bot.name,
      username:       bot.username,
      tag:            bot.tag,
      profilePicture: bot.profilePicture,
      premium:        false,
      verified:       false,
      level:          0,
      isAdmin:        true,
      isOwner:        isOwner(group, bot.botId),
      isBot:          true,
    },
    message: { ...message, chatId: groupId, chatType: groupType },
    groupId,
  };
  // Members get fromMe: false; bot's own WS gets fromMe: true
  broadcastToGroup(groupId, { ...botGroupUpdate, fromMe: false }, null);
  // Echo to the bot itself with fromMe: true
  const botSocketId = botClients.get(bot.botId);
if (botSocketId) {
  botNsp.to(botSocketId).emit("message", { ...botGroupUpdate, fromMe: true, type: "groupMessageSent" });
}

  return { message };
}

// ── Block a bot ───────────────────────────────────────────────────────────────
app.post("/api/bots/:botId/block", requireAuth, async (req, res) => {
  const myId  = req.user.userId;
  const botId = req.params.botId;
  const bot   = await db.collection("bots").findOne({ botId });
  if (!bot) return res.status(404).json({ error: "Bot not found." });

  const blocks = db.collection("blocks");
  const exists = await blocks.findOne({ blockerId: myId, blockedId: botId });
  if (exists) return res.status(409).json({ error: "Already blocked." });

  await blocks.insertOne({ blockerId: myId, blockedId: botId, isBot: true, createdAt: new Date().toISOString() });
  res.json({ message: "Bot blocked." });
});

app.delete("/api/bots/:botId/block", requireAuth, async (req, res) => {
  const { deletedCount } = await db.collection("blocks").deleteOne({
    blockerId: req.user.userId, blockedId: req.params.botId,
  });
  if (!deletedCount) return res.status(404).json({ error: "Block not found." });
  res.json({ message: "Bot unblocked." });
});

// ── Bot WebSocket (getUpdates) ────────────────────────────────────────────────
// Connect: ws://host/bot?token=<30-char-bot-token>
//
// ── Server → Bot (incoming updates) ──────────────────────────────────────────
// DM events:
//   { type:"newMessage",         message:{chatId,chatType:"private",senderId,senderName,senderUsername,senderAvatar,senderPremium,senderVerified,recipientId,...} }
//   { type:"messageSent",        message }           — echo of bot's own DM send
//   { type:"messageDeleted",     messageId, conversationId }
//   { type:"messageEdited",      messageId, text, editedAt }
//   { type:"messageReaction",    messageId, reactions, emoji, added, actorId }
//   { type:"messagesRead",       conversationId, readerId, upToMessageId }
//   { type:"typing",             conversationId, userId, isBot, isTyping }
//
// Group events (pushed when bot is a member of the group):
//   { type:"newGroupMessage",    groupId, message:{chatId,chatType:"space"|"feed",chatName,senderId,senderName,...} }
//   { type:"groupMessageSent",   message }           — echo of bot's own group send
//   { type:"groupMessageDeleted",groupId, messageId }
//   { type:"groupMessageEdited", groupId, messageId, text, editedAt }
//   { type:"groupMessageReaction",groupId, messageId, reactions, emoji, added, actorId }
//   { type:"memberJoined",       groupId/spaceId/feedId, member }
//   { type:"memberLeft",         groupId, userId }
//   { type:"memberBanned",       groupId, targetId, bannedBy }
//   { type:"memberPromoted",     groupId, targetId, promotedBy }
//   { type:"memberDemoted",      groupId, targetId }
//   { type:"messagePinned",      groupId, messageId, pinnedBy }
//
//   { type:"connected",          botId, name, username, tag }
//
// ── Bot → Server (outgoing commands) ─────────────────────────────────────────
// DM:
//   { type:"sendMessage",        to, msgType, text?, mediaUrl?, caption?, replyToMessageId? }
//   { type:"typing",             to, isTyping }
//
// Group (bot must be admin):
//   { type:"sendGroupMessage",   groupId, groupType, msgType, text?, mediaUrl?, caption?, replyToMessageId? }
//   { type:"groupTyping",        groupId, isTyping }
//   { type:"promoteMember",      groupId, groupType, targetId }
//   { type:"demoteMember",       groupId, groupType, targetId }
//   { type:"banMember",          groupId, groupType, targetId }
//   { type:"deleteGroupMessage", groupId, groupType, messageId }
//   { type:"pinGroupMessage",    groupId, groupType, messageId, unpin? }
//
//   { type:"ping" }

botNsp.use(async (socket, next) => {
  const token = socket.handshake.auth?.token ||
    socket.handshake.query?.token ||
    (socket.handshake.headers["authorization"] || "").replace(/^Bot /, "").trim();
  if (!token) return next(new Error("Missing bot token"));

  const bot = await db.collection("bots").findOne({ botToken: token });
  if (!bot)         return next(new Error("Invalid bot token"));
  if (bot.disabled) return next(new Error("Bot is disabled"));

  socket.bot = bot;
  next();
});

botNsp.on("connection", async (socket) => {
  const bot = socket.bot;

  // Disconnect previous connection
  const prevSocketId = botClients.get(bot.botId);
  if (prevSocketId) {
    botNsp.sockets.get(prevSocketId)?.disconnect(true);
  }

  botClients.set(bot.botId, socket.id);
  await db.collection("bots").updateOne({ botId: bot.botId }, { $set: { status: "online" } });

  socket.emit("message", {
    type: "connected",
    botId: bot.botId, name: bot.name, username: bot.username, tag: bot.tag,
  });

  const botErr = (error, ref) =>
    socket.emit("message", { type: "error", error, ...(ref && { ref }) });

  socket.on("ping", () => socket.emit("message", { type: "pong" }));

  socket.on("sendMessage", async (msg) => {
    const { to, msgType = "text", text, caption, replyToMessageId,
            mediaUrl, mediaSize, mediaMimeType, thumbnailUrl } = msg;
    if (!to) { botErr("to is required."); return; }

    const content = {
      replyToMessageId: replyToMessageId || null,
      ...(text         && { text }),
      ...(caption      && { caption }),
      ...(mediaUrl     && { mediaUrl, mediaSize, mediaMimeType }),
      ...(thumbnailUrl && { thumbnailUrl }),
      ...(msgType === "sticker" && { isSticker: true }),
      ...(msgType === "voice"   && { isVoice: true }),
    };
    if (msgType === "text" && !text?.trim()) { botErr("text is required."); return; }

    const result = await sendBotMessage({ bot, recipientId: to, type: msgType, content });
    if (result.error) botErr(result.error);
  });

  socket.on("typing", ({ to, isTyping }) => {
    if (!to) return;
    const conversationId = makeConversationId(bot.botId, to);
    broadcast(to, {
      type: "typing", conversationId,
      userId: bot.botId, isBot: true, botName: bot.name, isTyping: !!isTyping,
    });
  });

  socket.on("sendGroupMessage", async (msg) => {
    const { groupId, groupType = "space", msgType = "text", text, caption,
            replyToMessageId, mediaUrl, mediaSize, mediaMimeType } = msg;
    if (!groupId) { botErr("groupId is required."); return; }
    if (msgType === "text" && !text?.trim()) { botErr("text is required."); return; }
    if (!SPACE_ALLOWED_TYPES.includes(msgType)) {
      botErr(`msgType must be one of: ${SPACE_ALLOWED_TYPES.join(", ")}`);
      return;
    }

    const content = {
      replyToMessageId: replyToMessageId || null,
      ...(text     && { text:    text.slice(0, GROUP_TEXT_MAX) }),
      ...(caption  && { caption: caption.slice(0, GROUP_CAPTION_MAX) }),
      ...(mediaUrl && { mediaUrl, mediaSize, mediaMimeType }),
      ...(msgType === "sticker" && { isSticker: true }),
      ...(msgType === "voice"   && { isVoice: true }),
    };

    const result = await sendBotGroupMessage({ bot, groupId, groupType, type: msgType, content });
    if (result.error) { botErr(result.error); return; }
    socket.emit("message", {
      type: "groupMessageSent",
      chat: { chatId: groupId, chatType: groupType },
      message: result.message,
    });
  });

  socket.on("groupTyping", ({ groupId, isTyping }) => {
    if (!groupId) return;
    broadcastToGroup(groupId, {
      type: "typing", groupId,
      userId: bot.botId, isBot: true, botName: bot.name, isTyping: !!isTyping,
    }, bot.botId);
  });

  socket.on("promoteMember", async ({ groupId, groupType = "space", targetId }) => {
    const col = groupType === "space" ? "spaces" : "feeds";
    const idField = groupType === "space" ? "spaceId" : "feedId";
    const group = await db.collection(col).findOne({ [idField]: groupId });
    if (!group) { botErr("Group not found."); return; }
    const botMember = getMember(group, bot.botId);
    if (!botMember?.isAdmin) { botErr("Bot must be admin."); return; }
    const target = getMember(group, targetId);
    if (!target) { botErr("Target not a member."); return; }
    if (target.isAdmin) { botErr("Already admin."); return; }
    await db.collection(col).updateOne(
      { [idField]: groupId, "members.userId": targetId },
      { $set: { "members.$.isAdmin": true, "members.$.promotedBy": bot.botId } }
    );
    broadcastToGroup(groupId, { type: "memberPromoted", groupId, targetId, promotedBy: bot.botId });
    socket.emit("message", { type: "promoteMemberOk", groupId, targetId });
  });

  socket.on("demoteMember", async ({ groupId, groupType = "space", targetId }) => {
    const col = groupType === "space" ? "spaces" : "feeds";
    const idField = groupType === "space" ? "spaceId" : "feedId";
    const group = await db.collection(col).findOne({ [idField]: groupId });
    if (!group) { botErr("Group not found."); return; }
    const target = getMember(group, targetId);
    if (!target) { botErr("Target not found."); return; }
    if (target.isOwner) { botErr("Cannot demote owner."); return; }
    if (target.promotedBy !== bot.botId && !isOwner(group, bot.botId)) {
      botErr("Bot can only demote members it promoted."); return;
    }
    await db.collection(col).updateOne(
      { [idField]: groupId, "members.userId": targetId },
      { $set: { "members.$.isAdmin": false, "members.$.promotedBy": null } }
    );
    broadcastToGroup(groupId, { type: "memberDemoted", groupId, targetId });
    socket.emit("message", { type: "demoteMemberOk", groupId, targetId });
  });

  socket.on("banMember", async ({ groupId, groupType = "space", targetId }) => {
    const col = groupType === "space" ? "spaces" : "feeds";
    const idField = groupType === "space" ? "spaceId" : "feedId";
    const group = await db.collection(col).findOne({ [idField]: groupId });
    if (!group) { botErr("Group not found."); return; }
    const botMember = getMember(group, bot.botId);
    if (!botMember?.isAdmin) { botErr("Bot must be admin."); return; }
    const target = getMember(group, targetId);
    if (!target) { botErr("Target not found."); return; }
    if (target.isAdmin && !isOwner(group, bot.botId)) { botErr("Only owner can ban admins."); return; }
    await db.collection(col).updateOne(
      { [idField]: groupId, "members.userId": targetId },
      { $set: { "members.$.isBanned": true }, $inc: { memberCount: -1 } }
    );
    broadcastToGroup(groupId, { type: "memberBanned", groupId, targetId, bannedBy: bot.botId });
    socket.emit("message", { type: "banMemberOk", groupId, targetId });
  });

  socket.on("deleteGroupMessage", async ({ groupId, groupType = "space", messageId }) => {
    const col = groupType === "space" ? "spaces" : "feeds";
    const idField = groupType === "space" ? "spaceId" : "feedId";
    const group = await db.collection(col).findOne({ [idField]: groupId });
    if (!group) { botErr("Group not found."); return; }
    const botMember = getMember(group, bot.botId);
    if (!botMember?.isAdmin) { botErr("Bot must be admin."); return; }
    const gmsg = await db.collection("groupMessages").findOne({ messageId, groupId });
    if (!gmsg || gmsg.deleted) { botErr("Message not found."); return; }
    await db.collection("groupMessages").updateOne(
      { messageId },
      { $set: { deleted: true, content: {}, deletedAt: new Date().toISOString() } }
    );
    broadcastToGroup(groupId, { type: "groupMessageDeleted", groupId, messageId });
    socket.emit("message", { type: "deleteGroupMessageOk", groupId, messageId });
  });

  socket.on("pinGroupMessage", async ({ groupId, groupType = "space", messageId, unpin = false }) => {
    const col = groupType === "space" ? "spaces" : "feeds";
    const idField = groupType === "space" ? "spaceId" : "feedId";
    const group = await db.collection(col).findOne({ [idField]: groupId });
    if (!group) { botErr("Group not found."); return; }
    const botMember = getMember(group, bot.botId);
    if (!botMember?.isAdmin) { botErr("Bot must be admin."); return; }
    const newPin = unpin ? null : messageId;
    await db.collection(col).updateOne({ [idField]: groupId }, { $set: { pinnedMessageId: newPin } });
    broadcastToGroup(groupId, { type: "messagePinned", groupId, messageId: newPin, pinnedBy: bot.botId });
    socket.emit("message", { type: "pinGroupMessageOk", groupId, messageId: newPin });
  });

  socket.on("unbanMember", async ({ groupId, groupType = "space", targetId }) => {
    const col = groupType === "space" ? "spaces" : "feeds";
    const idField = groupType === "space" ? "spaceId" : "feedId";
    const group = await db.collection(col).findOne({ [idField]: groupId });
    if (!group) { botErr("Group not found."); return; }
    if (!isAdmin(group, bot.botId)) { botErr("Bot must be admin."); return; }
    await db.collection(col).updateOne(
      { [idField]: groupId, "members.userId": targetId },
      { $set: { "members.$.isBanned": false }, $inc: { memberCount: 1 } }
    );
    broadcastToGroup(groupId, { type: "memberUnbanned", groupId, targetId });
    socket.emit("message", { type: "unbanMemberOk", groupId, targetId });
  });

  socket.on("muteMember", async ({ groupId, groupType = "space", targetId, muted = true }) => {
    const col = groupType === "space" ? "spaces" : "feeds";
    const idField = groupType === "space" ? "spaceId" : "feedId";
    const group = await db.collection(col).findOne({ [idField]: groupId });
    if (!group) { botErr("Group not found."); return; }
    if (!isAdmin(group, bot.botId)) { botErr("Bot must be admin."); return; }
    await db.collection(col).updateOne(
      { [idField]: groupId, "members.userId": targetId },
      { $set: { "members.$.isMuted": !!muted } }
    );
    broadcastToGroup(groupId, { type: "memberMuted", groupId, targetId, muted: !!muted });
    socket.emit("message", { type: "muteMemberOk", groupId, targetId, muted });
  });

  socket.on("sendExiles", async ({ to, amount, note, ref }) => {
    if (!to)     { botErr("to (userId) is required.", ref); return; }
    if (!amount) { botErr("amount is required.", ref);      return; }
    const owner = await db.collection("users").findOne({ userId: bot.ownerId });
    if (!owner)  { botErr("Bot owner not found.", ref); return; }
    const result = await transferExiles({
      fromUserId: bot.ownerId, toUserId: to, amount: parseInt(amount),
      note, senderHint: owner, bypassBlock: true, senderIsBot: true, botName: bot.name,
    });
    if (!result.ok) { botErr(result.error, ref); return; }
    socket.emit("message", {
      type: "sendExilesOk", ref, to, amount: parseInt(amount),
      ownerNewBalance: result.newBalance, sentAt: result.sentAt, messageId: result.messageId,
    });
  });

  socket.on("disconnect", async () => {
    if (botClients.get(bot.botId) === socket.id) {
      botClients.delete(bot.botId);
      await db.collection("bots").updateOne({ botId: bot.botId }, { $set: { status: "offline" } });
    }
  });
});
// ════════════════════════════════════════════════════════════════════════════
//  SPACE XP ENGINE
// ════════════════════════════════════════════════════════════════════════════

// XP granted per activity type inside a space
const SPACE_XP_ACTIONS = {
  sendMessage:  2,
  sendMedia:    3,
  addMember:    5,   // when someone joins via invite link
  getReaction:  1,
};

/**
 * Grant XP to a space (and feed, same function).
 * groupType: "spaces" | "feeds"
 * idField:   "spaceId" | "feedId"
 * groupId:   the id value
 * action:    key from SPACE_XP_ACTIONS
 */
async function grantGroupXP(groupType, idField, groupId, action) {
  const xpGain = SPACE_XP_ACTIONS[action];
  if (!xpGain) return;
  await db.collection(groupType).updateOne(
    { [idField]: groupId },
    { $inc: { xp: xpGain } }
  );
}

// ── Forward message events to listening bot ───────────────────────────────────
// Called from delete/edit/react routes when the other party is a bot
function notifyBotOfEvent(botId, payload) {
  const socketId = botClients.get(botId);
  if (socketId) {
    botNsp.to(socketId).emit("message", payload);
  }
}

// ════════════════════════════════════════════════════════════════════════════
//  REPORTS
// ════════════════════════════════════════════════════════════════════════════

/**
 * POST /api/reports
 * body: { reportedId, reason, detail? }
 * reason: "spam" | "harassment" | "inappropriate" | "scam" | "other"
 */
app.post("/api/reports", requireAuth, async (req, res) => {
  const reporterId = req.user.userId;
  const { reportedId, reason, detail } = req.body;

  const validReasons = ["spam", "harassment", "inappropriate", "scam", "other"];
  if (!reportedId) return res.status(400).json({ error: "reportedId is required." });
  if (!validReasons.includes(reason))
    return res.status(400).json({ error: `reason must be one of: ${validReasons.join(", ")}` });
  if (reporterId === reportedId) return res.status(400).json({ error: "Cannot report yourself." });

  // Check reportedId exists in users or bots
  const users = db.collection("users");
  const bots  = db.collection("bots");
  const target = await users.findOne({ userId: reportedId }) || await bots.findOne({ botId: reportedId });
  if (!target) return res.status(404).json({ error: "Reported user not found." });

  const reports = db.collection("reports");

  // Prevent duplicate report from same reporter within 24h
  const recent = await reports.findOne({
    reporterId,
    reportedId,
    createdAt: { $gte: new Date(Date.now() - 86400000).toISOString() },
  });
  if (recent) return res.status(429).json({ error: "You already reported this user recently." });

  const reportId = uuidv4();
  await reports.insertOne({
    reportId, reporterId, reportedId,
    reason, detail: detail?.trim() || "",
    status:    "open",   // open | reviewed | dismissed | actioned
    createdAt: new Date().toISOString(),
    reviewedAt: null,
    reviewedBy: null,
    action:    null,
  });

  // Increment warningCount on the reported entity (non-binding — admin reviews)
  await users.updateOne({ userId: reportedId }, { $inc: { warningCount: 1 } });

  grantXP(reporterId, "reportSubmit", 1).catch(()=>{});
  res.status(201).json({ message: "Report submitted.", reportId });
});

// ════════════════════════════════════════════════════════════════════════════
//  ADMIN ROUTES (position: admin required)
// ════════════════════════════════════════════════════════════════════════════

function requireAdmin(req, res, next) {
  // Must be authenticated first
  const token = req.cookies?.token;
  if (!token) return res.status(401).json({ error: "Not authenticated." });
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded;
  } catch {
    return res.status(401).json({ error: "Invalid token." });
  }

  db.collection("users").findOne({ userId: req.user.userId }).then(user => {
    if (!user || user.position !== "admin")
      return res.status(403).json({ error: "Admin access required." });
    req.adminUser = user;
    next();
  }).catch(e => res.status(500).json({ error: e.message }));
}

// ── Get all reports (paginated) ───────────────────────────────────────────────
app.get("/api/admin/reports", requireAdmin, async (req, res) => {
  const status = req.query.status || "open";  // open | reviewed | dismissed | actioned | all
  const limit  = Math.min(parseInt(req.query.limit) || 30, 100);
  const page   = Math.max(parseInt(req.query.page)  || 1, 1);
  const skip   = (page - 1) * limit;

  const query = status === "all" ? {} : { status };
  const reports = await db.collection("reports")
    .find(query)
    .sort({ createdAt: -1 })
    .skip(skip)
    .limit(limit)
    .toArray();

  // Attach reporter and reported stubs
  const allIds    = [...new Set([...reports.map(r => r.reporterId), ...reports.map(r => r.reportedId)])];
  const userDocs  = await db.collection("users").find({ userId: { $in: allIds } }).toArray();
  const botDocs   = await db.collection("bots").find({ botId: { $in: allIds } }).toArray();
  const entityMap = Object.fromEntries([
    ...userDocs.map(u => [u.userId, { ...userStub(u), type: "user" }]),
    ...botDocs.map(b => [b.botId, { ...botStub(b), type: "bot" }]),
  ]);

  const enriched = reports.map(r => ({
    ...r,
    reporter: entityMap[r.reporterId] || null,
    reported: entityMap[r.reportedId] || null,
  }));

  const total = await db.collection("reports").countDocuments(query);
  res.json({ reports: enriched, total, page, limit });
});

// ── Review / action a report ──────────────────────────────────────────────────
/**
 * PATCH /api/admin/reports/:reportId
 * body: { status, action? }
 * status: reviewed | dismissed | actioned
 * action: warn | ban | none
 */
app.patch("/api/admin/reports/:reportId", requireAdmin, async (req, res) => {
  const { reportId }       = req.params;
  const { status, action } = req.body;

  const validStatuses = ["reviewed", "dismissed", "actioned"];
  if (!validStatuses.includes(status))
    return res.status(400).json({ error: `status must be one of: ${validStatuses.join(", ")}` });

  const reports = db.collection("reports");
  const report  = await reports.findOne({ reportId });
  if (!report) return res.status(404).json({ error: "Report not found." });

  const now = new Date().toISOString();
  await reports.updateOne({ reportId }, {
    $set: { status, action: action || null, reviewedAt: now, reviewedBy: req.user.userId },
  });

  // Apply action to reported user
  if (action === "ban") {
    await db.collection("users").updateOne({ userId: report.reportedId }, { $set: { banned: true } });
  } else if (action === "warn") {
    await db.collection("users").updateOne({ userId: report.reportedId }, { $inc: { warningCount: 1 } });
  }

  res.json({ message: "Report updated." });
});

// ── Get reported users summary ────────────────────────────────────────────────
app.get("/api/admin/reported-users", requireAdmin, async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 30, 100);
  const page  = Math.max(parseInt(req.query.page)  || 1, 1);
  const skip  = (page - 1) * limit;

  // Aggregate by reportedId, count open reports
  const results = await db.collection("reports").aggregate([
    { $match: { status: "open" } },
    { $group: { _id: "$reportedId", reportCount: { $sum: 1 }, reasons: { $addToSet: "$reason" } } },
    { $sort:  { reportCount: -1 } },
    { $skip:  skip },
    { $limit: limit },
  ]).toArray();

  const ids      = results.map(r => r._id);
  const userDocs = await db.collection("users").find({ userId: { $in: ids } }).toArray();
  const botDocs  = await db.collection("bots").find({ botId:  { $in: ids } }).toArray();
  const map      = Object.fromEntries([
    ...userDocs.map(u => [u.userId, userStub(u)]),
    ...botDocs.map(b => [b.botId, botStub(b)]),
  ]);

  res.json({
    users: results.map(r => ({
      ...map[r._id],
      reportCount: r.reportCount,
      reasons:     r.reasons,
    })),
    page, limit,
  });
});

// Add this BEFORE your requireAuth routes, near the other /auth/ routes
app.get("/auth/check-username", async (req, res) => {
  const { username } = req.query;
  if (!username?.trim()) return res.status(400).json({ error: "username is required." });

  const err = validateUsername(username.trim());
  if (err) return res.json({ available: false, reason: err });

  const taken = await db.collection("users").findOne({ 
    usernameLower: username.trim().toLowerCase() 
  });
  
  res.json({ available: !taken });
});

// ── Ban / unban / warn a user (direct admin action) ───────────────────────────
app.post("/api/admin/users/:userId/ban", requireAdmin, async (req, res) => {
  const { userId } = req.params;
  await db.collection("users").updateOne({ userId }, { $set: { banned: true } });
  res.json({ message: "User banned." });
});

app.post("/api/admin/users/:userId/unban", requireAdmin, async (req, res) => {
  const { userId } = req.params;
  await db.collection("users").updateOne({ userId }, { $set: { banned: false } });
  res.json({ message: "User unbanned." });
});

app.post("/api/admin/users/:userId/warn", requireAdmin, async (req, res) => {
  const { userId } = req.params;
  const { reason } = req.body;
  await db.collection("users").updateOne({ userId }, { $inc: { warningCount: 1 } });
  // Could also push to a warnings array for audit trail if needed
  res.json({ message: "Warning issued." });
});

// ── Set any user's position ───────────────────────────────────────────────────
/**
 * PATCH /api/admin/users/:userId/position
 * body: { position }   e.g. "admin" | "moderator" | "member"
 */
app.patch("/api/admin/users/:userId/position", requireAdmin, async (req, res) => {
  const { userId }   = req.params;
  const { position } = req.body;
  const valid        = ["admin", "moderator", "member", "vip"];
  if (!valid.includes(position))
    return res.status(400).json({ error: `position must be one of: ${valid.join(", ")}` });

  await db.collection("users").updateOne({ userId }, { $set: { position } });
  res.json({ message: `User position set to "${position}".` });
});

// ── Set premium (admin) ───────────────────────────────────────────────────────
app.patch("/api/admin/users/:userId/premium", requireAdmin, async (req, res) => {
  const { userId }      = req.params;
  const { premium, expiresAt } = req.body; // expiresAt: ISO date string or null
  await db.collection("users").updateOne({ userId }, {
    $set: { premium: !!premium, premiumExpiry: expiresAt || null },
  });
  res.json({ message: `Premium ${premium ? "granted" : "revoked"}.` });
});

// ── Disable / enable a bot (admin) ────────────────────────────────────────────
app.patch("/api/admin/bots/:botId/disable", requireAdmin, async (req, res) => {
  const { disabled } = req.body;
  await db.collection("bots").updateOne({ botId: req.params.botId }, { $set: { disabled: !!disabled } });
  if (disabled) {
    const existingSocketId = botClients.get(req.params.botId);
if (existingSocketId) {
  botNsp.sockets.get(existingSocketId)?.disconnect(true);
  botClients.delete(req.params.botId);
}
  }
  res.json({ message: `Bot ${disabled ? "disabled" : "enabled"}.` });
});

// ── List all users (admin) ────────────────────────────────────────────────────
app.get("/api/admin/users", requireAdmin, async (req, res) => {
  const limit  = Math.min(parseInt(req.query.limit) || 50, 200);
  const page   = Math.max(parseInt(req.query.page)  || 1, 1);
  const skip   = (page - 1) * limit;
  const search = req.query.q;

  const query = {};
  if (search) {
    const rx = new RegExp(search.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
    query.$or = [{ usernameLower: { $regex: search.toLowerCase() } }, { name: { $regex: rx } }];
  }

  const users = await db.collection("users").find(query).sort({ dateJoined: -1 }).skip(skip).limit(limit).toArray();
  const total = await db.collection("users").countDocuments(query);

  res.json({ users: users.map(u => sanitizeUser(u)), total, page, limit });
});


// ════════════════════════════════════════════════════════════════════════════
//  CONTROL PANEL
// ════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/controlpanel
 * Returns a snapshot of the authenticated user's dashboard state:
 *   - Identity (userId, name, username, position, status, premium, verified)
 *   - XP + streak + daysOnline
 *   - Unread message count
 *   - Total contacts count
 *   - Recent unread messages (truncated previews for notification centre)
 *   - Active bots (if any)
 */
app.get("/api/controlpanel", requireAuth, async (req, res) => {
  const userId  = req.user.userId;
  const users   = db.collection("users");
  const messages = db.collection("messages");

  const user = await users.findOne({ userId });
  if (!user) return res.status(404).json({ error: "User not found." });
  if (user.banned) return res.status(403).json({ error: "Account banned." });

  // Unread messages (up to 20 latest, for notification preview)
  const unreadMsgs = await messages.find({
    recipientId: userId,
    readAt:      null,
    deleted:     { $ne: true },
  })
  .sort({ sentAt: -1 })
  .limit(20)
  .toArray();

  const totalUnread = await messages.countDocuments({ recipientId: userId, readAt: null, deleted: { $ne: true } });

  // Batch-load senders for unread
  const senderIds  = [...new Set(unreadMsgs.map(m => m.senderId))];
  const senderDocs = await users.find({ userId: { $in: senderIds } }).toArray();
  const senderMap  = Object.fromEntries(senderDocs.map(u => [u.userId, u]));

  const unreadPreviews = unreadMsgs.map(m => ({
    messageId:      m.messageId,
    conversationId: m.conversationId,
    sentAt:         m.sentAt,
    preview:        m.type === "text"
      ? (m.content?.text || "").slice(0, 80) + ((m.content?.text || "").length > 80 ? "…" : "")
      : `[${m.type}]`,
    sender: senderMap[m.senderId] ? userStub(senderMap[m.senderId]) : { userId: m.senderId },
  }));

  // User's bots
  const bots = await db.collection("bots").find({ ownerId: userId }).project({
    botId: 1, name: 1, username: 1, status: 1, profilePicture: 1,
  }).toArray();

  res.json({
    // ── Identity ──
    userId:       user.userId,
    name:         user.name,
    username:     user.username,
    tag:          user.tag,
    position:     user.position,
    premium:      user.premium,
    premiumExpiry: user.premiumExpiry,
    verified:     user.verified,
    profilePicture: user.profilePicture,
    spiritEmoji:  user.spiritEmoji,

    // ── Status ──
    status:       user.status || "offline",
    lastSeen:     user.lastSeen || null,
    banned:       user.banned,

    // ── Progression ──
    xp:           user.xp,
    streaks:      user.streaks,
    daysOnline:   user.daysOnline,
    reputation:   user.reputation,
    exiles:       user.exiles,
    warningCount: user.warningCount,
    lastActiveDate: user.lastActiveDate || null,

    // ── Social ──
    contacts:     user.contacts,

    // ── Messages ──
    totalUnread,
    unreadPreviews,

    // ── Bots ──
    bots,
  });
});

// ── Control Panel WebSocket ────────────────────────────────────────────────────
// Connect: ws://host/controlpanel  (with cookie or ?token=<jwt>)
//
// ── Server → client events ──
//   { type: "init",             ...full snapshot (see below) }
//   { type: "notification",     notifType, conversationId?, messageId?, preview, sender, sentAt }
//       notifType values: "message" | "mention" | "gift" | "exiles" | "reply"
//   { type: "unreadCount",      unread }              — DM unread count changed
//   { type: "groupUnreadCount", chatId, chatType, unread }  — group unread changed
//   { type: "xpUpdate",         xp, level, gained, action, levelUp?, oldLevel?, newLevel?,
//                               reputationGained?, rank?, badges? }
//   { type: "badgeUnlocked",    badge }               — new badge earned
//   { type: "premiumUpdate",    premium, premiumExpiry, daysLeft }
//   { type: "exilesUpdate",     exiles, delta, reason } — balance changed
//   { type: "statusUpdate",     status, lastSeen }
//   { type: "ping" }
//
// ── Client → server ──
//   { type: "ping" | "pong" }

cpNsp.use(async (socket, next) => {
  const rawCookie = socket.handshake.headers.cookie || "";
  const cookies   = Object.fromEntries(
    rawCookie.split(";").filter(Boolean).map(c => {
      const [k, ...v] = c.trim().split("=");
      return [decodeURIComponent(k), decodeURIComponent(v.join("="))];
    })
  );
  const token = cookies["token"] || socket.handshake.auth?.token || socket.handshake.query?.token;
  try {
    socket.userId = jwt.verify(token, JWT_SECRET).userId;
    next();
  } catch {
    next(new Error("Unauthorized"));
  }
});

cpNsp.on("connection", async (socket) => {
  const userId = socket.userId;

  const user = await db.collection("users").findOne({ userId });
  if (!user || user.banned) { socket.disconnect(true); return; }

  if (!cpClients.has(userId)) cpClients.set(userId, new Set());
  cpClients.get(userId).add(socket.id);

  // ── Build and send init snapshot (copy the full init block from old cpWss) ──
  const messages_   = db.collection("messages");
  const users_      = db.collection("users");

  const unreadMsgs  = await messages_.find({ recipientId: userId, readAt: null, deleted: { $ne: true } })
    .sort({ sentAt: -1 }).limit(20).toArray();
  const totalUnread = await messages_.countDocuments({ recipientId: userId, readAt: null, deleted: { $ne: true } });

  const senderIds_  = [...new Set(unreadMsgs.map(m => m.senderId))];
  const senderDocs_ = await users_.find({ userId: { $in: senderIds_ } }).toArray();
  const senderMap_  = Object.fromEntries(senderDocs_.map(u => [u.userId, u]));

  const bots_ = await db.collection("bots").find({ ownerId: userId })
    .project({ botId: 1, name: 1, username: 1, status: 1, profilePicture: 1 }).toArray();

  const mySpaces_ = await db.collection("spaces").find({ "members.userId": userId }, { projection: { spaceId: 1, name: 1, profileImage: 1 } }).toArray();
  const myFeeds_  = await db.collection("feeds").find({ "members.userId": userId }, { projection: { feedId: 1, name: 1, profileImage: 1 } }).toArray();
  const groupIds_ = [...mySpaces_.map(s => s.spaceId), ...myFeeds_.map(f => f.feedId)];

  const grpUnreadAgg_ = groupIds_.length > 0
    ? await db.collection("groupMessages").aggregate([
        { $match: { groupId: { $in: groupIds_ }, deleted: { $ne: true }, readBy: { $nin: [userId] } } },
        { $group: { _id: "$groupId", count: { $sum: 1 } } },
      ]).toArray()
    : [];
  const grpUnreadMap_ = Object.fromEntries(grpUnreadAgg_.map(r => [r._id, r.count]));
  const totalGroupUnread_ = grpUnreadAgg_.reduce((acc, r) => acc + r.count, 0);

  const levelInfo_ = buildLevelInfo(user.xp || 0);
  const badges_    = computeBadges({ ...user, level: levelInfo_.level });

  socket.emit("message", {
    type: "init",
    userId: user.userId, name: user.name, username: user.username, tag: user.tag,
    position: user.position, premium: user.premium, premiumExpiry: user.premiumExpiry || null,
    verified: user.verified, profilePicture: user.profilePicture,
    status: user.status || "offline", exiles: user.exiles || 0,
    xp: user.xp || 0, streaks: user.streaks || 0, daysOnline: user.daysOnline || 0,
    reputation: user.reputation || 0, warningCount: user.warningCount || 0,
    contacts: user.contacts || [],
    levelInfo: levelInfo_, rank: getRank(levelInfo_.level),
    badges: badges_, topBadge: getHighlightBadge(badges_),
    totalUnread,
    unreadPreviews: unreadMsgs.map(m => ({
      messageId: m.messageId, conversationId: m.conversationId, sentAt: m.sentAt,
      preview: m.type === "text"
        ? (m.content?.text || "").slice(0, 80) + ((m.content?.text || "").length > 80 ? "…" : "")
        : `[${m.type}]`,
      sender: senderMap_[m.senderId] ? userStub(senderMap_[m.senderId]) : { userId: m.senderId },
    })),
    totalGroupUnread: totalGroupUnread_,
    groupUnreads: [
      ...mySpaces_.map(s => ({ chatType: "space", chatId: s.spaceId, name: s.name, avatar: s.profileImage, unread: grpUnreadMap_[s.spaceId] || 0 })),
      ...myFeeds_.map(f =>  ({ chatType: "feed",  chatId: f.feedId,  name: f.name, avatar: f.profileImage, unread: grpUnreadMap_[f.feedId]  || 0 })),
    ].filter(g => g.unread > 0),
    bots: bots_,
  });

  // Heartbeat
  const pingInterval = setInterval(() => {
    if (socket.connected) socket.emit("message", { type: "ping" });
  }, 30_000);

  socket.on("ping", () => socket.emit("message", { type: "pong" }));
  socket.on("pong", () => {}); // no-op acknowledgement

  socket.on("disconnect", () => {
    clearInterval(pingInterval);
    const socks = cpClients.get(userId);
    if (socks) {
      socks.delete(socket.id);
      if (socks.size === 0) cpClients.delete(userId);
    }
  });
});
// ── XP leaderboard (bonus feature) ───────────────────────────────────────────
/**
 * GET /api/xp/leaderboard?limit=20
 * Returns top users sorted by XP. Public — no auth required.
 */
app.get("/api/xp/leaderboard", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 20, 100);
  const users = await db.collection("users").find({ banned: { $ne: true } })
    .sort({ xp: -1 })
    .limit(limit)
    .project({ userId: 1, name: 1, username: 1, tag: 1, profilePicture: 1, premium: 1, verified: 1, xp: 1, level: 1, streaks: 1, position: 1, reputation: 1 })
    .toArray();

  res.json({ leaderboard: users.map((u, i) => ({
    rank:       i + 1,
    userId:     u.userId,
    name:       u.name,
    username:   u.username,
    tag:        u.tag,
    profilePicture: u.profilePicture,
    premium:    u.premium,
    verified:   u.verified,
    position:   u.position,
    reputation: u.reputation || 0,
    streaks:    u.streaks    || 0,
    ...buildLevelInfo(u.xp || 0),
  })) });
});

/**
 * GET /api/xp/me
 * Returns authenticated user's XP + level info using the tiered level formula.
 */
app.get("/api/xp/me", requireAuth, async (req, res) => {
  const user = await db.collection("users").findOne({ userId: req.user.userId },
    { projection: { xp: 1, level: 1, reputation: 1, streaks: 1, daysOnline: 1, lastActiveDate: 1, warningCount: 1 } }
  );
  if (!user) return res.status(404).json({ error: "User not found." });

  res.json({
    ...buildLevelInfo(user.xp || 0),
    reputation:     user.reputation || 0,
    streaks:        user.streaks    || 0,
    daysOnline:     user.daysOnline || 0,
    lastActiveDate: user.lastActiveDate || null,
    warningCount:   user.warningCount  || 0,
  });
});

// ── (routes continue below) ──

// ════════════════════════════════════════════════════════════════════════════
//  SPACES (Groups)
// ════════════════════════════════════════════════════════════════════════════

const HOST = process.env.HOST || "https://exile.nett.to";

// ── Middleware helpers for space/feed ─────────────────────────────────────────

async function resolveSpace(spaceId) {
  return db.collection("spaces").findOne({ spaceId });
}
async function resolveFeed(feedId) {
  return db.collection("feeds").findOne({ feedId });
}
function getMember(group, userId) {
  return (group.members || []).find(m => m.userId === userId);
}
function isOwner(group, userId) { return group.ownerId === userId; }
function isAdmin(group, userId) {
  const m = getMember(group, userId);
  return m && (m.isAdmin === true || isOwner(group, userId));
}

// ── Create Space ──────────────────────────────────────────────────────────────
app.post("/api/spaces", requireAuth, upload.single("profileImage"), async (req, res) => {
  const ownerId = req.user.userId;
  const spaces  = db.collection("spaces");

  const owned = await spaces.countDocuments({ ownerId });
  if (owned >= 3) return res.status(403).json({ error: "Maximum of 3 spaces per account." });

  const { name, bio } = req.body;
  if (!name?.trim()) return res.status(400).json({ error: "name is required." });

  const owner = await db.collection("users").findOne({ userId: ownerId });

  let imageUrl = null;
  if (req.file) {
    try { imageUrl = await uploadMedia(req.file.buffer, req.file.originalname || "space.jpg"); }
    catch (e) { /* non-fatal */ }
  }

  const spaceId = await generateSpaceId();
  let joinId;
  do { joinId = generateJoinId(); }
  while (await spaces.findOne({ joinId }));

  const now = new Date().toISOString();
  const space = {
    spaceId,
    joinId,
    joinLink:    `${HOST}/join-space/${joinId}`,
    ownerId,
    name:        name.trim(),
    bio:         bio?.trim() || "Hey this is my space",
    profileImage: imageUrl,
    isPrivate:   req.body.isPrivate === "true" || req.body.isPrivate === true,
    memberCount: 1,
    premium:     false,
    premiumExpiry: null,
    banned:      false,
    banReason:   null,
    mutedMedia:  false,
    pinnedMessageId: null,
    xp:          0,
    createdAt:   now,
    members: [{
      userId:         ownerId,
      name:           owner.name,
      username:       owner.username,
      profilePicture: owner.profilePicture,
      premium:        owner.premium,
      verified:       owner.verified,
      isAdmin:        true,
      isOwner:        true,
      isMuted:        false,
      isBanned:       false,
      joinedAt:       now,
      promotedBy:     null,
    }],
  };

  await spaces.insertOne(space);
  res.status(201).json({ message: "Space created.", space: sanitizeGroup(space) });
});

// ── Create Feed ───────────────────────────────────────────────────────────────
app.post("/api/feeds", requireAuth, upload.single("profileImage"), async (req, res) => {
  const ownerId = req.user.userId;
  const feeds   = db.collection("feeds");

  const owned = await feeds.countDocuments({ ownerId });
  if (owned >= 3) return res.status(403).json({ error: "Maximum of 3 feeds per account." });

  const { name, bio } = req.body;
  if (!name?.trim()) return res.status(400).json({ error: "name is required." });

  const owner = await db.collection("users").findOne({ userId: ownerId });

  let imageUrl = null;
  if (req.file) {
    try { imageUrl = await uploadMedia(req.file.buffer, req.file.originalname || "feed.jpg"); }
    catch (e) { /* non-fatal */ }
  }

  const feedId = await generateSpaceId();
  let joinId;
  do { joinId = generateJoinId(); }
  while (await feeds.findOne({ joinId }) || await db.collection("spaces").findOne({ joinId }));

  const now  = new Date().toISOString();
  const feed = {
    feedId,
    joinId,
    joinLink:    `${HOST}/join-feed/${joinId}`,
    ownerId,
    name:        name.trim(),
    bio:         bio?.trim() || "Hey this is my feed",
    profileImage: imageUrl,
    isPrivate:   req.body.isPrivate === "true" || req.body.isPrivate === true,
    memberCount: 1,
    premium:     false,
    premiumExpiry: null,
    banned:      false,
    banReason:   null,
    mutedMedia:  false,
    pinnedMessageId: null,
    xp:          0,
    createdAt:   now,
    members: [{
      userId:         ownerId,
      name:           owner.name,
      username:       owner.username,
      profilePicture: owner.profilePicture,
      premium:        owner.premium,
      verified:       owner.verified,
      isAdmin:        true,
      isOwner:        true,
      isMuted:        false,
      isBanned:       false,
      joinedAt:       now,
      promotedBy:     null,
    }],
  };

  await feeds.insertOne(feed);
  res.status(201).json({ message: "Feed created.", feed: sanitizeGroup(feed) });
});

function sanitizeGroup(g) {
  if (!g) return null;
  const { _id, ...safe } = g;
  return safe;
}

// ── Get Space Info ────────────────────────────────────────────────────────────
app.get("/api/spaces/:spaceId", async (req, res) => {
  const space = await resolveSpace(req.params.spaceId);
  if (!space) return res.status(404).json({ error: "Space not found." });
  if (space.banned) return res.json({ banned: true, banReason: space.banReason });
  await checkGroupPremiumExpiry("spaces", "spaceId", space.spaceId, space);
  res.json({ space: sanitizeGroup(space) });
});

// ── Get Feed Info ─────────────────────────────────────────────────────────────
app.get("/api/feeds/:feedId", async (req, res) => {
  const feed = await resolveFeed(req.params.feedId);
  if (!feed) return res.status(404).json({ error: "Feed not found." });
  if (feed.banned) return res.json({ banned: true, banReason: feed.banReason });
  await checkGroupPremiumExpiry("feeds", "feedId", feed.feedId, feed);
  res.json({ feed: sanitizeGroup(feed) });
});

// ── Get Spaces/Feeds owned by a user ─────────────────────────────────────────
app.get("/api/users/:userId/groups", async (req, res) => {
  const { userId } = req.params;
  const spaces = await db.collection("spaces").find({ ownerId: userId }).toArray();
  const feeds  = await db.collection("feeds").find({ ownerId: userId }).toArray();
  res.json({
    spaces: spaces.map(sanitizeGroup),
    feeds:  feeds.map(sanitizeGroup),
  });
});

/**
 * GET /api/me/spaces
 * Returns all spaces the authenticated user is a member of (including private ones).
 */
app.get("/api/me/spaces", requireAuth, async (req, res) => {
  const userId = req.user.userId;
  const spaces = await db.collection("spaces")
    .find({ "members.userId": userId, banned: { $ne: true } })
    .toArray();
  res.json({ spaces: spaces.map(s => ({
    spaceId:      s.spaceId,
    name:         s.name,
    bio:          s.bio,
    profileImage: s.profileImage,
    memberCount:  s.memberCount || 0,
    xp:           s.xp || 0,
    premium:      s.premium || false,
    premiumExpiry: s.premiumExpiry || null,
    isPrivate:    s.isPrivate || false,
    joinLink:     s.joinLink,
    ownerId:      s.ownerId,
    isOwner:      s.ownerId === userId,
    isAdmin:      !!(s.members || []).find(m => m.userId === userId)?.isAdmin,
    createdAt:    s.createdAt,
  })) });
});

/**
 * GET /api/me/feeds
 * Returns all feeds the authenticated user is a member of (including private ones).
 */
app.get("/api/me/feeds", requireAuth, async (req, res) => {
  const userId = req.user.userId;
  const feeds = await db.collection("feeds")
    .find({ "members.userId": userId, banned: { $ne: true } })
    .toArray();
  res.json({ feeds: feeds.map(f => ({
    feedId:       f.feedId,
    name:         f.name,
    bio:          f.bio,
    profileImage: f.profileImage,
    memberCount:  f.memberCount || 0,
    xp:           f.xp || 0,
    premium:      f.premium || false,
    premiumExpiry: f.premiumExpiry || null,
    isPrivate:    f.isPrivate || false,
    joinLink:     f.joinLink,
    ownerId:      f.ownerId,
    isOwner:      f.ownerId === userId,
    isAdmin:      !!(f.members || []).find(m => m.userId === userId)?.isAdmin,
    createdAt:    f.createdAt,
  })) });
});

// ── Join Space ────────────────────────────────────────────────────────────────
app.post("/api/spaces/join/:joinId", requireAuth, async (req, res) => {
  const userId  = req.user.userId;
  const spaces  = db.collection("spaces");
  const joinId  = req.params.joinId.replace(/.*\/join-space\//, "");
  const space   = await spaces.findOne({ joinId });
  if (!space) return res.status(404).json({ error: "Space not found." });
  if (space.banned) return res.json({ banned: true });

  const existing = getMember(space, userId);
  if (existing?.isBanned) return res.status(403).json({ error: "You are banned from this space." });
  if (existing) return res.status(409).json({ error: "Already a member." });

  const user = await db.collection("users").findOne({ userId });
  const now  = new Date().toISOString();
  const memberEntry = {
    userId, name: user.name, username: user.username,
    profilePicture: user.profilePicture, premium: user.premium, verified: user.verified,
    isAdmin: false, isOwner: false, isMuted: false, isBanned: false,
    joinedAt: now, promotedBy: null,
  };

  await spaces.updateOne({ spaceId: space.spaceId }, {
    $push: { members: memberEntry },
    $inc:  { memberCount: 1 },
  });

  broadcastToGroup(space.spaceId, { type: "memberJoined", spaceId: space.spaceId, member: memberEntry });
  res.json({ message: "Joined space.", spaceId: space.spaceId });
});

// ── Join Feed ─────────────────────────────────────────────────────────────────
app.post("/api/feeds/join/:joinId", requireAuth, async (req, res) => {
  const userId = req.user.userId;
  const feeds  = db.collection("feeds");
  const joinId = req.params.joinId.replace(/.*\/join-feed\//, "");
  const feed   = await feeds.findOne({ joinId });
  if (!feed) return res.status(404).json({ error: "Feed not found." });
  if (feed.banned) return res.json({ banned: true });

  const existing = getMember(feed, userId);
  if (existing?.isBanned) return res.status(403).json({ error: "You are banned from this feed." });
  if (existing) return res.status(409).json({ error: "Already a member." });

  const user = await db.collection("users").findOne({ userId });
  const now  = new Date().toISOString();
  const memberEntry = {
    userId, name: user.name, username: user.username,
    profilePicture: user.profilePicture, premium: user.premium, verified: user.verified,
    isAdmin: false, isOwner: false, isMuted: false, isBanned: false,
    joinedAt: now, promotedBy: null,
  };

  await feeds.updateOne({ feedId: feed.feedId }, {
    $push: { members: memberEntry },
    $inc:  { memberCount: 1 },
  });

  broadcastToGroup(feed.feedId, { type: "memberJoined", feedId: feed.feedId, member: memberEntry });
  res.json({ message: "Joined feed.", feedId: feed.feedId });
});

// ── Leave Space ───────────────────────────────────────────────────────────────
app.post("/api/spaces/:spaceId/leave", requireAuth, async (req, res) => {
  const userId = req.user.userId;
  const spaces = db.collection("spaces");
  const space  = await resolveSpace(req.params.spaceId);
  if (!space) return res.status(404).json({ error: "Space not found." });

  const member = getMember(space, userId);
  if (!member) return res.status(404).json({ error: "Not a member." });

  await spaces.updateOne({ spaceId: space.spaceId }, {
    $pull: { members: { userId } },
    $inc:  { memberCount: -1 },
  });
  broadcastToGroup(space.spaceId, { type: "memberLeft", spaceId: space.spaceId, userId });
  res.json({ message: "Left space." });
});

// ── Leave Feed ────────────────────────────────────────────────────────────────
app.post("/api/feeds/:feedId/leave", requireAuth, async (req, res) => {
  const userId = req.user.userId;
  const feeds  = db.collection("feeds");
  const feed   = await resolveFeed(req.params.feedId);
  if (!feed) return res.status(404).json({ error: "Feed not found." });

  const member = getMember(feed, userId);
  if (!member) return res.status(404).json({ error: "Not a member." });

  await feeds.updateOne({ feedId: feed.feedId }, {
    $pull: { members: { userId } },
    $inc:  { memberCount: -1 },
  });
  broadcastToGroup(feed.feedId, { type: "memberLeft", feedId: feed.feedId, userId });
  res.json({ message: "Left feed." });
});

// ── Edit Space ────────────────────────────────────────────────────────────────
app.patch("/api/spaces/:spaceId", requireAuth, upload.single("profileImage"), async (req, res) => {
  const userId = req.user.userId;
  const spaces = db.collection("spaces");
  const space  = await resolveSpace(req.params.spaceId);
  if (!space) return res.status(404).json({ error: "Space not found." });
  if (!isAdmin(space, userId)) return res.status(403).json({ error: "Admin access required." });

  const updates = {};
  if (req.body.name?.trim()) updates.name = req.body.name.trim();
  if (req.body.bio !== undefined) updates.bio = req.body.bio.trim();
  if (req.body.isPrivate !== undefined) updates.isPrivate = req.body.isPrivate === "true" || req.body.isPrivate === true;
  if (req.file) {
    try { updates.profileImage = await uploadMedia(req.file.buffer, req.file.originalname || "space.jpg"); }
    catch (e) { return res.status(500).json({ error: "Upload failed." }); }
  }
  if (Object.keys(updates).length === 0) return res.json({ message: "Nothing to update." });

  await spaces.updateOne({ spaceId: space.spaceId }, { $set: updates });
  broadcastToGroup(space.spaceId, { type: "groupUpdated", spaceId: space.spaceId, updates });
  res.json({ message: "Space updated.", space: sanitizeGroup({ ...space, ...updates }) });
});

// ── Edit Feed ─────────────────────────────────────────────────────────────────
app.patch("/api/feeds/:feedId", requireAuth, upload.single("profileImage"), async (req, res) => {
  const userId = req.user.userId;
  const feeds  = db.collection("feeds");
  const feed   = await resolveFeed(req.params.feedId);
  if (!feed) return res.status(404).json({ error: "Feed not found." });
  if (!isAdmin(feed, userId)) return res.status(403).json({ error: "Admin access required." });

  const updates = {};
  if (req.body.name?.trim()) updates.name = req.body.name.trim();
  if (req.body.bio !== undefined) updates.bio = req.body.bio.trim();
  if (req.body.isPrivate !== undefined) updates.isPrivate = req.body.isPrivate === "true" || req.body.isPrivate === true;
  if (req.file) {
    try { updates.profileImage = await uploadMedia(req.file.buffer, req.file.originalname || "feed.jpg"); }
    catch (e) { return res.status(500).json({ error: "Upload failed." }); }
  }
  if (Object.keys(updates).length === 0) return res.json({ message: "Nothing to update." });

  await feeds.updateOne({ feedId: feed.feedId }, { $set: updates });
  broadcastToGroup(feed.feedId, { type: "groupUpdated", feedId: feed.feedId, updates });
  res.json({ message: "Feed updated.", feed: sanitizeGroup({ ...feed, ...updates }) });
});

// ── Promote / Demote member ───────────────────────────────────────────────────
// groupType: space | feed
app.post("/api/:groupType/:groupId/members/:targetId/promote", requireAuth, async (req, res) => {
  const { groupType, groupId, targetId } = req.params;
  const actorId = req.user.userId;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";
  const group   = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });

  const actor  = getMember(group, actorId);
  const target = getMember(group, targetId);
  if (!actor?.isAdmin && !isOwner(group, actorId)) return res.status(403).json({ error: "Admin access required." });
  if (!target) return res.status(404).json({ error: "Target not a member." });
  if (target.isAdmin) return res.status(409).json({ error: "Already an admin." });

  await db.collection(col).updateOne(
    { [idField]: groupId, "members.userId": targetId },
    { $set: { "members.$.isAdmin": true, "members.$.promotedBy": actorId } }
  );
  broadcastToGroup(groupId, { type: "memberPromoted", groupId, targetId, promotedBy: actorId });
  res.json({ message: "Member promoted to admin." });
});

app.post("/api/:groupType/:groupId/members/:targetId/demote", requireAuth, async (req, res) => {
  const { groupType, groupId, targetId } = req.params;
  const actorId = req.user.userId;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";
  const group   = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });

  const actor  = getMember(group, actorId);
  const target = getMember(group, targetId);
  if (!target) return res.status(404).json({ error: "Target not a member." });

  // Only owner OR the admin who promoted target can demote
  const canDemote = isOwner(group, actorId) || target.promotedBy === actorId;
  if (!canDemote) return res.status(403).json({ error: "Only the owner or promoting admin can demote." });
  if (target.isOwner) return res.status(403).json({ error: "Cannot demote the owner." });

  await db.collection(col).updateOne(
    { [idField]: groupId, "members.userId": targetId },
    { $set: { "members.$.isAdmin": false, "members.$.promotedBy": null } }
  );
  broadcastToGroup(groupId, { type: "memberDemoted", groupId, targetId });
  res.json({ message: "Admin rights revoked." });
});

// ── Ban / Unban member ────────────────────────────────────────────────────────
app.post("/api/:groupType/:groupId/members/:targetId/ban", requireAuth, async (req, res) => {
  const { groupType, groupId, targetId } = req.params;
  const actorId = req.user.userId;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";
  const group   = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });

  if (!isAdmin(group, actorId)) return res.status(403).json({ error: "Admin access required." });
  const target = getMember(group, targetId);
  if (!target) return res.status(404).json({ error: "Target not a member." });
  // Admins can't ban other admins — only owner can
  if (target.isAdmin && !isOwner(group, actorId))
    return res.status(403).json({ error: "Only the owner can ban admins." });
  if (target.isOwner) return res.status(403).json({ error: "Cannot ban the owner." });

  await db.collection(col).updateOne(
    { [idField]: groupId, "members.userId": targetId },
    {
      $set: { "members.$.isBanned": true },
      $inc: { memberCount: -1 },
    }
  );
  broadcastToGroup(groupId, { type: "memberBanned", groupId, targetId, bannedBy: actorId });
  res.json({ message: "Member banned." });
});

app.post("/api/:groupType/:groupId/members/:targetId/unban", requireAuth, async (req, res) => {
  const { groupType, groupId, targetId } = req.params;
  const actorId = req.user.userId;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";
  const group   = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });
  if (!isAdmin(group, actorId)) return res.status(403).json({ error: "Admin access required." });

  await db.collection(col).updateOne(
    { [idField]: groupId, "members.userId": targetId },
    {
      $set: { "members.$.isBanned": false },
      $inc: { memberCount: 1 },
    }
  );
  res.json({ message: "Member unbanned." });
});

// ── Mute / Unmute member ──────────────────────────────────────────────────────
app.post("/api/:groupType/:groupId/members/:targetId/mute", requireAuth, async (req, res) => {
  const { groupType, groupId, targetId } = req.params;
  const actorId = req.user.userId;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";
  const group   = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });
  if (!isAdmin(group, actorId)) return res.status(403).json({ error: "Admin access required." });

  const muted = req.body.muted !== false; // default true
  await db.collection(col).updateOne(
    { [idField]: groupId, "members.userId": targetId },
    { $set: { "members.$.isMuted": muted } }
  );
  broadcastToGroup(groupId, { type: "memberMuted", groupId, targetId, muted });
  res.json({ message: muted ? "Member muted." : "Member unmuted." });
});

// ── Mute media for whole space/feed ──────────────────────────────────────────
app.post("/api/:groupType/:groupId/mute-media", requireAuth, async (req, res) => {
  const { groupType, groupId } = req.params;
  const actorId = req.user.userId;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";
  const group   = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });
  if (!isAdmin(group, actorId)) return res.status(403).json({ error: "Admin access required." });

  const mutedMedia = req.body.mutedMedia !== false;
  await db.collection(col).updateOne({ [idField]: groupId }, { $set: { mutedMedia } });
  broadcastToGroup(groupId, { type: "groupMediaMuted", groupId, mutedMedia });
  res.json({ message: mutedMedia ? "Media muted in group." : "Media unmuted in group." });
});

// ── Get Members ───────────────────────────────────────────────────────────────
app.get("/api/:groupType/:groupId/members", requireAuth, async (req, res) => {
  const { groupType, groupId } = req.params;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";
  const group   = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });
  if (group.banned) return res.json({ banned: true });
  res.json({ members: group.members || [] });
});

// ── Delete Space / Feed ───────────────────────────────────────────────────────
app.delete("/api/spaces/:spaceId", requireAuth, async (req, res) => {
  const space = await resolveSpace(req.params.spaceId);
  if (!space) return res.status(404).json({ error: "Space not found." });
  if (space.ownerId !== req.user.userId) return res.status(403).json({ error: "Only owner can delete." });
  await db.collection("spaces").deleteOne({ spaceId: space.spaceId });
  await db.collection("groupMessages").deleteMany({ groupId: space.spaceId });
  broadcastToGroup(space.spaceId, { type: "groupDeleted", spaceId: space.spaceId });
  res.json({ message: "Space deleted." });
});

app.delete("/api/feeds/:feedId", requireAuth, async (req, res) => {
  const feed = await resolveFeed(req.params.feedId);
  if (!feed) return res.status(404).json({ error: "Feed not found." });
  if (feed.ownerId !== req.user.userId) return res.status(403).json({ error: "Only owner can delete." });
  await db.collection("feeds").deleteOne({ feedId: feed.feedId });
  await db.collection("groupMessages").deleteMany({ groupId: feed.feedId });
  broadcastToGroup(feed.feedId, { type: "groupDeleted", feedId: feed.feedId });
  res.json({ message: "Feed deleted." });
});

// ── Report Space / Feed ───────────────────────────────────────────────────────
app.post("/api/:groupType/:groupId/report", requireAuth, async (req, res) => {
  const { groupType, groupId } = req.params;
  const { reason, detail }    = req.body;
  const validReasons = ["spam", "inappropriate", "scam", "harassment", "other"];
  if (!validReasons.includes(reason)) return res.status(400).json({ error: "Invalid reason." });

  const reportId = uuidv4();
  await db.collection("reports").insertOne({
    reportId, reporterId: req.user.userId, reportedId: groupId,
    reportedType: groupType, reason, detail: detail?.trim() || "",
    status: "open", createdAt: new Date().toISOString(),
    reviewedAt: null, reviewedBy: null, action: null,
  });
  res.status(201).json({ message: "Report submitted.", reportId });
});

// ── Admin: Ban / Unban Space or Feed ─────────────────────────────────────────
app.post("/api/admin/:groupType/:groupId/ban", requireAdmin, async (req, res) => {
  const { groupType, groupId } = req.params;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";
  const { reason } = req.body;
  await db.collection(col).updateOne({ [idField]: groupId }, { $set: { banned: true, banReason: reason || "Violation of terms." } });
  res.json({ message: `${groupType} banned.` });
});

app.post("/api/admin/:groupType/:groupId/unban", requireAdmin, async (req, res) => {
  const { groupType, groupId } = req.params;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";
  await db.collection(col).updateOne({ [idField]: groupId }, { $set: { banned: false, banReason: null } });
  res.json({ message: `${groupType} unbanned.` });
});

// ════════════════════════════════════════════════════════════════════════════
//  GROUP MESSAGES (Spaces & Feeds)
// ════════════════════════════════════════════════════════════════════════════

// GROUP_TEXT_MAX / GROUP_CAPTION_MAX / SPACE_ALLOWED_TYPES moved to top

/**
 * POST /api/:groupType/:groupId/messages
 * multipart/form-data — same field convention as DM messages
 * For feeds: only admins/owner may send
 */
app.post("/api/:groupType/:groupId/messages",
  requireAuth,
  upload.fields([{ name: "media", maxCount: 1 }]),
  async (req, res) => {
    const { groupType, groupId } = req.params;
    const senderId = req.user.userId;
    const col      = groupType === "space" ? "spaces" : "feeds";
    const idField  = groupType === "space" ? "spaceId" : "feedId";

    const group  = await db.collection(col).findOne({ [idField]: groupId });
    if (!group) return res.status(404).json({ error: "Group not found." });
    if (group.banned) return res.json({ banned: true });

    const member = getMember(group, senderId);
    if (!member)           return res.status(403).json({ error: "You are not a member." });
    if (member.isBanned)   return res.status(403).json({ error: "You are banned from this group." });
    if (member.isMuted)    return res.status(403).json({ error: "You are muted in this group." });
    // Feeds: only admin/owner can post
    if (groupType === "feed" && !member.isAdmin && !isOwner(group, senderId))
      return res.status(403).json({ error: "Only admins can post in a feed." });

    const { type, text, caption, replyToMessageId } = req.body;
    if (!SPACE_ALLOWED_TYPES.includes(type))
      return res.status(400).json({ error: `type must be one of: ${SPACE_ALLOWED_TYPES.join(", ")}` });

    // Media mute check
    if (group.mutedMedia && type !== "text")
      return res.status(403).json({ error: "Media messages are disabled in this group." });

    const content = { replyToMessageId: replyToMessageId || null };

    if (type === "text") {
      if (!text?.trim()) return res.status(400).json({ error: "text is required." });
      // Support line breaks — just store as-is (trim leading/trailing only)
      const trimmed = text.replace(/^\s+|\s+$/g, "");
      if (trimmed.length > GROUP_TEXT_MAX) return res.status(400).json({ error: `Text exceeds ${GROUP_TEXT_MAX} chars.` });
      content.text = trimmed;
    }

    const needsMedia = ["image", "image_caption", "sticker", "audio", "voice"];
    if (needsMedia.includes(type)) {
      // Stickers may be sent by URL instead of a file upload
      if (type === "sticker" && req.body.stickerUrl) {
        const stickerUrl = req.body.stickerUrl.trim();
        if (!/^https?:\/\/.+/.test(stickerUrl))
          return res.status(400).json({ error: "stickerUrl must be a valid http(s) URL." });
        const owned = await db.collection("stickers").countDocuments({ urls: stickerUrl });
        if (owned === 0)
          return res.status(400).json({ error: "stickerUrl is not a recognised sticker." });
        content.mediaUrl = stickerUrl;
      } else {
        const file = req.files?.media?.[0];
        if (!file) return res.status(400).json({ error: "media file is required." });
        try { content.mediaUrl = await uploadMedia(file.buffer, file.originalname || "media"); }
        catch (e) { return res.status(500).json({ error: "Upload failed: " + e.message }); }
        content.mediaSize     = file.size;
        content.mediaMimeType = file.mimetype;
      }
    }
    if (["image_caption"].includes(type)) {
      if (caption && caption.length > GROUP_CAPTION_MAX)
        return res.status(400).json({ error: "Caption too long." });
      content.caption = caption?.trim() || "";
    }
    if (type === "sticker") content.isSticker = true;
    if (type === "voice")   content.isVoice   = true;

    // Handle reply — include full original message content
    let replyTo = null;
    if (content.replyToMessageId) {
      const orig = await db.collection("groupMessages").findOne({ messageId: content.replyToMessageId });
      if (orig && !orig.deleted) {
        replyTo = {
          messageId:      orig.messageId,
          senderId:       orig.senderId,
          senderName:     orig.senderName,
          senderUsername: orig.senderUsername,
          senderAvatar:   orig.senderAvatar,
          senderIsBot:    orig.senderIsBot || false,
          type:           orig.type,
          content:        orig.content,   // full content for bots/UIs
          preview:        orig.type === "text"
            ? (orig.content?.text || "").slice(0, 60) + ((orig.content?.text || "").length > 60 ? "…" : "")
            : `[${orig.type}]`,
          sentAt:         orig.sentAt,
        };
      }
    }

    // Extract @mentions (e.g. @username)
    const mentionedUsernames = (content.text || "")
      .match(/@([a-zA-Z0-9_]+)/g)?.map(m => m.slice(1).toLowerCase()) || [];

    const user      = await db.collection("users").findOne({ userId: senderId });
    const messageId = uuidv4();
    const now       = new Date().toISOString();

    const message = {
      messageId,
      groupId,
      chatId:      groupId,          // unified field for bot handlers
      chatType:    groupType,        // "space" | "feed"
      chatName:    group.name,
      type, content, replyTo,
      senderId,
      senderIsBot: false,
      senderName:     user.name,
      senderUsername: user.username,
      senderAvatar:   user.profilePicture,
      senderPremium:  user.premium,
      senderVerified: user.verified,
      senderLevel:    user.level || 1,
      senderPosition: getMember(group, senderId)?.isAdmin ? (isOwner(group, senderId) ? "owner" : "admin") : "member",
      mentions:       mentionedUsernames,
      sentAt:         now,
      editedAt:       null,
      deleted:        false,
      reactions:      {},
      readBy:         [senderId],
      seenMentions:   [],
    };

    await db.collection("groupMessages").insertOne(message);

    // Grant XP to space/feed for activity
    const xpAction_ = ["image","image_caption","video","video_caption","sticker","audio","voice"].includes(type)
      ? "sendMedia" : "sendMessage";
    const grpCol_    = groupType === "space" ? "spaces" : "feeds";
    const grpIdField_= groupType === "space" ? "spaceId" : "feedId";
    grantGroupXP(grpCol_, grpIdField_, groupId, xpAction_).catch(() => {});
    grantXP(senderId, "sendMessage", 1).catch(() => {});

    // Emit chatListUpdate so clients can reorder their chat list
    broadcastToGroup(groupId, {
      type:    "chatListUpdate",
      chatId:  groupId,
      chatType: groupType,
      lastMessage: {
        preview:    type === "text"
          ? (content.text || "").slice(0, 80) + ((content.text || "").length > 80 ? "…" : "")
          : `[${type}]`,
        type,
        sentAt:     now,
        senderName: user.name,
        fromMe:     false,
      },
    }, null);

    // Build full Telegram-style update envelope for broadcast
    // Each member evaluates fromMe = (message.senderId === myUserId)
    // We embed senderId in the message so every client/bot can compute it
    const groupUpdate = {
      type: "newGroupMessage",
      chat: {
        chatId:   groupId,
        chatType: groupType,  // "space" or "feed"
        chatName: group.name,
        joinLink: group.joinLink,
      },
      from: {
        userId:         user.userId,
        name:           user.name,
        username:       user.username,
        tag:            user.tag,
        profilePicture: user.profilePicture,
        premium:        user.premium,
        verified:       user.verified,
        level:          user.level || computeLevel(user.xp || 0),
        isAdmin:        member.isAdmin,
        isOwner:        isOwner(group, senderId),
        isBot:          false,
      },
      // fromMe: clients compare message.senderId to their own userId
      // For convenience we broadcast TWO events: one for the sender (fromMe:true), rest (fromMe:false)
      message: { ...message, chatId: groupId, chatType: groupType },
      groupId,
    };
    // Everyone except sender gets fromMe: false
    broadcastToGroup(groupId, { ...groupUpdate, fromMe: false }, senderId);
    // Sender gets fromMe: true
    const senderSockets = onlineClients.get(senderId) || new Set();
for (const socketId of senderSockets) {
  mainNsp.to(socketId).emit("message", { ...groupUpdate, fromMe: true });
}

    // Push groupUnreadCount update to CP for all members (except sender)
    {
      const grpMembers = group.members || [];
      for (const m_ of grpMembers) {
        if (m_.userId === senderId || m_.isBanned) continue;
        broadcastCP(m_.userId, {
          type:     "groupUnreadCount",
          chatId:   groupId,
          chatType: groupType,
        });
      }
    }

    // Push mention notifications
    if (mentionedUsernames.length > 0) {
      const mentionedUsers = await db.collection("users").find({
        usernameLower: { $in: mentionedUsernames },
      }).toArray();
      for (const mu of mentionedUsers) {
        broadcastCP(mu.userId, {
          type:           "mention",
          groupId,
          groupType,
          chatName:       group.name,
          messageId,
          sentAt:         now,
          senderId:       user.userId,
          senderName:     user.name,
          senderUsername: user.username,
          senderAvatar:   user.profilePicture,
          senderPremium:  user.premium,
          content:        content,
          msgType:        type,
          replyTo:        replyTo || null,
          preview:        (content.text || "").slice(0, 80),
        });
      }
    }

    res.status(201).json({ message });
  }
);

/**
 * GET /api/:groupType/:groupId/messages
 * Cursor pagination with three modes:
 *   ?before=<messageId>        — load older messages (scroll up)
 *   ?after=<messageId>         — load newer messages (scroll down)
 *   ?around=<messageId>&limit= — center on a specific message (jump-to)
 *                                returns floor(limit/2) before + anchor + rest after
 *                                includes hasMoreAbove + hasMoreBelow for both scroll dirs
 */
app.get("/api/:groupType/:groupId/messages", requireAuth, async (req, res) => {
  const { groupType, groupId } = req.params;
  const userId   = req.user.userId;
  const limit    = Math.min(parseInt(req.query.limit) || 30, 100);
  const beforeId = req.query.before;
  const afterId  = req.query.after;
  const aroundId = req.query.around;
  const col      = groupType === "space" ? "spaces" : "feeds";
  const idField  = groupType === "space" ? "spaceId" : "feedId";

  const group = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });
  if (group.banned) return res.json({ banned: true });

  const member = getMember(group, userId);
  if (!member || member.isBanned) return res.status(403).json({ error: "Access denied." });

  const gmsgs     = db.collection("groupMessages");
  const baseQuery = { groupId, deleted: false };

  // Apply clearedAt filter (per-user clear chat)
  const clearedAt = member.clearedAt;
  if (clearedAt) baseQuery.sentAt = { $gt: clearedAt };

  let sorted;
  let hasMoreAbove = false;
  let hasMoreBelow = false;

  if (aroundId) {
    // ── AROUND MODE ──────────────────────────────────────────────────────────
    const anchor = await gmsgs.findOne({ messageId: aroundId, groupId });
    if (!anchor) return res.status(404).json({ error: "Message not found." });

    const half   = Math.floor(limit / 2);
    const aboveN = half;
    const belowN = limit - half - 1; // remaining slots after anchor

    const aboveQuery = { ...baseQuery, sentAt: { ...(baseQuery.sentAt || {}), $lt: anchor.sentAt } };
    const belowQuery = { ...baseQuery, sentAt: { ...(baseQuery.sentAt || {}), $gt: anchor.sentAt } };

    const [aboveDocs, belowDocs] = await Promise.all([
      gmsgs.find(aboveQuery).sort({ sentAt: -1 }).limit(aboveN + 1).toArray(), // +1 to detect hasMore
      gmsgs.find(belowQuery).sort({ sentAt:  1 }).limit(belowN + 1).toArray(),
    ]);

    hasMoreAbove = aboveDocs.length > aboveN;
    hasMoreBelow = belowDocs.length > belowN;

    const above = aboveDocs.slice(0, aboveN).reverse(); // trim extra, then put in asc order
    const below = belowDocs.slice(0, belowN);

    sorted = [...above, anchor, ...below];

  } else {
    // ── BEFORE / AFTER MODE ──────────────────────────────────────────────────
    const query = { ...baseQuery };

    if (beforeId) {
      const cursor = await gmsgs.findOne({ messageId: beforeId });
      if (cursor) {
        const existing = query.sentAt || {};
        query.sentAt = { ...existing, $lt: cursor.sentAt };
      }
    } else if (afterId) {
      const cursor = await gmsgs.findOne({ messageId: afterId });
      if (cursor) {
        const existing = query.sentAt || {};
        query.sentAt = { ...existing, $gt: cursor.sentAt };
      }
    }

    const sortDir = afterId ? 1 : -1;
    const docs    = await gmsgs.find(query).sort({ sentAt: sortDir }).limit(limit + 1).toArray();

    const hasMore = docs.length > limit;
    const trimmed = docs.slice(0, limit);
    sorted        = sortDir === -1 ? trimmed.reverse() : trimmed;

    hasMoreAbove  = afterId  ? true  : hasMore;  // when going up, hasMore = more above
    hasMoreBelow  = afterId  ? hasMore : false;   // when going down, hasMore = more below
  }

  // Mark as read (background)
  const unreadIds = sorted.filter(m => !m.readBy?.includes(userId)).map(m => m.messageId);
  if (unreadIds.length > 0) {
    gmsgs.updateMany(
      { messageId: { $in: unreadIds } },
      { $addToSet: { readBy: userId } }
    ).catch(() => {});
  }

  // Pinned message
  let pinnedMessage = null;
  if (group.pinnedMessageId) {
    pinnedMessage = await gmsgs.findOne({ messageId: group.pinnedMessageId });
    if (pinnedMessage?.deleted) pinnedMessage = null;
  }

  res.json({
    messages: sorted,
    pinnedMessage,
    pagination: {
      count:           sorted.length,
      hasMoreAbove,
      hasMoreBelow,
      hasMore:         hasMoreAbove || hasMoreBelow, // legacy compat
      oldestMessageId: sorted[0]?.messageId                   || null,
      newestMessageId: sorted[sorted.length - 1]?.messageId   || null,
      anchorMessageId: aroundId || null,
    },
  });
});

// ── Delete group message (own) ────────────────────────────────────────────────
app.delete("/api/:groupType/:groupId/messages/:messageId", requireAuth, async (req, res) => {
  const { groupType, groupId, messageId } = req.params;
  const userId = req.user.userId;
  const col    = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";

  const group = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });
  const member = getMember(group, userId);

  const msg = await db.collection("groupMessages").findOne({ messageId, groupId });
  if (!msg) return res.status(404).json({ error: "Message not found." });
  if (msg.deleted) return res.status(410).json({ error: "Already deleted." });

  // Own message or admin
  if (msg.senderId !== userId && !isAdmin(group, userId))
    return res.status(403).json({ error: "Cannot delete this message." });

  await db.collection("groupMessages").updateOne(
    { messageId },
    { $set: { deleted: true, content: {}, deletedAt: new Date().toISOString() } }
  );

  // Clear pin if this was the pinned message
  if (group.pinnedMessageId === messageId) {
    await db.collection(col).updateOne({ [idField]: groupId }, { $set: { pinnedMessageId: null } });
    broadcastToGroup(groupId, { type: "messagePinned", groupId, messageId: null });
  }

  broadcastToGroup(groupId, {
    type: "groupMessageDeleted",
    groupId,
    messageId,
    chat: { chatId: groupId, chatType: groupType },
    actorId: userId,
    sentAt: msg.sentAt,
    senderId: msg.senderId,
  });
  res.json({ message: "Message deleted." });
});

// ── Admin: Delete all messages by a user ──────────────────────────────────────
app.delete("/api/:groupType/:groupId/members/:targetId/messages", requireAuth, async (req, res) => {
  const { groupType, groupId, targetId } = req.params;
  const actorId = req.user.userId;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";

  const group = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });
  if (!isAdmin(group, actorId)) return res.status(403).json({ error: "Admin access required." });

  const now = new Date().toISOString();
  const result = await db.collection("groupMessages").updateMany(
    { groupId, senderId: targetId, deleted: false },
    { $set: { deleted: true, content: {}, deletedAt: now } }
  );

  broadcastToGroup(groupId, { type: "allUserMessagesDeleted", groupId, userId: targetId, count: result.modifiedCount });
  res.json({ message: `${result.modifiedCount} messages deleted.` });
});

// ── Edit group message (text only) ────────────────────────────────────────────
app.patch("/api/:groupType/:groupId/messages/:messageId", requireAuth, async (req, res) => {
  const { groupType, groupId, messageId } = req.params;
  const userId = req.user.userId;
  const { text } = req.body;

  if (!text?.trim()) return res.status(400).json({ error: "text is required." });
  if (text.length > GROUP_TEXT_MAX) return res.status(400).json({ error: "Text too long." });

  const msg = await db.collection("groupMessages").findOne({ messageId, groupId });
  if (!msg)         return res.status(404).json({ error: "Message not found." });
  if (msg.deleted)  return res.status(410).json({ error: "Message deleted." });
  if (msg.senderId !== userId) return res.status(403).json({ error: "Cannot edit someone else's message." });
  if (msg.type !== "text") return res.status(400).json({ error: "Only text messages can be edited." });

  const editedAt = new Date().toISOString();
  await db.collection("groupMessages").updateOne(
    { messageId },
    { $set: { "content.text": text.trim(), editedAt } }
  );

  broadcastToGroup(groupId, { type: "groupMessageEdited", groupId, messageId, text: text.trim(), editedAt });
  res.json({ message: "Message edited." });
});

// ── React to group message ────────────────────────────────────────────────────
app.post("/api/:groupType/:groupId/messages/:messageId/react", requireAuth, async (req, res) => {
  const { groupType, groupId, messageId } = req.params;
  const userId = req.user.userId;
  const { emoji } = req.body;

  if (!emoji) return res.status(400).json({ error: "emoji is required." });

  const msg = await db.collection("groupMessages").findOne({ messageId, groupId });
  if (!msg)        return res.status(404).json({ error: "Message not found." });
  if (msg.deleted) return res.status(410).json({ error: "Cannot react to a deleted message." });

  const key        = `reactions.${emoji}`;
  const hasReacted = (msg.reactions?.[emoji] || []).includes(userId);

  if (hasReacted) {
    await db.collection("groupMessages").updateOne({ messageId }, { $pull: { [key]: userId } });
  } else {
    await db.collection("groupMessages").updateOne({ messageId }, { $addToSet: { [key]: userId } });
  }

  const updated  = await db.collection("groupMessages").findOne({ messageId });
  const reactions = updated.reactions || {};

  // Build enriched reactions: { emoji: { count, users: [userId,...] } }
  const enriched = {};
  for (const [em, uids] of Object.entries(reactions)) {
    enriched[em] = { count: uids.length, users: uids };
  }

  broadcastToGroup(groupId, {
    type: "groupMessageReaction", groupId, messageId,
    reactions: enriched,
    actorId: userId, emoji, added: !hasReacted,
  });
  res.json({ reactions: enriched });
});

// ── Pin message ────────────────────────────────────────────────────────────────
app.post("/api/:groupType/:groupId/messages/:messageId/pin", requireAuth, async (req, res) => {
  const { groupType, groupId, messageId } = req.params;
  const actorId = req.user.userId;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";

  const group = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });
  if (!isAdmin(group, actorId)) return res.status(403).json({ error: "Admin access required." });

  const msg = await db.collection("groupMessages").findOne({ messageId, groupId });
  if (!msg || msg.deleted) return res.status(404).json({ error: "Message not found or deleted." });

  await db.collection(col).updateOne({ [idField]: groupId }, { $set: { pinnedMessageId: messageId } });
  broadcastToGroup(groupId, { type: "messagePinned", groupId, messageId, pinnedBy: actorId });
  res.json({ message: "Message pinned." });
});

// ── Unpin message ─────────────────────────────────────────────────────────────
app.post("/api/:groupType/:groupId/unpin", requireAuth, async (req, res) => {
  const { groupType, groupId } = req.params;
  const actorId = req.user.userId;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";

  const group = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });
  if (!isAdmin(group, actorId)) return res.status(403).json({ error: "Admin access required." });

  await db.collection(col).updateOne({ [idField]: groupId }, { $set: { pinnedMessageId: null } });
  broadcastToGroup(groupId, { type: "messagePinned", groupId, messageId: null });
  res.json({ message: "Unpinned." });
});

// ── Get readers of a group message ─────────────────────────────────────────────
/**
 * GET /api/:groupType/:groupId/messages/:messageId/readers
 * Admins/sender see full reader list; others see only count.
 */
app.get("/api/:groupType/:groupId/messages/:messageId/readers", requireAuth, async (req, res) => {
  const { groupType, groupId, messageId } = req.params;
  const userId  = req.user.userId;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";

  const group = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });

  const member = getMember(group, userId);
  if (!member || member.isBanned) return res.status(403).json({ error: "Access denied." });

  const msg = await db.collection("groupMessages").findOne({ messageId, groupId });
  if (!msg || msg.deleted) return res.status(404).json({ error: "Message not found." });

  const readerIds  = (msg.readBy || []).filter(id => id !== msg.senderId);
  const canSeeWho  = member.isAdmin || msg.senderId === userId;

  if (!canSeeWho) return res.json({ count: readerIds.length });

  const readerDocs = await db.collection("users")
    .find({ userId: { $in: readerIds } })
    .project({ userId: 1, name: 1, username: 1, profilePicture: 1, premium: 1, verified: 1 })
    .toArray();

  res.json({
    count:   readerIds.length,
    readers: readerDocs.map(u => ({
      userId: u.userId, name: u.name, username: u.username,
      profilePicture: u.profilePicture, premium: u.premium, verified: u.verified,
    })),
  });
});

// ── Get unseen mentions (for tag floater) ────────────────────────────────────
/**
 * GET /api/:groupType/:groupId/mentions/unseen
 * Returns all messages in this group where the user was @mentioned but hasn't
 * dismissed yet. Sorted oldest→newest so frontend can navigate in order.
 * Includes full message + reply context so the floater can render a preview.
 *
 * Also supports:
 *   GET /api/mentions/unseen         — across ALL groups the user is in
 */
app.get("/api/:groupType/:groupId/mentions/unseen", requireAuth, async (req, res) => {
  const { groupId } = req.params;
  const userId   = req.user.userId;
  const me = await db.collection("users").findOne({ userId }, { projection: { username: 1 } });
  if (!me) return res.status(404).json({ error: "User not found." });

  const username = me.username?.toLowerCase();

  const msgs = await db.collection("groupMessages").find({
    groupId,
    deleted:      { $ne: true },
    mentions:     username,
    seenMentions: { $ne: userId },
  })
  .sort({ sentAt: 1 })   // oldest first → navigate forward naturally
  .toArray();

  res.json({
    groupId,
    count: msgs.length,
    unseen: msgs.map(m => ({
      messageId:      m.messageId,
      sentAt:         m.sentAt,
      senderId:       m.senderId,
      senderName:     m.senderName,
      senderUsername: m.senderUsername,
      senderAvatar:   m.senderAvatar,
      senderPremium:  m.senderPremium,
      content:        m.content,
      type:           m.type,
      replyTo:        m.replyTo || null,   // full replyTo so frontend can show "↩ replying to X"
    })),
  });
});

/**
 * GET /api/mentions/unseen
 * Cross-group: all unseen @mentions across every group the user belongs to.
 * Useful for a global notification badge or a global mentions panel.
 */
app.get("/api/mentions/unseen", requireAuth, async (req, res) => {
  const userId = req.user.userId;
  const me = await db.collection("users").findOne({ userId }, { projection: { username: 1 } });
  if (!me) return res.status(404).json({ error: "User not found." });

  const username = me.username?.toLowerCase();

  const msgs = await db.collection("groupMessages").find({
    deleted:      { $ne: true },
    mentions:     username,
    seenMentions: { $ne: userId },
  })
  .sort({ sentAt: 1 })
  .toArray();

  // Group by chatId so frontend can show per-group counts
  const byGroup = {};
  for (const m of msgs) {
    if (!byGroup[m.groupId]) byGroup[m.groupId] = { groupId: m.groupId, chatType: m.chatType, chatName: m.chatName, count: 0, messages: [] };
    byGroup[m.groupId].count++;
    byGroup[m.groupId].messages.push({
      messageId:      m.messageId,
      sentAt:         m.sentAt,
      senderId:       m.senderId,
      senderName:     m.senderName,
      senderUsername: m.senderUsername,
      senderAvatar:   m.senderAvatar,
      content:        m.content,
      type:           m.type,
      replyTo:        m.replyTo || null,
    });
  }

  res.json({
    totalCount: msgs.length,
    groups: Object.values(byGroup),
  });
});

// ── Mark a single mention as seen ────────────────────────────────────────────
/**
 * POST /api/:groupType/:groupId/messages/:messageId/seen-mention
 * Dismiss one specific @mention (user clicked through / navigated past it).
 */
app.post("/api/:groupType/:groupId/messages/:messageId/seen-mention", requireAuth, async (req, res) => {
  const { messageId } = req.params;
  const userId = req.user.userId;
  await db.collection("groupMessages").updateOne(
    { messageId },
    { $addToSet: { seenMentions: userId } }
  );
  res.json({ ok: true, messageId });
});

// ── Mark all mentions in a group as seen ─────────────────────────────────────
/**
 * POST /api/:groupType/:groupId/mentions/seen-all
 * Dismiss every pending @mention in this group at once (e.g. user left the chat).
 */
app.post("/api/:groupType/:groupId/mentions/seen-all", requireAuth, async (req, res) => {
  const { groupId } = req.params;
  const userId = req.user.userId;
  const me = await db.collection("users").findOne({ userId }, { projection: { username: 1 } });
  const username = me?.username?.toLowerCase();

  const result = await db.collection("groupMessages").updateMany(
    { groupId, mentions: username, seenMentions: { $ne: userId } },
    { $addToSet: { seenMentions: userId } }
  );
  res.json({ ok: true, cleared: result.modifiedCount });
});

// ── Mark ALL mentions everywhere as seen ─────────────────────────────────────
/**
 * POST /api/mentions/seen-all
 * Nuke every pending mention across all groups (e.g. "mark all read" button).
 */
app.post("/api/mentions/seen-all", requireAuth, async (req, res) => {
  const userId = req.user.userId;
  const me = await db.collection("users").findOne({ userId }, { projection: { username: 1 } });
  const username = me?.username?.toLowerCase();

  const result = await db.collection("groupMessages").updateMany(
    { mentions: username, seenMentions: { $ne: userId } },
    { $addToSet: { seenMentions: userId } }
  );
  res.json({ ok: true, cleared: result.modifiedCount });
});

// ── @mention member search ────────────────────────────────────────────────────
/**
 * GET /api/:groupType/:groupId/members/search?q=jo&limit=10
 * Fuzzy search members of a group by username or name.
 * Used when user types "@" in the input — returns matching members
 * so the frontend can show a pick-list and autocomplete the full @username.
 *
 * Returns: [{ userId, name, username, tag, profilePicture, premium, verified, isAdmin, isOwner }]
 */
app.get("/api/:groupType/:groupId/members/search", requireAuth, async (req, res) => {
  const { groupType, groupId } = req.params;
  const q     = (req.query.q || "").trim();
  const limit = Math.min(parseInt(req.query.limit) || 10, 20);

  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";

  const group = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });

  // Collect active (non-banned) member userIds
  const memberIds = (group.members || [])
    .filter(m => !m.isBanned)
    .map(m => m.userId);

  if (memberIds.length === 0) return res.json({ members: [] });

  const filter = { userId: { $in: memberIds } };
  if (q) {
    const escaped = q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    filter.$or = [
      { usernameLower: { $regex: escaped.toLowerCase() } },
      { name:          { $regex: new RegExp(escaped, "i") } },
    ];
  }

  const users = await db.collection("users")
    .find(filter)
    .limit(limit)
    .project({ userId: 1, name: 1, username: 1, tag: 1, profilePicture: 1, premium: 1, verified: 1 })
    .toArray();

  // Build a quick lookup for admin/owner status
  const memberMeta = Object.fromEntries((group.members || []).map(m => [m.userId, m]));

  res.json({
    members: users.map(u => ({
      userId:         u.userId,
      name:           u.name,
      username:       u.username,
      tag:            u.tag,
      profilePicture: u.profilePicture,
      premium:        u.premium  || false,
      verified:       u.verified || false,
      isAdmin:        memberMeta[u.userId]?.isAdmin || group.ownerId === u.userId,
      isOwner:        group.ownerId === u.userId,
    })),
  });
});

// ════════════════════════════════════════════════════════════════════════════
//  GIFTS
// ════════════════════════════════════════════════════════════════════════════

// Cost table
const PREMIUM_COSTS = {
  "1month":  { exiles: 5,  months: 1 },
  "3months": { exiles: 13, months: 3 },
};
const GIFT_PREMIUM_COST   = 6;   // exiles to gift premium to another user (1 month)
const GIFT_SPACE_COST     = 10;  // exiles to gift premium to a space/feed
const GIFT_FEED_COST      = 10;

/** Add months to a date string, returning ISO string */
function addMonths(dateStr, months) {
  const d = dateStr ? new Date(dateStr) : new Date();
  d.setMonth(d.getMonth() + months);
  return d.toISOString();
}

/**
 * Check a group (space/feed) for premium expiry and auto-expire in-place.
 * Mutates the group object so the calling route always reflects updated state.
 */
async function checkGroupPremiumExpiry(col, idField, groupId, group) {
  if (group.premium && group.premiumExpiry && new Date(group.premiumExpiry) <= new Date()) {
    await db.collection(col).updateOne(
      { [idField]: groupId },
      { $set: { premium: false, premiumExpiry: null } }
    );
    group.premium       = false;
    group.premiumExpiry = null;
  }
}

/**
 * POST /api/premium/buy
 * body: { plan: "1month" | "3months" }
 * Deducts exiles from requester's account, sets premium + expiry.
 */
app.post("/api/premium/buy", requireAuth, async (req, res) => {
  const userId = req.user.userId;
  const { plan } = req.body;
  const planInfo = PREMIUM_COSTS[plan];
  if (!planInfo) return res.status(400).json({ error: `plan must be "1month" or "3months".` });

  const users = db.collection("users");
  const user  = await users.findOne({ userId });
  if (!user) return res.status(404).json({ error: "User not found." });
  if ((user.exiles || 0) < planInfo.exiles)
    return res.status(402).json({ error: `Insufficient exiles. Need ${planInfo.exiles}, have ${user.exiles || 0}.` });

  const expiry = addMonths(user.premiumExpiry && new Date(user.premiumExpiry) > new Date() ? user.premiumExpiry : null, planInfo.months);

  await users.updateOne({ userId }, {
    $inc: { exiles: -planInfo.exiles },
    $set: { premium: true, premiumExpiry: expiry },
  });

  const newUserAfterBuy = await db.collection("users").findOne({ userId: req.user.userId }, { projection: { premium: 1, premiumExpiry: 1, exiles: 1 } });
  const daysLeftAfterBuy = newUserAfterBuy.premiumExpiry
    ? Math.max(0, Math.ceil((new Date(newUserAfterBuy.premiumExpiry) - Date.now()) / 86400000))
    : 0;
  broadcastCP(req.user.userId, { type: "premiumUpdate", premium: true, premiumExpiry: expiry, daysLeft: daysLeftAfterBuy });
  broadcastCP(req.user.userId, { type: "exilesUpdate", exiles: newUserAfterBuy.exiles || 0, delta: -planInfo.exiles, reason: "premium_purchase" });
  res.json({ message: `Premium activated for ${planInfo.months} month(s).`, premiumExpiry: expiry });
});

/**
 * POST /api/premium/gift/:recipientId
 * Gifts 1 month of premium to another user. Costs GIFT_PREMIUM_COST exiles.
 * Sends a special system DM even if blocked.
 */
app.post("/api/premium/gift/:recipientId", requireAuth, async (req, res) => {
  const gifterId    = req.user.userId;
  const recipientId = req.params.recipientId;
  if (gifterId === recipientId) return res.status(400).json({ error: "Cannot gift yourself." });

  const users  = db.collection("users");
  const gifter = await users.findOne({ userId: gifterId });
  const recip  = await users.findOne({ userId: recipientId });

  if (!recip) return res.status(404).json({ error: "Recipient not found." });
  if ((gifter.exiles || 0) < GIFT_PREMIUM_COST)
    return res.status(402).json({ error: `Insufficient exiles. Need ${GIFT_PREMIUM_COST}, have ${gifter.exiles || 0}.` });

  const expiry = addMonths(recip.premiumExpiry && new Date(recip.premiumExpiry) > new Date() ? recip.premiumExpiry : null, 1);

  await users.updateOne({ userId: gifterId },    { $inc: { exiles: -GIFT_PREMIUM_COST, giftSent: 1 } });
  await users.updateOne({ userId: recipientId }, { $set: { premium: true, premiumExpiry: expiry } });

  // ── Special system DM (bypasses blocks) ──
  const conversationId = makeConversationId(gifterId, recipientId);
  const messageId      = uuidv4();
  const now            = new Date().toISOString();
  const giftMsg = {
    messageId, conversationId,
    senderId: gifterId, recipientId,
    type:    "gift",
    content: {
      text: `${gifter.name} gifted you 1 month of premium for ${GIFT_PREMIUM_COST} exiles 🎁 Enjoy!`,
      giftType: "premium",
      months:   1,
      exilesCost: GIFT_PREMIUM_COST,
      gifterName: gifter.name,
      gifterUserId: gifterId,
    },
    replyTo:  null, sentAt: now, editedAt: null, readAt: null,
    deleted:  false, reactions: {}, isSystemMessage: true,
  };

  await db.collection("messages").insertOne(giftMsg);
  await db.collection("conversations").updateOne(
    { conversationId },
    { $set: { conversationId, participants: [gifterId, recipientId], lastSenderId: gifterId, lastMessageId: messageId, lastMessageAt: now, lastMessageType: "gift", lastMessagePreview: "[Gift]" }, $setOnInsert: { createdAt: now } },
    { upsert: true }
  );
  broadcast(recipientId, { type: "newMessage", message: { ...giftMsg, sender: userStub(gifter) } });
  broadcastCP(recipientId, {
    type: "notification", notifType: "gift",
    preview: giftMsg.content.text, sender: userStub(gifter), sentAt: now,
  });

  res.json({ message: "Premium gifted.", premiumExpiry: expiry });
});

/**
 * POST /api/premium/gift-space/:spaceId
 * Gifts premium to a space. Sends special DM to owner.
 */
app.post("/api/premium/gift-space/:spaceId", requireAuth, async (req, res) => {
  const gifterId = req.user.userId;
  const users    = db.collection("users");
  const gifter   = await users.findOne({ userId: gifterId });
  if ((gifter.exiles || 0) < GIFT_SPACE_COST)
    return res.status(402).json({ error: `Need ${GIFT_SPACE_COST} exiles.` });

  const space = await resolveSpace(req.params.spaceId);
  if (!space) return res.status(404).json({ error: "Space not found." });

  await users.updateOne({ userId: gifterId }, { $inc: { exiles: -GIFT_SPACE_COST, giftSent: 1 } });
  const spaceExpiry = addMonths(
    space.premiumExpiry && new Date(space.premiumExpiry) > new Date() ? space.premiumExpiry : null, 1
  );
  await db.collection("spaces").updateOne(
    { spaceId: space.spaceId },
    { $set: { premium: true, premiumExpiry: spaceExpiry } }
  );

  // DM to owner (bypasses blocks)
  const conversationId = makeConversationId(gifterId, space.ownerId);
  const messageId      = uuidv4();
  const now            = new Date().toISOString();
  const msg = {
    messageId, conversationId, senderId: gifterId, recipientId: space.ownerId,
    type: "gift",
    content: {
      text: `${gifter.name} gifted premium to your space "${space.name}" for ${GIFT_SPACE_COST} exiles 🎁`,
      giftType: "space", spaceId: space.spaceId, spaceName: space.name,
      exilesCost: GIFT_SPACE_COST, gifterName: gifter.name, gifterUserId: gifterId,
    },
    replyTo: null, sentAt: now, editedAt: null, readAt: null,
    deleted: false, reactions: {}, isSystemMessage: true,
  };
  await db.collection("messages").insertOne(msg);
  await db.collection("conversations").updateOne(
    { conversationId },
    { $set: { conversationId, participants: [gifterId, space.ownerId], lastSenderId: gifterId, lastMessageId: messageId, lastMessageAt: now, lastMessageType: "gift", lastMessagePreview: "[Gift]" }, $setOnInsert: { createdAt: now } },
    { upsert: true }
  );
  broadcast(space.ownerId, { type: "newMessage", message: { ...msg, sender: userStub(gifter) } });
  broadcastCP(space.ownerId, { type: "notification", notifType: "gift", preview: msg.content.text, sender: userStub(gifter), sentAt: now });

  res.json({ message: "Space premium gifted.", premiumExpiry: spaceExpiry });
});

/**
 * POST /api/premium/gift-feed/:feedId
 * Same as gift-space but for feeds.
 */
app.post("/api/premium/gift-feed/:feedId", requireAuth, async (req, res) => {
  const gifterId = req.user.userId;
  const users    = db.collection("users");
  const gifter   = await users.findOne({ userId: gifterId });
  if ((gifter.exiles || 0) < GIFT_FEED_COST)
    return res.status(402).json({ error: `Need ${GIFT_FEED_COST} exiles.` });

  const feed = await resolveFeed(req.params.feedId);
  if (!feed) return res.status(404).json({ error: "Feed not found." });

  await users.updateOne({ userId: gifterId }, { $inc: { exiles: -GIFT_FEED_COST, giftSent: 1 } });
  const feedExpiry = addMonths(
    feed.premiumExpiry && new Date(feed.premiumExpiry) > new Date() ? feed.premiumExpiry : null, 1
  );
  await db.collection("feeds").updateOne(
    { feedId: feed.feedId },
    { $set: { premium: true, premiumExpiry: feedExpiry } }
  );

  const conversationId = makeConversationId(gifterId, feed.ownerId);
  const messageId      = uuidv4();
  const now            = new Date().toISOString();
  const msg = {
    messageId, conversationId, senderId: gifterId, recipientId: feed.ownerId,
    type: "gift",
    content: {
      text: `${gifter.name} gifted premium to your feed "${feed.name}" for ${GIFT_FEED_COST} exiles 🎁`,
      giftType: "feed", feedId: feed.feedId, feedName: feed.name,
      exilesCost: GIFT_FEED_COST, gifterName: gifter.name, gifterUserId: gifterId,
    },
    replyTo: null, sentAt: now, editedAt: null, readAt: null,
    deleted: false, reactions: {}, isSystemMessage: true,
  };
  await db.collection("messages").insertOne(msg);
  await db.collection("conversations").updateOne(
    { conversationId },
    { $set: { conversationId, participants: [gifterId, feed.ownerId], lastSenderId: gifterId, lastMessageId: messageId, lastMessageAt: now, lastMessageType: "gift", lastMessagePreview: "[Gift]" }, $setOnInsert: { createdAt: now } },
    { upsert: true }
  );
  broadcast(feed.ownerId, { type: "newMessage", message: { ...msg, sender: userStub(gifter) } });
  broadcastCP(feed.ownerId, { type: "notification", notifType: "gift", preview: msg.content.text, sender: userStub(gifter), sentAt: now });

  res.json({ message: "Feed premium gifted.", premiumExpiry: feedExpiry });
});

/**
 * GET /api/premium/status
 * Returns current user's premium status + days left.
 */
app.get("/api/premium/status", requireAuth, async (req, res) => {
  const user = await db.collection("users").findOne(
    { userId: req.user.userId },
    { projection: { premium: 1, premiumExpiry: 1, exiles: 1 } }
  );
  if (!user) return res.status(404).json({ error: "User not found." });

  let daysLeft = 0;
  if (user.premium && user.premiumExpiry) {
    const diff = new Date(user.premiumExpiry) - Date.now();
    daysLeft   = Math.max(0, Math.ceil(diff / 86400000));
    // Auto-expire
    if (daysLeft === 0) {
      await db.collection("users").updateOne({ userId: req.user.userId }, { $set: { premium: false, premiumExpiry: null } });
    }
  }

  res.json({
    premium:      user.premium && daysLeft > 0,
    premiumExpiry: user.premiumExpiry,
    daysLeft,
    exiles:       user.exiles || 0,
    costs:        PREMIUM_COSTS,
    giftCost:     GIFT_PREMIUM_COST,
  });
});

// ════════════════════════════════════════════════════════════════════════════
//  LEADERBOARDS
// ════════════════════════════════════════════════════════════════════════════

/**
 * Compute a user's current rank for a given leaderboard type.
 * Returns the integer rank (1-based).
 */
async function computeRank(userId, type) {
  const user = await db.collection("users").findOne({ userId });
  if (!user) return null;

  if (type === "combined") {
    const score = (user.xp || 0) * 0.5 + (user.reputation || 0) * 300 + (user.streaks || 0) * 20;
    return await db.collection("users").countDocuments({
      banned: { $ne: true },
      $expr: {
        $gt: [
          { $add: [
            { $multiply: [{ $ifNull: ["$xp",         0] }, 0.5] },
            { $multiply: [{ $ifNull: ["$reputation", 0] }, 300] },
            { $multiply: [{ $ifNull: ["$streaks",    0] },  20] },
          ]},
          score,
        ],
      },
    }) + 1;
  }
  const fieldMap = { xp: "xp", reputation: "reputation", streaks: "streaks", veterans: "daysOnline" };
  const field    = fieldMap[type] || "xp";
  return await db.collection("users").countDocuments({
    banned: { $ne: true },
    [field]: { $gt: (user[field] || 0) },
  }) + 1;
}

/**
 * Returns trend info for a user on a given leaderboard type.
 * trend: "up" | "down" | "same" | "new"
 * delta: number of positions moved (positive = improved)
 */
async function getRankTrend(userId, type, currentRank) {
  const snap = await db.collection("rankSnapshots").findOne({ userId, type });
  if (!snap) return { trend: "new", delta: 0, previousRank: null };
  const prev  = snap.rank;
  const delta = prev - currentRank; // positive = moved up
  return {
    trend:        delta > 0 ? "up" : delta < 0 ? "down" : "same",
    delta:        Math.abs(delta),
    previousRank: prev,
  };
}

/**
 * Persist rank snapshot for a user so next call can compute trend.
 * Called after every leaderboard fetch that includes the requester.
 * Fire-and-forget (non-blocking).
 */
function saveRankSnapshot(userId, type, rank) {
  db.collection("rankSnapshots").updateOne(
    { userId, type },
    { $set: { rank, updatedAt: new Date().toISOString() } },
    { upsert: true }
  ).catch(() => {});
}

/**
 * Enrich a leaderboard entry list with trend data loaded from rankSnapshots.
 * Also marks which entry is the requesting user (isSelf: true) and appends
 * a "you" entry at the end if the requester isn't already visible on this page.
 */
async function enrichLeaderboard(entries, requesterId, type) {
  if (!requesterId) return { entries, requester: null };

  // Load snapshots for everyone on this page in one query
  const ids   = entries.map(e => e.userId);
  const snaps = await db.collection("rankSnapshots")
    .find({ userId: { $in: ids }, type })
    .toArray();
  const snapMap = Object.fromEntries(snaps.map(s => [s.userId, s.rank]));

  const enriched = entries.map(e => {
    const prev  = snapMap[e.userId];
    const delta = prev != null ? prev - e.rank : null;
    return {
      ...e,
      isSelf:       e.userId === requesterId,
      previousRank: prev ?? null,
      trend:        delta == null ? "new" : delta > 0 ? "up" : delta < 0 ? "down" : "same",
      trendDelta:   delta == null ? 0 : Math.abs(delta),
    };
  });

  // Save snapshots for everyone on this page (background)
  for (const e of entries) saveRankSnapshot(e.userId, type, e.rank);

  // If requester isn't on this page, fetch their rank separately
  let requester = null;
  if (requesterId && !entries.find(e => e.userId === requesterId)) {
    const currentRank = await computeRank(requesterId, type);
    if (currentRank) {
      const trendInfo = await getRankTrend(requesterId, type, currentRank);
      const u = await db.collection("users").findOne(
        { userId: requesterId },
        { projection: { userId: 1, name: 1, username: 1, tag: 1, profilePicture: 1,
            premium: 1, verified: 1, position: 1, xp: 1, level: 1,
            reputation: 1, streaks: 1, daysOnline: 1 } }
      );
      if (u) {
        requester = {
          rank:         currentRank,
          isSelf:       true,
          ...trendInfo,
          userId:       u.userId,
          name:         u.name,
          username:     u.username,
          tag:          u.tag,
          profilePicture: u.profilePicture,
          premium:      u.premium,
          verified:     u.verified,
          position:     u.position,
          reputation:   u.reputation || 0,
          streaks:      u.streaks    || 0,
          daysOnline:   u.daysOnline || 0,
          ...buildLevelInfo(u.xp || 0),
        };
        saveRankSnapshot(requesterId, type, currentRank);
      }
    }
  } else if (requesterId) {
    requester = enriched.find(e => e.userId === requesterId) || null;
  }

  return { entries: enriched, requester };
}

/**
 * Shared leaderboard builder.
 * sortField: "xp" | "reputation" | "streaks" | "daysOnline"
 */
async function buildLeaderboard(sortField, limit, page) {
  const skip  = (page - 1) * limit;
  const users = await db.collection("users")
    .find({ banned: { $ne: true } })
    .sort({ [sortField]: -1 })
    .skip(skip)
    .limit(limit)
    .project({
      userId: 1, name: 1, username: 1, tag: 1,
      profilePicture: 1, premium: 1, verified: 1,
      position: 1, xp: 1, level: 1, reputation: 1,
      streaks: 1, daysOnline: 1,
    })
    .toArray();

  return users.map((u, i) => ({
    rank:           skip + i + 1,
    userId:         u.userId,
    name:           u.name,
    username:       u.username,
    tag:            u.tag,
    profilePicture: u.profilePicture,
    premium:        u.premium,
    verified:       u.verified,
    position:       u.position,
    reputation:     u.reputation || 0,
    streaks:        u.streaks    || 0,
    daysOnline:     u.daysOnline || 0,
    ...buildLevelInfo(u.xp || 0),
  }));
}

/** Parse optional auth cookie/header without hard-failing */
function tryGetRequesterId(req) {
  try {
    const token = req.cookies?.token || req.headers["authorization"]?.replace("Bearer ", "");
    if (!token) return null;
    return jwt.verify(token, JWT_SECRET).userId;
  } catch { return null; }
}

/**
 * GET /api/leaderboard/xp?limit=20&page=1
 * Top users by XP / level.
 * If authenticated: includes isSelf, trend, trendDelta, previousRank on each entry
 * + a top-level `requester` object showing your position even if off-page.
 */
app.get("/api/leaderboard/xp", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 20, 100);
  const page  = Math.max(parseInt(req.query.page)  || 1,  1);
  const raw   = await buildLeaderboard("xp", limit, page);
  const requesterId = tryGetRequesterId(req);
  const { entries, requester } = await enrichLeaderboard(raw, requesterId, "xp");
  res.json({ leaderboard: entries, requester, page, limit });
});

/**
 * GET /api/leaderboard/reputation?limit=20&page=1
 */
app.get("/api/leaderboard/reputation", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 20, 100);
  const page  = Math.max(parseInt(req.query.page)  || 1,  1);
  const raw   = await buildLeaderboard("reputation", limit, page);
  const requesterId = tryGetRequesterId(req);
  const { entries, requester } = await enrichLeaderboard(raw, requesterId, "reputation");
  res.json({ leaderboard: entries, requester, page, limit });
});

/**
 * GET /api/leaderboard/streaks?limit=20&page=1
 */
app.get("/api/leaderboard/streaks", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 20, 100);
  const page  = Math.max(parseInt(req.query.page)  || 1,  1);
  const raw   = await buildLeaderboard("streaks", limit, page);
  const requesterId = tryGetRequesterId(req);
  const { entries, requester } = await enrichLeaderboard(raw, requesterId, "streaks");
  res.json({ leaderboard: entries, requester, page, limit });
});

/**
 * GET /api/leaderboard/veterans?limit=20&page=1
 */
app.get("/api/leaderboard/veterans", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 20, 100);
  const page  = Math.max(parseInt(req.query.page)  || 1,  1);
  const raw   = await buildLeaderboard("daysOnline", limit, page);
  const requesterId = tryGetRequesterId(req);
  const { entries, requester } = await enrichLeaderboard(raw, requesterId, "veterans");
  res.json({ leaderboard: entries, requester, page, limit });
});

/**
 * GET /api/leaderboard/combined?limit=20&page=1
 * Composite score: xp * 0.5 + reputation * 300 + streaks * 20
 */
app.get("/api/leaderboard/combined", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 20, 100);
  const page  = Math.max(parseInt(req.query.page)  || 1,  1);
  const skip  = (page - 1) * limit;

  const results = await db.collection("users").aggregate([
    { $match: { banned: { $ne: true } } },
    { $addFields: {
      score: {
        $add: [
          { $multiply: [{ $ifNull: ["$xp",         0] }, 0.5] },
          { $multiply: [{ $ifNull: ["$reputation", 0] }, 300] },
          { $multiply: [{ $ifNull: ["$streaks",    0] },  20] },
        ],
      },
    }},
    { $sort: { score: -1 } },
    { $skip: skip },
    { $limit: limit },
    { $project: {
      userId: 1, name: 1, username: 1, tag: 1,
      profilePicture: 1, premium: 1, verified: 1, position: 1,
      xp: 1, level: 1, reputation: 1, streaks: 1, daysOnline: 1, score: 1,
    }},
  ]).toArray();

  const raw = results.map((u, i) => ({
    rank:           skip + i + 1,
    userId:         u.userId,
    name:           u.name,
    username:       u.username,
    tag:            u.tag,
    profilePicture: u.profilePicture,
    premium:        u.premium,
    verified:       u.verified,
    position:       u.position,
    reputation:     u.reputation || 0,
    streaks:        u.streaks    || 0,
    daysOnline:     u.daysOnline || 0,
    score:          Math.round(u.score),
    ...buildLevelInfo(u.xp || 0),
  }));

  const requesterId = tryGetRequesterId(req);
  const { entries, requester } = await enrichLeaderboard(raw, requesterId, "combined");

  res.json({
    leaderboard: entries,
    requester,
    page, limit,
    scoreFormula: "xp × 0.5 + reputation × 300 + streaks × 20",
  });
});

/**
 * GET /api/leaderboard/me?type=xp|reputation|streaks|veterans|combined
 * Returns the authenticated user's rank + trend on the requested leaderboard.
 */
app.get("/api/leaderboard/me", requireAuth, async (req, res) => {
  const type   = req.query.type || "xp";
  const userId = req.user.userId;
  const user   = await db.collection("users").findOne({ userId });
  if (!user) return res.status(404).json({ error: "User not found." });

  const rank      = await computeRank(userId, type);
  const trendInfo = await getRankTrend(userId, type, rank);

  // Save snapshot for next call
  saveRankSnapshot(userId, type, rank);

  res.json({
    rank,
    ...trendInfo,
    userId:     user.userId,
    name:       user.name,
    username:   user.username,
    ...buildLevelInfo(user.xp || 0),
    reputation: user.reputation || 0,
    streaks:    user.streaks    || 0,
    daysOnline: user.daysOnline || 0,
    leaderboardType: type,
  });
});

// ════════════════════════════════════════════════════════════════════════════
//  EXILE TRANSFER (Send Exiles)
// ════════════════════════════════════════════════════════════════════════════

const EXILE_TRANSFER_MIN = 1;    // minimum exiles per transfer
const EXILE_TRANSFER_MAX = 1000; // maximum per single transfer

/**
 * Core exile transfer logic — shared by user HTTP route, bot HTTP route, and bot WS.
 *
 * @param {string}  fromUserId   - userId of sender (or bot owner when bot sends)
 * @param {string}  toUserId     - userId of recipient
 * @param {number}  amount       - exiles to send (positive integer)
 * @param {string}  [note]       - optional personal note (max 200 chars)
 * @param {object}  [senderHint] - pre-fetched sender doc (saves a DB round-trip)
 * @param {boolean} [bypassBlock=false] - skip block check (set true for bot-owner transfers)
 * @returns {{ ok: boolean, error?: string, status?: number, newBalance?: number }}
 */
async function transferExiles({ fromUserId, toUserId, amount, note, senderHint, bypassBlock = false, senderIsBot = false, botName = null }) {
  if (fromUserId === toUserId)
    return { ok: false, error: "Cannot send exiles to yourself.", status: 400 };

  amount = Math.floor(Number(amount));
  if (!Number.isFinite(amount) || amount < EXILE_TRANSFER_MIN)
    return { ok: false, error: `Minimum transfer is ${EXILE_TRANSFER_MIN} exile.`, status: 400 };
  if (amount > EXILE_TRANSFER_MAX)
    return { ok: false, error: `Maximum transfer is ${EXILE_TRANSFER_MAX} exiles.`, status: 400 };

  const users = db.collection("users");

  const sender    = senderHint || await users.findOne({ userId: fromUserId });
  const recipient = await users.findOne({ userId: toUserId });

  if (!sender)    return { ok: false, error: "Sender not found.",    status: 404 };
  if (!recipient) return { ok: false, error: "Recipient not found.", status: 404 };
  if (recipient.banned) return { ok: false, error: "Recipient is banned.", status: 403 };

  // Block check (unless bypassed)
  if (!bypassBlock) {
    const block = await db.collection("blocks").findOne({
      $or: [
        { blockerId: toUserId,   blockedId: fromUserId },
        { blockerId: fromUserId, blockedId: toUserId   },
      ],
    });
    if (block) return { ok: false, error: "Cannot send exiles — user is blocked.", status: 403 };
  }

  if ((sender.exiles || 0) < amount)
    return { ok: false, error: `Insufficient exiles. You have ${sender.exiles || 0}, need ${amount}.`, status: 402 };

  // Atomic debit sender / credit recipient
  await users.updateOne({ userId: fromUserId }, { $inc: { exiles: -amount } });
  await users.updateOne({ userId: toUserId   }, { $inc: { exiles:  amount } });

  const newBalance = (sender.exiles || 0) - amount;

  // ── Special system DM (bypasses blocks — it's a credit notification) ──
  const conversationId = makeConversationId(fromUserId, toUserId);
  const messageId      = uuidv4();
  const now            = new Date().toISOString();
  const noteText       = note?.trim().slice(0, 200) || null;
  const senderLabel    = senderIsBot && botName ? `${sender.name} (via ${botName})` : sender.name;

  const giftMsg = {
    messageId,
    conversationId,
    senderId:    fromUserId,
    recipientId: toUserId,
    type:        "exileTransfer",
    content: {
      text:         `${senderLabel} sent you ${amount} exile${amount !== 1 ? "s" : ""} 💰${noteText ? ` "${noteText}"` : ""}`,
      giftType:     "exiles",
      amount,
      note:         noteText,
      gifterName:   sender.name,
      gifterUserId: fromUserId,
      senderIsBot:  senderIsBot,
      botName:      botName || null,
    },
    replyTo:          null,
    sentAt:           now,
    editedAt:         null,
    readAt:           null,
    deleted:          false,
    reactions:        {},
    isSystemMessage:  true,
  };

  await db.collection("messages").insertOne(giftMsg);
  await db.collection("conversations").updateOne(
    { conversationId },
    {
      $set: {
        conversationId,
        participants:        [fromUserId, toUserId],
        lastMessageId:       messageId,
        lastMessageAt:       now,
        lastSenderId:        fromUserId,
        lastMessageType:     "exileTransfer",
        lastMessagePreview:  `[${amount} exiles]`,
      },
      $setOnInsert: { createdAt: now },
    },
    { upsert: true }
  );

  // Push to recipient's sockets + CP
  broadcast(toUserId, {
    type:    "newMessage",
    fromMe:  false,
    chat:    { chatId: conversationId, chatType: "dm" },
    from:    userStub(sender),
    message: { ...giftMsg, sender: userStub(sender) },
  });
  broadcastCP(toUserId, {
    type:      "notification",
    notifType: "exileTransfer",
    preview:   giftMsg.content.text,
    sender:    userStub(sender),
    sentAt:    now,
    amount,
  });
  broadcastCP(toUserId, { type: "exilesUpdate", delta: +amount, reason: "received" });

  // Echo to sender + balance update
  broadcast(fromUserId, {
    type:    "messageSent",
    fromMe:  true,
    chat:    { chatId: conversationId, chatType: "dm" },
    message: giftMsg,
  });
  broadcastCP(fromUserId, { type: "exilesUpdate", delta: -amount, reason: "sent" });

  return { ok: true, newBalance, sentAt: now, messageId };
}

/**
 * POST /api/exiles/send
 * Authenticated user sends exiles to another user.
 * body: { to: userId, amount: number, note?: string }
 */
app.post("/api/exiles/send", requireAuth, async (req, res) => {
  const fromUserId = req.user.userId;
  const { to, amount, note } = req.body;

  if (!to) return res.status(400).json({ error: "to (userId) is required." });

  const result = await transferExiles({ fromUserId, toUserId: to, amount, note });
  if (!result.ok) return res.status(result.status || 400).json({ error: result.error });

  res.json({
    message:    `${amount} exile${amount !== 1 ? "s" : ""} sent successfully.`,
    newBalance: result.newBalance,
    sentAt:     result.sentAt,
    messageId:  result.messageId,
  });
});

/**
 * GET /api/exiles/balance
 * Returns authenticated user's exile balance.
 */
app.get("/api/exiles/balance", requireAuth, async (req, res) => {
  const user = await db.collection("users").findOne(
    { userId: req.user.userId },
    { projection: { exiles: 1 } }
  );
  if (!user) return res.status(404).json({ error: "User not found." });
  res.json({ exiles: user.exiles || 0 });
});

/**
 * GET /api/exiles/history?limit=20&page=1
 * Exile transfer history for authenticated user (sent + received).
 */
app.get("/api/exiles/history", requireAuth, async (req, res) => {
  const userId = req.user.userId;
  const limit  = Math.min(parseInt(req.query.limit) || 20, 100);
  const page   = Math.max(parseInt(req.query.page)  || 1,  1);
  const skip   = (page - 1) * limit;

  const msgs = await db.collection("messages").find({
    $or: [{ senderId: userId }, { recipientId: userId }],
    type:    "exileTransfer",
    deleted: { $ne: true },
  })
  .sort({ sentAt: -1 })
  .skip(skip)
  .limit(limit)
  .toArray();

  const ids   = [...new Set(msgs.map(m => m.senderId === userId ? m.recipientId : m.senderId))];
  const udocs = await db.collection("users").find({ userId: { $in: ids } }).toArray();
  const umap  = Object.fromEntries(udocs.map(u => [u.userId, u]));

  res.json({
    history: msgs.map(m => {
      const isSender = m.senderId === userId;
      const otherId  = isSender ? m.recipientId : m.senderId;
      return {
        messageId:   m.messageId,
        direction:   isSender ? "sent" : "received",
        amount:      m.content.amount,
        note:        m.content.note || null,
        counterpart: umap[otherId] ? userStub(umap[otherId]) : { userId: otherId },
        sentAt:      m.sentAt,
      };
    }),
    page, limit,
  });
});

/**
 * GET /api/bot/groups/:groupType/:groupId/is-member/:userId
 * Bot checks whether a given userId is an active member of a space or feed.
 * Authorization: Bot <token>
 * groupType: space | feed
 * Returns: { isMember: true/false, isAdmin, isOwner, isBanned, isMuted, joinedAt }
 */
app.get("/api/bot/groups/:groupType/:groupId/is-member/:userId", requireBotAuth, async (req, res) => {
  const { groupType, groupId, userId } = req.params;
  const col     = groupType === "space" ? "spaces" : "feeds";
  const idField = groupType === "space" ? "spaceId" : "feedId";

  const group = await db.collection(col).findOne({ [idField]: groupId });
  if (!group) return res.status(404).json({ error: "Group not found." });

  const member = getMember(group, userId);

  if (!member) return res.json({ isMember: false, isAdmin: false, isOwner: false, isBanned: false, isMuted: false, joinedAt: null });

  res.json({
    isMember: !member.isBanned,
    isAdmin:  member.isAdmin  || group.ownerId === userId,
    isOwner:  group.ownerId === userId,
    isBanned: member.isBanned || false,
    isMuted:  member.isMuted  || false,
    joinedAt: member.joinedAt || null,
  });
});


/**
 * Bot sends exiles on behalf of its owner.
 * Authorization: Bot <token>
 * body: { to: userId, amount: number, note?: string }
 *
 * Rules:
 *  - Deducts from the bot OWNER's balance (not a bot-specific wallet)
 *  - Bypasses block checks (owner is the economic actor, not the bot)
 *  - Subject to same min/max limits
 */
app.post("/api/bot/exiles/send", requireBotAuth, async (req, res) => {
  const bot        = req.bot;
  const { to, amount, note } = req.body;

  if (!to) return res.status(400).json({ error: "to (userId) is required." });

  // Fetch owner as sender
  const owner = await db.collection("users").findOne({ userId: bot.ownerId });
  if (!owner) return res.status(404).json({ error: "Bot owner not found." });

  const result = await transferExiles({
    fromUserId:  bot.ownerId,
    toUserId:    to,
    amount,
    note,
    senderHint:  owner,
    bypassBlock: true,    // bot acts on owner's behalf — owner can always transfer
    senderIsBot: true,
    botName:     bot.name,
  });

  if (!result.ok) return res.status(result.status || 400).json({ error: result.error });

  res.json({
    message:    `${amount} exile${amount !== 1 ? "s" : ""} sent from bot owner.`,
    ownerBalance: result.newBalance,
    sentAt:     result.sentAt,
    messageId:  result.messageId,
  });
});

// ════════════════════════════════════════════════════════════════════════════
//  SPACE / FEED LEADERBOARDS
// ════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/leaderboard/spaces?sort=members|xp&limit=20&page=1
 * Leaderboard of spaces sorted by memberCount or XP.
 */
app.get("/api/leaderboard/spaces", async (req, res) => {
  const sort  = req.query.sort === "xp" ? "xp" : "memberCount";
  const limit = Math.min(parseInt(req.query.limit) || 20, 100);
  const page  = Math.max(parseInt(req.query.page)  || 1,  1);
  const skip  = (page - 1) * limit;

  const spaces = await db.collection("spaces").find(
    { banned: { $ne: true }, isPrivate: { $ne: true } },
    { projection: {
      spaceId: 1, name: 1, bio: 1, profileImage: 1,
      memberCount: 1, xp: 1, premium: 1, premiumExpiry: 1,
      ownerId: 1, createdAt: 1,
    }}
  )
  .sort({ [sort]: -1 })
  .skip(skip)
  .limit(limit)
  .toArray();

  res.json({
    leaderboard: spaces.map((s, i) => ({
      rank:         skip + i + 1,
      spaceId:      s.spaceId,
      name:         s.name,
      bio:          s.bio,
      profileImage: s.profileImage,
      memberCount:  s.memberCount || 0,
      xp:           s.xp         || 0,
      premium:      s.premium     || false,
      premiumExpiry: s.premiumExpiry || null,
      ownerId:      s.ownerId,
      createdAt:    s.createdAt,
    })),
    sort, page, limit,
  });
});

/**
 * GET /api/leaderboard/feeds?sort=members|xp&limit=20&page=1
 * Same for feeds.
 */
app.get("/api/leaderboard/feeds", async (req, res) => {
  const sort  = req.query.sort === "xp" ? "xp" : "memberCount";
  const limit = Math.min(parseInt(req.query.limit) || 20, 100);
  const page  = Math.max(parseInt(req.query.page)  || 1,  1);
  const skip  = (page - 1) * limit;

  const feeds = await db.collection("feeds").find(
    { banned: { $ne: true }, isPrivate: { $ne: true } },
    { projection: {
      feedId: 1, name: 1, bio: 1, profileImage: 1,
      memberCount: 1, xp: 1, premium: 1, premiumExpiry: 1,
      ownerId: 1, createdAt: 1,
    }}
  )
  .sort({ [sort]: -1 })
  .skip(skip)
  .limit(limit)
  .toArray();

  res.json({
    leaderboard: feeds.map((f, i) => ({
      rank:         skip + i + 1,
      feedId:       f.feedId,
      name:         f.name,
      bio:          f.bio,
      profileImage: f.profileImage,
      memberCount:  f.memberCount || 0,
      xp:           f.xp         || 0,
      premium:      f.premium     || false,
      premiumExpiry: f.premiumExpiry || null,
      ownerId:      f.ownerId,
      createdAt:    f.createdAt,
    })),
    sort, page, limit,
  });
});

// ════════════════════════════════════════════════════════════════════════════
//  EXPLORE
// ════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/explore
 * Returns public spaces, feeds, and users sorted by various criteria.
 *
 * Query params:
 *   type      = all | spaces | feeds | users   (default: all)
 *   sort      = xp | members | recent | combined  (default: xp for groups, xp for users)
 *   limit     = 1–50  (default 20)
 *   page      = 1+    (default 1)
 *   q         = optional search query (name / username / bio)
 */
app.get("/api/explore", async (req, res) => {
  const type  = req.query.type  || "all";
  const sort  = req.query.sort  || "xp";
  const limit = Math.min(parseInt(req.query.limit) || 20, 50);
  const page  = Math.max(parseInt(req.query.page)  || 1,  1);
  const skip  = (page - 1) * limit;
  const q     = req.query.q?.trim();

  const result = {};

  // ── Spaces ──
  if (type === "all" || type === "spaces") {
    const spaceFilter = { banned: { $ne: true }, isPrivate: { $ne: true } };
    if (q) {
      const rx = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
      spaceFilter.$or = [{ name: { $regex: rx } }, { bio: { $regex: rx } }];
    }
    const sortField = sort === "members" ? "memberCount" : "xp";
    const spaces = await db.collection("spaces")
      .find(spaceFilter, { projection: {
        spaceId: 1, name: 1, bio: 1, profileImage: 1,
        memberCount: 1, xp: 1, premium: 1, premiumExpiry: 1,
        joinLink: 1, ownerId: 1, createdAt: 1,
      }})
      .sort({ [sortField]: -1 })
      .skip(skip)
      .limit(limit)
      .toArray();
    result.spaces = spaces.map(s => ({
      spaceId:      s.spaceId,
      name:         s.name,
      bio:          s.bio,
      profileImage: s.profileImage,
      memberCount:  s.memberCount || 0,
      xp:           s.xp || 0,
      premium:      s.premium || false,
      premiumExpiry: s.premiumExpiry || null,
      joinLink:     s.joinLink,
      ownerId:      s.ownerId,
      createdAt:    s.createdAt,
    }));
  }

  // ── Feeds ──
  if (type === "all" || type === "feeds") {
    const feedFilter = { banned: { $ne: true }, isPrivate: { $ne: true } };
    if (q) {
      const rx = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
      feedFilter.$or = [{ name: { $regex: rx } }, { bio: { $regex: rx } }];
    }
    const sortField = sort === "members" ? "memberCount" : "xp";
    const feeds = await db.collection("feeds")
      .find(feedFilter, { projection: {
        feedId: 1, name: 1, bio: 1, profileImage: 1,
        memberCount: 1, xp: 1, premium: 1, premiumExpiry: 1,
        joinLink: 1, ownerId: 1, createdAt: 1,
      }})
      .sort({ [sortField]: -1 })
      .skip(skip)
      .limit(limit)
      .toArray();
    result.feeds = feeds.map(f => ({
      feedId:       f.feedId,
      name:         f.name,
      bio:          f.bio,
      profileImage: f.profileImage,
      memberCount:  f.memberCount || 0,
      xp:           f.xp || 0,
      premium:      f.premium || false,
      premiumExpiry: f.premiumExpiry || null,
      joinLink:     f.joinLink,
      ownerId:      f.ownerId,
      createdAt:    f.createdAt,
    }));
  }

  // ── Users (contacts / people to discover) ──
  if (type === "all" || type === "users") {
    const userFilter = { banned: { $ne: true } };
    if (q) {
      const rx = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
      userFilter.$or = [
        { name: { $regex: rx } },
        { usernameLower: { $regex: q.toLowerCase() } },
      ];
    }

    // Sort field
    const userSortMap = {
      xp:         "xp",
      reputation: "reputation",
      streaks:    "streaks",
      members:    "contacts",
      combined:   "xp",
      recent:     "dateJoined",
    };
    const userSortField = userSortMap[sort] || "xp";

    const users = await db.collection("users")
      .find(userFilter, { projection: {
        userId: 1, name: 1, username: 1, tag: 1,
        profilePicture: 1, premium: 1, verified: 1,
        position: 1, xp: 1, level: 1, reputation: 1,
        streaks: 1, daysOnline: 1, contacts: 1, dateJoined: 1,
      }})
      .sort({ [userSortField]: -1 })
      .skip(skip)
      .limit(limit)
      .toArray();

    result.users = users.map(u => ({
      userId:         u.userId,
      name:           u.name,
      username:       u.username,
      tag:            u.tag,
      profilePicture: u.profilePicture,
      premium:        u.premium,
      verified:       u.verified,
      position:       u.position,
      contacts:       u.contacts || 0,
      reputation:     u.reputation || 0,
      streaks:        u.streaks    || 0,
      ...buildLevelInfo(u.xp || 0),
    }));
  }

  res.json({ ...result, sort, page, limit });
});



// ════════════════════════════════════════════════════════════════════════════
//  STICKERS
// ════════════════════════════════════════════════════════════════════════════

const STICKER_MAX_BYTES  = 512 * 1024;   // 512 KB
const STICKER_MAX_COUNT  = 20;
const STICKER_DIMENSION  = 512;          // must be exactly 512×512

const stickerUpload = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: STICKER_MAX_BYTES },
  fileFilter: (_req, file, cb) => {
    if (!file.mimetype.startsWith("image/"))
      return cb(new Error("Only image files are accepted for stickers."));
    cb(null, true);
  },
});

/**
 * Helper – fetch a remote URL into a buffer and return { buffer, contentType, contentLength }.
 * Rejects if the response is > 512 KB.
 */
async function fetchRemoteSticker(url) {
  const response = await axios.get(url, {
    responseType: "arraybuffer",
    maxContentLength: STICKER_MAX_BYTES,
    timeout: 15000,
  });
  const contentType = response.headers["content-type"] || "";
  if (!contentType.startsWith("image/"))
    throw new Error("URL does not point to an image.");
  const buffer = Buffer.from(response.data);
  if (buffer.byteLength > STICKER_MAX_BYTES)
    throw new Error(`Sticker exceeds 512 KB (got ${(buffer.byteLength / 1024).toFixed(1)} KB).`);
  return { buffer, contentType };
}

/**
 * Helper – validate image dimensions using sharp (if available) or skip silently.
 * Returns true when dimensions are exactly 512×512, false otherwise.
 */
async function validateStickerDimensions(buffer) {
  try {
    const sharp = require("sharp");
    const meta  = await sharp(buffer).metadata();
    return meta.width === STICKER_DIMENSION && meta.height === STICKER_DIMENSION;
  } catch {
    // sharp not installed – skip dimension check
    return true;
  }
}

// ─── POST /api/stickers/create ───────────────────────────────────────────────
/**
 * Upload a new sticker image (multipart file upload).
 * The image is uploaded to Catbox/touch.io and its URL is stored under your userId.
 *
 * Body (multipart/form-data):
 *   file  – the sticker image (≤512 KB, 512×512 px)
 *
 * Returns: { url }
 */
app.post("/api/stickers/create", requireAuth, stickerUpload.single("file"), async (req, res) => {
  try {
    if (!req.file)
      return res.status(400).json({ error: "No file uploaded." });

    const buffer = req.file.buffer;

    // Size guard (multer already rejects > 512 KB but double-check)
    if (buffer.byteLength > STICKER_MAX_BYTES)
      return res.status(400).json({ error: "Sticker exceeds 512 KB." });

    // Dimension check
    const dimsOk = await validateStickerDimensions(buffer);
    if (!dimsOk)
      return res.status(400).json({ error: "Sticker must be exactly 512×512 pixels." });

    // Check current count
    const col    = db.collection("stickers");
    const record = await col.findOne({ userId: req.user.userId });
    if (record && (record.urls || []).length >= STICKER_MAX_COUNT)
      return res.status(400).json({ error: `Sticker limit reached (max ${STICKER_MAX_COUNT}).` });

    // Upload
    const ext      = req.file.originalname.split(".").pop() || "png";
    const filename = `sticker_${req.user.userId}_${Date.now()}.${ext}`;
    const url      = await uploadMedia(buffer, filename);

    // Persist
    await col.updateOne(
      { userId: req.user.userId },
      { $push: { urls: url }, $setOnInsert: { userId: req.user.userId, createdAt: new Date() } },
      { upsert: true },
    );

    return res.status(201).json({ url });
  } catch (err) {
    console.error("stickers/create error:", err);
    return res.status(500).json({ error: err.message || "Failed to create sticker." });
  }
});

// ─── POST /api/stickers/add ───────────────────────────────────────────────────
/**
 * Add an existing sticker URL to your collection.
 * The URL must already be saved by at least one other user, and you must not already own it.
 *
 * Body (JSON): { url }
 */
app.post("/api/stickers/add", requireAuth, async (req, res) => {
  try {
    const { url } = req.body;
    if (!url || typeof url !== "string")
      return res.status(400).json({ error: "url is required." });

    const col = db.collection("stickers");

    // Must be owned by at least one user
    const ownerCount = await col.countDocuments({ urls: url });
    if (ownerCount === 0)
      return res.status(400).json({ error: "This sticker URL is not owned by any user." });

    // Must not already own it
    const alreadyOwned = await col.findOne({ userId: req.user.userId, urls: url });
    if (alreadyOwned)
      return res.status(409).json({ error: "You already have this sticker." });

    // Limit check
    const record = await col.findOne({ userId: req.user.userId });
    if (record && (record.urls || []).length >= STICKER_MAX_COUNT)
      return res.status(400).json({ error: `Sticker limit reached (max ${STICKER_MAX_COUNT}).` });

    await col.updateOne(
      { userId: req.user.userId },
      { $push: { urls: url }, $setOnInsert: { userId: req.user.userId, createdAt: new Date() } },
      { upsert: true },
    );

    return res.json({ message: "Sticker added.", url });
  } catch (err) {
    console.error("stickers/add error:", err);
    return res.status(500).json({ error: err.message || "Failed to add sticker." });
  }
});

// ─── DELETE /api/stickers/remove ─────────────────────────────────────────────
/**
 * Remove a single sticker URL from your collection.
 *
 * Body (JSON): { url }
 */
app.delete("/api/stickers/remove", requireAuth, async (req, res) => {
  try {
    const { url } = req.body;
    if (!url || typeof url !== "string")
      return res.status(400).json({ error: "url is required." });

    const col    = db.collection("stickers");
    const result = await col.updateOne(
      { userId: req.user.userId },
      { $pull: { urls: url } },
    );

    if (result.matchedCount === 0)
      return res.status(404).json({ error: "No sticker collection found for your account." });

    return res.json({ message: "Sticker removed.", url });
  } catch (err) {
    console.error("stickers/remove error:", err);
    return res.status(500).json({ error: err.message || "Failed to remove sticker." });
  }
});

// ─── GET /api/stickers ────────────────────────────────────────────────────────
/**
 * Return all sticker URLs in your collection.
 *
 * Response: { urls: string[], count: number }
 */
app.get("/api/stickers", requireAuth, async (req, res) => {
  try {
    const col    = db.collection("stickers");
    const record = await col.findOne({ userId: req.user.userId });
    const urls   = record?.urls || [];
    return res.json({ urls, count: urls.length });
  } catch (err) {
    console.error("stickers GET error:", err);
    return res.status(500).json({ error: err.message || "Failed to fetch stickers." });
  }
});

// ─── DELETE /api/stickers/all ─────────────────────────────────────────────────
/**
 * Delete your entire sticker collection.
 *
 * Response: { message, deleted: number }
 */
app.delete("/api/stickers/all", requireAuth, async (req, res) => {
  try {
    const col    = db.collection("stickers");
    const record = await col.findOne({ userId: req.user.userId });
    const count  = record?.urls?.length || 0;

    await col.deleteOne({ userId: req.user.userId });

    return res.json({ message: "All stickers deleted.", deleted: count });
  } catch (err) {
    console.error("stickers/all DELETE error:", err);
    return res.status(500).json({ error: err.message || "Failed to delete stickers." });
  }
});

// ════════════════════════════════════════════════════════════════════════════

connectDB()
  .then(() => {
    server.listen(PORT, () => console.log(`✓ Exile server running on port ${PORT}`));
  })
  .catch(err => {
    console.error("Failed to connect to MongoDB:", err);
    process.exit(1);
  });

module.exports = { app, server };

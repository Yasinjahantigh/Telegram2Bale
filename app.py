#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Public, multi-tenant Telegram ↔ Bale bridge bot.

Key features
------------
- Users link their own Telegram/Bale groups and channels with a /link_* code flow
- Each user can register multiple groups/channels and pair them independently
- Full isolation: messages only flow within a user's own pairs
- Two-way bridging for DMs (optional), groups, and channels
- Text + photo + document + video supported
- SQLite persistence via aiosqlite
- Logging: console + rotating file

Requirements
------------
Python 3.10+
pip install:
  aiogram==3.*
  Balethon
  aiosqlite
  pyyaml
  python-dotenv

Run
---
1) Fill .env (tokens) and run:
     python bridge_public.py
2) Talk to the bot in Telegram or Bale DMs. Use /help for commands.

Security
--------
- Verification codes are single-use, 10 min TTL, bound to (platform, user_id).
- Loops are prevented: we ignore messages sent by the bot itself.
"""

from __future__ import annotations
import asyncio
import logging
import os
import random
import string
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Optional, Tuple
import uuid

import aiosqlite
from dotenv import load_dotenv

# Telegram
from aiogram import Bot as TgBot, Dispatcher, Router, F, types
from aiogram.types import BufferedInputFile
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardMarkup, CallbackQuery


# Bale
from balethon import Client as BaleClient
from balethon.objects import InlineKeyboard  # ← NEW (inline buttons)

# -----------------
# Environment / cfg
# -----------------

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
BALE_TOKEN = os.getenv("BALE_TOKEN", "")

# (Optional) If you're the operator and want a copy of every DM to the bot:
def getenv_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "on"}

def getenv_int_opt(name: str) -> Optional[int]:
    v = os.getenv(name)
    if v is None:
        return None
    v = v.strip()
    if v == "":
        return None
    try:
        return int(v)
    except ValueError:
        raise ValueError(f"{name} must be an integer or empty, got {v!r}")

def getenv_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return float(v)
    except ValueError:
        raise ValueError(f"{name} must be a float, got {v!r}")

# (Optional) If you're the operator and want a copy of every DM to the bot:
MIRROR_DMS_TO_OWNER = getenv_bool("MIRROR_DMS_TO_OWNER", False)
OWNER_TG_CHAT_ID = getenv_int_opt("OWNER_TG_CHAT_ID")
OWNER_BALE_CHAT_ID = getenv_int_opt("OWNER_BALE_CHAT_ID")

DB_PATH = os.getenv("DB_PATH", "bridge_public.db")
BALE_POLL_INTERVAL = getenv_float("BALE_POLL_INTERVAL", 1.0)

if not TELEGRAM_TOKEN or not BALE_TOKEN:
    print("Please set TELEGRAM_TOKEN and BALE_TOKEN in .env")
    sys.exit(1)
BALE_WIZ: dict[int, dict] = {}
TG_WIZ: dict[int, dict] = {}


# -------------
# Logging setup
# -------------
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = RotatingFileHandler("bridge_public.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

setup_logging()

# -----
# DB IO
# -----

INIT_SQL = """
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tg_user_id INTEGER,
  bale_user_id INTEGER,
  dm_target_bale_chat_id INTEGER,
  dm_target_telegram_chat_id INTEGER,
  created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_tg ON users(tg_user_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_bale ON users(bale_user_id);

CREATE TABLE IF NOT EXISTS chats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_user_id INTEGER NOT NULL,
  platform TEXT NOT NULL CHECK (platform IN ('tg','bale')),
  chat_type TEXT NOT NULL CHECK (chat_type IN ('group','channel')),
  chat_id INTEGER NOT NULL,            -- platform-native chat id
  title TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(platform, chat_id),
  FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_chats_owner ON chats(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_chats_platform_chatid ON chats(platform, chat_id);

CREATE TABLE IF NOT EXISTS group_links (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_user_id INTEGER NOT NULL,
  tg_group_id INTEGER NOT NULL,
  bale_group_id INTEGER NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  UNIQUE(tg_group_id, bale_group_id),
  FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_group_links_owner ON group_links(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_group_links_tg ON group_links(tg_group_id);
CREATE INDEX IF NOT EXISTS idx_group_links_bale ON group_links(bale_group_id);

CREATE TABLE IF NOT EXISTS channel_links (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_user_id INTEGER NOT NULL,
  tg_channel_id INTEGER NOT NULL,
  bale_channel_id INTEGER NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  UNIQUE(tg_channel_id, bale_channel_id),
  FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_channel_links_owner ON channel_links(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_channel_links_tg ON channel_links(tg_channel_id);
CREATE INDEX IF NOT EXISTS idx_channel_links_bale ON channel_links(bale_channel_id);

CREATE TABLE IF NOT EXISTS verify_tokens (
  code TEXT PRIMARY KEY,
  owner_user_id INTEGER NOT NULL,
  platform TEXT NOT NULL CHECK (platform IN ('tg','bale')),
  chat_type TEXT NOT NULL CHECK (chat_type IN ('group','channel')),
  platform_user_id INTEGER NOT NULL,
  expires_at TEXT NOT NULL,
  consumed INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- --- DM verify tokens table ---
CREATE TABLE IF NOT EXISTS dm_verify_tokens (
  code TEXT PRIMARY KEY,
  owner_user_id INTEGER NOT NULL,
  target_platform TEXT NOT NULL CHECK (target_platform IN ('tg','bale')),
  target_chat_id INTEGER NOT NULL,
  expires_at TEXT NOT NULL,
  consumed INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE
);
"""

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(INIT_SQL)
        await db.commit()

async def get_user_row_by_id(db: aiosqlite.Connection, owner_user_id: int):
    cur = await db.execute(
        "SELECT id, tg_user_id, bale_user_id, dm_target_bale_chat_id, dm_target_telegram_chat_id FROM users WHERE id=?",
        (owner_user_id,)
    )
    row = await cur.fetchone()
    await cur.close()
    return row

async def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ---------------
# Utility helpers
# ---------------
BALE_HELP_TEXT = (
    "👋 <b>Public Bridge Bot</b>\n\n"
    "این منو برای مدیریت بخش‌های بله است.\n"
    "• لینک‌کردن گروه/کانال بله و گرفتن کد /verify\n"
    "• دیدن لیست گروه‌ها/کانال‌های لینک‌شده\n"
    "• جفت‌کردن گروه/کانال تلگرام ↔ بله (اگر هر دو را لینک کرده‌اید)\n"
    "• تنظیمات دایرکت (DM)\n\n"
    "<b>راهنما:</b>\n"
    "• در گروه/کانال مقصد، دستور <code>/verify CODE</code> را ارسال کنید.\n"
    "• برای پیدا کردن شناسه کاربری بله خود، از دستور <code>/myid</code> استفاده کنید."
)

def bale_kb_main_menu() -> InlineKeyboard:
    return InlineKeyboard(
        [("🔗 Link Bale Group", "B_LINK_GROUP"), ("🔗 Link Bale Channel", "B_LINK_CHANNEL")],
        [("📋 My Groups", "B_MY_GROUPS"), ("📋 My Channels", "B_MY_CHANNELS")],
        [("🔁 Pair Groups", "B_PAIR_GROUPS"), ("🔁 Pair Channels", "B_PAIR_CHANNELS")],
        [("⚙️ DM Settings", "B_DM_SETTINGS")]
    )

def bale_kb_back_menu() -> InlineKeyboard:
    return InlineKeyboard([("⬅️ Back to Menu", "B_MENU")])

def bale_kb_select_bale_group(rows) -> InlineKeyboard:
    # rows: (id, platform, chat_type, chat_id, title)
    buttons = []
    for (_rid, platform, ctype, chat_id, title) in rows:
        if platform == "bale" and ctype == "group":
            buttons.append([(f"[bale] {title or chat_id}", f"B_G_ITEM:{chat_id}")])
    buttons.append([("⬅️ Back to Menu", "B_MENU")])
    return InlineKeyboard(*buttons) if buttons else bale_kb_back_menu()

def bale_kb_select_bale_channel(rows) -> InlineKeyboard:
    buttons = []
    for (_rid, platform, ctype, chat_id, title) in rows:
        if platform == "bale" and ctype == "channel":
            buttons.append([(f"[bale] {title or chat_id}", f"B_C_ITEM:{chat_id}")])
    buttons.append([("⬅️ Back to Menu", "B_MENU")])
    return InlineKeyboard(*buttons) if buttons else bale_kb_back_menu()

def bale_kb_select_tg_group(rows) -> InlineKeyboard:
    # User is in Bale; show TG groups that belong to the same owner (if any)
    buttons = []
    for (_rid, platform, ctype, chat_id, title) in rows:
        if platform == "tg" and ctype == "group":
            buttons.append([(f"[tg] {title or chat_id}", f"B_PG_TG:{chat_id}")])
    buttons.append([("⬅️ Back to Menu", "B_MENU")])
    return InlineKeyboard(*buttons) if buttons else bale_kb_back_menu()

def bale_kb_select_tg_channel(rows) -> InlineKeyboard:
    buttons = []
    for (_rid, platform, ctype, chat_id, title) in rows:
        if platform == "tg" and ctype == "channel":
            buttons.append([(f"[tg] {title or chat_id}", f"B_PC_TG:{chat_id}")])
    buttons.append([("⬅️ Back to Menu", "B_MENU")])
    return InlineKeyboard(*buttons) if buttons else bale_kb_back_menu()

def kb_select_bale_dm_target(rows):
    kb = InlineKeyboardBuilder()
    for (_rid, platform, ctype, chat_id, title) in rows:
        if platform == "bale":
            label = f"[{ctype}] {title or chat_id}"
            kb.button(text=label, callback_data=f"SET_DM_TG2BALE_SELECT:{chat_id}")
    kb.button(text="⬅️ Back to Menu", callback_data="MENU")
    kb.adjust(1)
    return kb.as_markup()

def bale_kb_dm_settings(can_set_tg_target: bool) -> InlineKeyboard:
    rows = [
        [("Set TG→Bale DM target (this chat)", "B_SET_DM_TG2BALE_THIS")],
        [("Clear TG→Bale DM target", "B_CLR_DM_TG2BALE")],
    ]
    # Bale→TG target needs a TG chat/user id; we ask the user to send next message as the id
    if can_set_tg_target:
        rows.append([("Set Bale→TG DM target (enter TG id next)", "B_SET_DM_BALE2TG")])
        rows.append([("Clear Bale→TG DM target", "B_CLR_DM_BALE2TG")])
    else:
        rows.append([("Set Bale→TG DM target (enter TG id next)", "B_SET_DM_BALE2TG")])
    rows.append([("⬅️ Back to Menu", "B_MENU")])
    return InlineKeyboard(*rows)


HELP_TEXT = (
    "👋 <b>Public Bridge Bot</b>\n\n"
    "Use the buttons below to link your own groups/channels and pair them for two-way forwarding.\n\n"
    "• Link TG Group/Channel → get a verification code and instructions\n"
    "• My Groups/Channels → list your linked chats (with chat IDs)\n"
    "• Pair Groups/Channels → pick a TG chat then a Bale chat to bridge\n"
    "• DM Settings → set where your DMs should forward\n\n"
    "<b>Helpful Commands:</b>\n"
    "• On Bale, use <code>/link_group</code> or <code>/link_channel</code>, then <code>/verify CODE</code> in the target chat.\n"
    "• Use <code>/myid</code> to find your Telegram User ID."
)

# ---- Inline keyboards ----

def kb_main_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Link TG Group", callback_data="LINK_TG_GROUP")
    kb.button(text="🔗 Link TG Channel", callback_data="LINK_TG_CHANNEL")
    kb.button(text="📋 My Groups", callback_data="MY_GROUPS")
    kb.button(text="📋 My Channels", callback_data="MY_CHANNELS")
    kb.button(text="🔁 Pair Groups", callback_data="PAIR_GROUPS")
    kb.button(text="🔁 Pair Channels", callback_data="PAIR_CHANNELS")
    kb.button(text="⚙️ DM Settings", callback_data="DM_SETTINGS")
    kb.adjust(2, 2, 2, 1)
    return kb.as_markup()

def kb_back_to_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Back to Menu", callback_data="MENU")
    return kb.as_markup()

def kb_groups_select(rows, platform_filter=None) -> InlineKeyboardMarkup:
    """
    rows: list of (id, platform, chat_type, chat_id, title)
    platform_filter: "tg" or "bale" or None
    """
    kb = InlineKeyboardBuilder()
    for (_rid, platform, ctype, chat_id, title) in rows:
        if ctype != "group":
            continue
        if platform_filter and platform != platform_filter:
            continue
        label = f"[{platform}] {title or chat_id}"
        # We choose ‘TGSEL:’ and ‘BALESEL:’ later in the flow
        cb = f"GROUP_ITEM:{platform}:{chat_id}"
        kb.button(text=label, callback_data=cb)
    kb.adjust(1)
    kb.button(text="⬅️ Back to Menu", callback_data="MENU")
    return kb.as_markup()

def kb_channels_select(rows, platform_filter=None) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for (_rid, platform, ctype, chat_id, title) in rows:
        if ctype != "channel":
            continue
        if platform_filter and platform != platform_filter:
            continue
        label = f"[{platform}] {title or chat_id}"
        cb = f"CHANNEL_ITEM:{platform}:{chat_id}"
        kb.button(text=label, callback_data=cb)
    kb.adjust(1)
    kb.button(text="⬅️ Back to Menu", callback_data="MENU")
    return kb.as_markup()

def kb_dm_settings(has_tg_id: bool, has_bale_id: bool, tg_uid: int | None, bale_uid: int | None) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if bale_uid:
        kb.button(text=f"Set TG→Bale DM target = {bale_uid}", callback_data=f"SET_DM_TG2BALE:{bale_uid}")
    else:
        kb.button(text="Set TG→Bale DM target (choose from linked Bale chats)", callback_data="SET_DM_TG2BALE_PICK")
    if tg_uid:
        kb.button(text=f"Set Bale→TG DM target = {tg_uid}", callback_data=f"SET_DM_BALE2TG:{tg_uid}")
    else:
        kb.button(text="Set Bale→TG DM target (send /start on TG)", callback_data="NEED_TG_START")
    kb.button(text="ℹ️ How to find IDs?", callback_data="DM_IDS_HELP")
    kb.button(text="⬅️ Back to Menu", callback_data="MENU")
    kb.adjust(1)
    return kb.as_markup()

def gen_code(prefix: str) -> str:
    body = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"{prefix}-{body}"

def tg_name(u: types.User) -> str:
    if u.username:
        return f"@{u.username}"
    full = " ".join(filter(None, [u.first_name, u.last_name]))
    return full or f"id:{u.id}"

def prefix_with_username(username: str, text: str) -> str:
    return f"{username} sent this message: {text}"

def bale_name(author) -> str:
    if getattr(author, "username", None):
        return f"@{author.username}"
    full = " ".join(filter(None, [getattr(author, "first_name", None), getattr(author, "last_name", None)]))
    return full or f"id:{getattr(author, 'id', 'unknown')}"

# ------------------------------------------
# Media forwarding helpers (TG ↔ Bale)
# ------------------------------------------

async def forward_tg_text_to_bale(bale: BaleClient, chat_id: int, text: str):
    try:
        await bale.send_message(chat_id, text)
    except Exception:
        logging.exception("Bale send_message failed")

async def forward_bale_text_to_tg(tg_bot: TgBot, chat_id: int, text: str):
    try:
        await tg_bot.send_message(chat_id, text)
    except TelegramBadRequest as e:
        logging.error(f"Telegram API error: {e}")
    except Exception:
        logging.exception("Telegram send_message failed")

async def forward_tg_photo_to_bale(tg_bot: TgBot, bale: BaleClient, target_chat_id: int, photo_sizes, caption: Optional[str]):
    try:
        p: types.PhotoSize = photo_sizes[-1]
        bio = await tg_bot.download(p.file_id)
        await bale.send_photo(target_chat_id, bio.getvalue(), caption or "")
    except Exception:
        logging.exception("Forward TG photo → Bale failed")

async def forward_tg_document_to_bale(tg_bot: TgBot, bale: BaleClient, target_chat_id: int, document: types.Document, caption: Optional[str]):
    try:
        bio = await tg_bot.download(document.file_id)
        await bale.send_document(target_chat_id, bio.getvalue(), caption or (document.file_name or ""))
    except Exception:
        logging.exception("Forward TG document → Bale failed")

async def forward_tg_video_to_bale(tg_bot: TgBot, bale: BaleClient, target_chat_id: int, video: types.Video, caption: Optional[str]):
    try:
        bio = await tg_bot.download(video.file_id)
        await bale.send_video(target_chat_id, bio.getvalue(), caption or "")
    except Exception:
        logging.exception("Forward TG video → Bale failed")

async def forward_bale_photo_to_tg(tg_bot: TgBot, chat_id: int, file_id: str, bale: BaleClient, caption: Optional[str]):
    try:
        data = await bale.download(file_id)
        bf = BufferedInputFile(data, filename="photo.jpg")
        await tg_bot.send_photo(chat_id, bf, caption=caption or "")
    except Exception:
        logging.exception("Forward Bale photo → TG failed")

async def forward_bale_document_to_tg(tg_bot: TgBot, chat_id: int, file_id: str, bale: BaleClient, filename: str = "document.bin", caption: Optional[str] = None):
    try:
        data = await bale.download(file_id)
        bf = BufferedInputFile(data, filename=filename or "document.bin")
        await tg_bot.send_document(chat_id, bf, caption=caption or "")
    except Exception:
        logging.exception("Forward Bale document → TG failed")

async def forward_bale_video_to_tg(tg_bot: TgBot, chat_id: int, file_id: str, bale: BaleClient, caption: Optional[str]):
    try:
        data = await bale.download(file_id)
        bf = BufferedInputFile(data, filename="video.mp4")
        await tg_bot.send_video(chat_id, bf, caption=caption or "")
    except Exception:
        logging.exception("Forward Bale video → TG failed")

# ----------------
# User management
# ----------------

async def get_or_create_user_by_tg(db: aiosqlite.Connection, tg_user_id: int) -> int:
    cur = await db.execute("SELECT id FROM users WHERE tg_user_id=?", (tg_user_id,))
    row = await cur.fetchone()
    await cur.close()
    if row:
        return row[0]
    await db.execute(
        "INSERT INTO users (tg_user_id, created_at) VALUES (?, ?)",
        (tg_user_id, await now_iso())
    )
    await db.commit()
    return await get_or_create_user_by_tg(db, tg_user_id)

async def get_or_create_user_by_bale(db: aiosqlite.Connection, bale_user_id: int) -> int:
    cur = await db.execute("SELECT id FROM users WHERE bale_user_id=?", (bale_user_id,))
    row = await cur.fetchone()
    await cur.close()
    if row:
        return row[0]
    await db.execute(
        "INSERT INTO users (bale_user_id, created_at) VALUES (?, ?)",
        (bale_user_id, await now_iso())
    )
    await db.commit()
    return await get_or_create_user_by_bale(db, bale_user_id)

# ----------------------
# Verify code management
# ----------------------

async def create_verify_code(db: aiosqlite.Connection, owner_user_id: int, platform: str, chat_type: str, platform_user_id: int) -> str:
    code = gen_code("G" if chat_type == "group" else "C")
    expires = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    await db.execute(
        "INSERT INTO verify_tokens (code, owner_user_id, platform, chat_type, platform_user_id, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
        (code, owner_user_id, platform, chat_type, platform_user_id, expires)
    )
    await db.commit()
    return code

async def consume_verify_code(db: aiosqlite.Connection, code: str, platform: str, platform_user_id: int) -> Optional[Tuple[int, str]]:
    """
    Return (owner_user_id, chat_type) if valid and mark consumed; else None.
    """
    cur = await db.execute(
        "SELECT owner_user_id, chat_type, expires_at, consumed FROM verify_tokens WHERE code=? AND platform=? AND platform_user_id=?",
        (code, platform, platform_user_id)
    )
    row = await cur.fetchone()
    await cur.close()
    if not row:
        return None
    owner_user_id, chat_type, expires_at, consumed = row
    if consumed:
        return None
    if datetime.fromisoformat(expires_at) < datetime.now(timezone.utc):
        return None
    await db.execute("UPDATE verify_tokens SET consumed=1 WHERE code=?", (code,))
    await db.commit()
    return owner_user_id, chat_type

# --- DM verify tokens management ---

async def create_dm_verify_code(db, owner_user_id, target_platform, target_chat_id):
    code = "DM-" + uuid.uuid4().hex[:8].upper()
    expires = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    await db.execute(
        "INSERT INTO dm_verify_tokens (code, owner_user_id, target_platform, target_chat_id, expires_at) VALUES (?, ?, ?, ?, ?)",
        (code, owner_user_id, target_platform, target_chat_id, expires)
    )
    await db.commit()
    return code

async def consume_dm_verify_code(db, code, platform, chat_id):
    cur = await db.execute(
        "SELECT owner_user_id, target_platform, target_chat_id, expires_at, consumed FROM dm_verify_tokens WHERE code=?",
        (code,)
    )
    row = await cur.fetchone()
    await cur.close()
    if not row:
        return None
    owner_user_id, target_platform, target_chat_id, expires_at, consumed = row
    if consumed or target_platform != platform or target_chat_id != chat_id:
        return None
    if datetime.fromisoformat(expires_at) < datetime.now(timezone.utc):
        return None
    await db.execute("UPDATE dm_verify_tokens SET consumed=1 WHERE code=?", (code,))
    await db.commit()
    return owner_user_id

# ----------------
# Chat registries
# ----------------

async def register_chat(db: aiosqlite.Connection, owner_user_id: int, platform: str, chat_type: str, chat_id: int, title: str):
    await db.execute(
        """INSERT OR IGNORE INTO chats (owner_user_id, platform, chat_type, chat_id, title, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (owner_user_id, platform, chat_type, chat_id, title, await now_iso())
    )
    await db.commit()

async def list_owner_chats(db: aiosqlite.Connection, owner_user_id: int, platform: Optional[str], chat_type: Optional[str]):
    q = "SELECT id, platform, chat_type, chat_id, title FROM chats WHERE owner_user_id=?"
    args = [owner_user_id]
    if platform:
        q += " AND platform=?"
        args.append(platform)
    if chat_type:
        q += " AND chat_type=?"
        args.append(chat_type)
    q += " ORDER BY id"
    cur = await db.execute(q, tuple(args))
    rows = await cur.fetchall()
    await cur.close()
    return rows

async def pair_groups(db: aiosqlite.Connection, owner_user_id: int, tg_group_id: int, bale_group_id: int):
    await db.execute(
        """INSERT INTO group_links (owner_user_id, tg_group_id, bale_group_id, enabled, created_at)
           VALUES (?, ?, ?, 1, ?)""",
        (owner_user_id, tg_group_id, bale_group_id, await now_iso())
    )
    await db.commit()

async def pair_channels(db: aiosqlite.Connection, owner_user_id: int, tg_channel_id: int, bale_channel_id: int):
    await db.execute(
        """INSERT INTO channel_links (owner_user_id, tg_channel_id, bale_channel_id, enabled, created_at)
           VALUES (?, ?, ?, 1, ?)""",
        (owner_user_id, tg_channel_id, bale_channel_id, await now_iso())
    )
    await db.commit()

async def find_group_link_by_tg(db: aiosqlite.Connection, tg_group_id: int):
    cur = await db.execute(
        "SELECT bale_group_id, enabled FROM group_links WHERE tg_group_id=?", (tg_group_id,)
    )
    row = await cur.fetchone()
    await cur.close()
    if not row: return None
    return {"bale_group_id": row[0], "enabled": bool(row[1])}

async def find_group_link_by_bale(db: aiosqlite.Connection, bale_group_id: int):
    cur = await db.execute(
        "SELECT tg_group_id, enabled FROM group_links WHERE bale_group_id=?", (bale_group_id,)
    )
    row = await cur.fetchone()
    await cur.close()
    if not row: return None
    return {"tg_group_id": row[0], "enabled": bool(row[1])}

async def find_channel_link_by_tg(db: aiosqlite.Connection, tg_channel_id: int):
    cur = await db.execute(
        "SELECT bale_channel_id, enabled FROM channel_links WHERE tg_channel_id=?", (tg_channel_id,)
    )
    row = await cur.fetchone()
    await cur.close()
    if not row: return None
    return {"bale_channel_id": row[0], "enabled": bool(row[1])}

async def find_channel_link_by_bale(db: aiosqlite.Connection, bale_channel_id: int):
    cur = await db.execute(
        "SELECT tg_channel_id, enabled FROM channel_links WHERE bale_channel_id=?", (bale_channel_id,)
    )
    row = await cur.fetchone()
    await cur.close()
    if not row: return None
    return {"tg_channel_id": row[0], "enabled": bool(row[1])}

# ----------------------------
# Global bot instances/ids
# ----------------------------

@dataclass
class Bots:
    tg_bot: TgBot
    tg_bot_id: int
    bale: BaleClient
    bale_self_id: int

# ----------------------------
# Telegram handlers (aiogram)
# ----------------------------

async def merge_user_accounts(db: aiosqlite.Connection, tg_user_id: int, bale_user_id: int) -> int:
    """
    Links a tg_user_id and bale_user_id, merging records if they exist separately.
    Returns the final, correct owner_id for the merged user.
    """
    async with db.execute("BEGIN"):
        try:
            cur = await db.execute("SELECT id FROM users WHERE tg_user_id=?", (tg_user_id,))
            tg_user_row = await cur.fetchone()
            tg_owner_id = tg_user_row[0] if tg_user_row else None

            cur = await db.execute("SELECT id FROM users WHERE bale_user_id=?", (bale_user_id,))
            bale_user_row = await cur.fetchone()
            bale_owner_id = bale_user_row[0] if bale_user_row else None

            if tg_owner_id and bale_owner_id and tg_owner_id != bale_owner_id:
                # This is the merge case: two separate records for the same user.
                # We will merge everything into the bale_owner_id record.
                
                # 1. Re-assign chats and links from the temporary TG record to the main Bale record.
                await db.execute("UPDATE chats SET owner_user_id=? WHERE owner_user_id=?", (bale_owner_id, tg_owner_id))
                await db.execute("UPDATE group_links SET owner_user_id=? WHERE owner_user_id=?", (bale_owner_id, tg_owner_id))
                await db.execute("UPDATE channel_links SET owner_user_id=? WHERE owner_user_id=?", (bale_owner_id, tg_owner_id))
                await db.execute("UPDATE verify_tokens SET owner_user_id=? WHERE owner_user_id=?", (bale_owner_id, tg_owner_id))
                await db.execute("UPDATE dm_verify_tokens SET owner_user_id=? WHERE owner_user_id=?", (bale_owner_id, tg_owner_id))

                # 2. Add the tg_user_id to the main Bale record.
                await db.execute("UPDATE users SET tg_user_id=? WHERE id=?", (tg_user_id, bale_owner_id))

                # 3. Delete the now-empty temporary TG record.
                await db.execute("DELETE FROM users WHERE id=?", (tg_owner_id,))
                
                await db.commit()
                return bale_owner_id

            elif tg_owner_id and not bale_owner_id:
                # Simple case: Only a TG record exists. Link the Bale ID to it.
                await db.execute("UPDATE users SET bale_user_id=? WHERE id=?", (bale_user_id, tg_owner_id))
                await db.commit()
                return tg_owner_id
            
            elif bale_owner_id:
                # A Bale record exists. Ensure the TG ID is linked to it.
                await db.execute("UPDATE users SET tg_user_id=? WHERE id=?", (tg_user_id, bale_owner_id))
                await db.commit()
                return bale_owner_id
            
            return await get_or_create_user_by_tg(db, tg_user_id) # Fallback

        except Exception as e:
            await db.rollback()
            logging.error(f"Failed to merge accounts for tg_user_id={tg_user_id}, bale_user_id={bale_user_id}: {e}")
            raise

def setup_telegram_handlers(router: Router, bots: Bots):

    @router.message(F.chat.type == "private", F.text.in_({"/start", "/help", "/myid"}))
    async def on_start_help(message: types.Message):
        command = (message.text or "").strip()
        
        if command == "/myid":
            # Respond directly with the user's ID
            await message.reply(f"Your Telegram User ID is: <code>{message.from_user.id}</code>")
        else:
            # For /start and /help, show the main menu
            await message.answer(HELP_TEXT, reply_markup=kb_main_menu())
    @router.callback_query(F.data == "MENU")
    async def cb_menu(cq: CallbackQuery):
        await cq.message.edit_text(HELP_TEXT, reply_markup=kb_main_menu())
        await cq.answer()

    # ... (Omitted the LINK, MY, and PAIR handlers for brevity. Keep them in your code) ...
    # Link TG Group / Channel
    @router.callback_query(F.data == "LINK_TG_GROUP")
    async def cb_link_tg_group(cq: CallbackQuery):
        async with aiosqlite.connect(DB_PATH) as db:
            owner_id = await get_or_create_user_by_tg(db, cq.from_user.id)
            code = await create_verify_code(db, owner_id, "tg", "group", cq.from_user.id)
        text = (
            "🔗 <b>Link a Telegram Group</b>\n"
            "1) Add this bot to your TG group as admin\n"
            f"2) In that group, send: <code>/verify {code}</code>\n"
            "   (expires in 10 minutes)"
        )
        await cq.message.edit_text(text, reply_markup=kb_back_to_menu())
        await cq.answer("Code generated")

    @router.callback_query(F.data == "LINK_TG_CHANNEL")
    async def cb_link_tg_channel(cq: CallbackQuery):
        async with aiosqlite.connect(DB_PATH) as db:
            owner_id = await get_or_create_user_by_tg(db, cq.from_user.id)
            code = await create_verify_code(db, owner_id, "tg", "channel", cq.from_user.id)
        text = (
            "🔗 <b>Link a Telegram Channel</b>\n"
            "1) Add this bot to your TG channel with permission to post\n"
            f"2) Post in that channel: <code>/verify {code}</code>\n"
            "   (expires in 10 minutes)"
        )
        await cq.message.edit_text(text, reply_markup=kb_back_to_menu())
        await cq.answer("Code generated")

    # My Groups / Channels
    @router.callback_query(F.data == "MY_GROUPS")
    async def cb_my_groups(cq: CallbackQuery):
        async with aiosqlite.connect(DB_PATH) as db:
            owner_id = await get_or_create_user_by_tg(db, cq.from_user.id)
            rows = await list_owner_chats(db, owner_id, None, "group")
        if not rows:
            await cq.message.edit_text("You have no linked groups yet.", reply_markup=kb_back_to_menu())
        else:
            await cq.message.edit_text("📋 <b>Your Groups</b>:", reply_markup=kb_groups_select(rows))
        await cq.answer()

    # ... (keep other list/pair handlers)

    # ----- DM Settings (REVISED STRUCTURE) -----

    @router.callback_query(F.data == "DM_SETTINGS")
    async def cb_dm_settings(cq: CallbackQuery):
        async with aiosqlite.connect(DB_PATH) as db:
            owner_id = await get_or_create_user_by_tg(db, cq.from_user.id)
            row = await get_user_row_by_id(db, owner_id)
        _id, tg_uid, _bale_uid, tg2b_target, b2tg_target = row
        
        kb = InlineKeyboardBuilder()
        current_tg2b = f"(Current: {tg2b_target})" if tg2b_target else "(Not Set)"
        kb.button(text=f"Set TG→Bale Target {current_tg2b}", callback_data="SET_DM_TG2BALE_PICK")

        current_b2tg = f"(Current: {b2tg_target})" if b2tg_target else "(Not Set)"
        kb.button(text=f"Set Bale→TG Target {current_b2tg}", callback_data=f"SET_DM_BALE2TG:{tg_uid}")
        
        kb.button(text="⬅️ Back to Menu", callback_data="MENU")
        kb.adjust(1)
        
        await cq.message.edit_text("⚙️ <b>DM Settings</b>", reply_markup=kb.as_markup())
        await cq.answer()

    @router.callback_query(F.data.startswith("SET_DM_BALE2TG:"))
    async def cb_set_dm_bale2tg(cq: CallbackQuery):
        tg_chat_id = int(cq.data.split(":")[1])
        async with aiosqlite.connect(DB_PATH) as db:
            owner_id = await get_or_create_user_by_tg(db, cq.from_user.id)
            await db.execute("UPDATE users SET dm_target_telegram_chat_id=? WHERE id=?", (tg_chat_id, owner_id))
            await db.commit()
        await cq.message.edit_text("✔ Bale→TG DM target updated to this chat.", reply_markup=kb_back_to_menu())
        await cq.answer("Saved!")

    @router.callback_query(F.data == "SET_DM_TG2BALE_PICK")
    async def cb_set_dm_tg2bale_pick(cq: CallbackQuery):
        async with aiosqlite.connect(DB_PATH) as db:
            owner_id = await get_or_create_user_by_tg(db, cq.from_user.id)
            rows = await list_owner_chats(db, owner_id, "bale", None)
        
        kb = InlineKeyboardBuilder()
        kb.button(text="👤 Use my Bale Private Chat (Enter ID)", callback_data="SET_DM_TG2BALE_MANUAL")
        
        if rows:
            kb.button(text="--- OR Select a Linked Chat ---", callback_data="noop")
            for (_rid, platform, ctype, chat_id, title) in rows:
                kb.button(text=f"[{ctype}] {title or chat_id}", callback_data=f"SET_DM_TG2BALE_SELECT:{chat_id}")

        kb.button(text="⬅️ Back to Menu", callback_data="MENU")
        kb.adjust(1)
        await cq.message.edit_text("Select your TG→Bale DM target:", reply_markup=kb.as_markup())
        await cq.answer()

    @router.callback_query(F.data == "SET_DM_TG2BALE_MANUAL")
    async def cb_set_dm_tg2bale_manual(cq: CallbackQuery):
        TG_WIZ[cq.from_user.id] = {"mode": "AWAIT_BALE_ID"}
        text = (
            "Please send your Bale User ID in the next message.\n\n"
            "<b>How to find your ID?</b>\n"
            "1. Go to the bot in your **Bale** DMs.\n"
            "2. Send the command: <code>/myid</code>"
        )
        await cq.message.edit_text(text, reply_markup=kb_back_to_menu())
        await cq.answer("Waiting for your Bale ID...")

    @router.callback_query(F.data.startswith("SET_DM_TG2BALE_SELECT:"))
    async def cb_set_dm_tg2bale_select(cq: CallbackQuery):
        bale_chat_id = int(cq.data.split(":")[1])
        async with aiosqlite.connect(DB_PATH) as db:
            owner_id = await get_or_create_user_by_tg(db, cq.from_user.id)
            code = await create_dm_verify_code(db, owner_id, "bale", bale_chat_id)
        txt = (
            f"To confirm, send this code in the target Bale chat (<code>{bale_chat_id}</code>):\n"
            f"<code>/verify_dm {code}</code>"
        )
        await cq.message.edit_text(txt, reply_markup=kb_back_to_menu())
        await cq.answer("Verification code generated")

    @router.message(F.text.startswith("/verify_dm"))
    async def on_tg_verify_dm(message: types.Message):
        # ... (This function is unchanged)
        parts = (message.text or "").split(maxsplit=1)
        code = parts[1].strip() if len(parts) == 2 else ""
        async with aiosqlite.connect(DB_PATH) as db:
            owner_user_id = await consume_dm_verify_code(db, code, "tg", message.chat.id)
            if not owner_user_id:
                await message.reply("❌ Invalid/expired DM verification code.")
            else:
                await db.execute("UPDATE users SET dm_target_telegram_chat_id=? WHERE id=?", (message.chat.id, owner_user_id))
                await db.commit()
                await message.reply(f"✔ Bale→TG DM target set to <code>{message.chat.id}</code>.")

    # =========================================================================
    # === NEW DEDICATED HANDLER FOR WIZARD INPUT (THE FIX) ===
    # This handler ONLY runs for users who are currently in the AWAIT_BALE_ID state.
    # =========================================================================
    @router.message(F.chat.type == "private", lambda msg: msg.from_user.id in TG_WIZ)
    async def on_tg_await_bale_id(message: types.Message):
        user_id = message.from_user.id
        bale_id_str = (message.text or "").strip()

        if not (user_id in TG_WIZ and TG_WIZ[user_id].get("mode") == "AWAIT_BALE_ID"):
             # This check is for safety, but the lambda filter should prevent this.
            return

        if bale_id_str.isdigit():
            bale_chat_id = int(bale_id_str)
            async with aiosqlite.connect(DB_PATH) as db:
                try:
                    owner_id = await merge_user_accounts(db, user_id, bale_chat_id)
                    code = await create_dm_verify_code(db, owner_id, "bale", bale_chat_id)
                    txt = (
                        f"Great! I've received the ID <code>{bale_chat_id}</code>.\n\n"
                        f"To complete the setup, send this verification code in your **Bale DMs**:\n"
                        f"<code>/verify_dm {code}</code>"
                    )
                    await message.answer(txt, reply_markup=kb_back_to_menu())
                except Exception as e:
                    logging.error(f"Error during AWAIT_BALE_ID wizard step: {e}")
                    await message.answer("❌ An unexpected error occurred. Please try again.")
            
            del TG_WIZ[user_id]  # Clean up wizard state
        else:
            await message.answer("❌ That doesn't look like a valid ID. Please send numbers only.")

    # =========================================================================
    # === REVISED GENERIC DM HANDLER (for forwarding only) ===
    # =========================================================================
    @router.message(F.chat.type == "private")
    async def on_tg_dm(message: types.Message):
        # The wizard logic has been moved to its own handler.
        # This handler now only deals with commands and forwarding.
        if message.text and message.text.startswith('/'):
            if message.text.split(maxsplit=1)[0] not in ['/start', '/help', '/verify_dm']:
                await message.answer("Unknown command. Please use /start to see the main menu.")
            return

        async with aiosqlite.connect(DB_PATH) as db:
            owner_id = await get_or_create_user_by_tg(db, message.from_user.id)
            cur = await db.execute("SELECT dm_target_bale_chat_id FROM users WHERE id=?", (owner_id,))
            row = await cur.fetchone()
            if row and row[0]:
                target_bale = int(row[0])
                sender_prefix = f"[From Telegram DM] {tg_name(message.from_user)}: "
                if message.text: await forward_tg_text_to_bale(bots.bale, target_bale, f"{sender_prefix}{message.text}")
                elif message.photo: await forward_tg_photo_to_bale(bots.tg_bot, bots.bale, target_bale, message.photo, f"{sender_prefix}{message.caption or ''}")
                # ... etc for other media
            else:
                await message.answer("Your message was not forwarded. Use **DM Settings** to set a target.", reply_markup=kb_back_to_menu())

    # --- Keep all your other handlers for groups and channels below this line ---
    @router.message(F.text.startswith("/verify"))
    async def on_tg_verify_in_group(message: types.Message):
        # ... (function is unchanged)
        if message.from_user and message.from_user.id == bots.tg_bot_id: return
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) != 2: return
        code = parts[1].strip()
        platform_chat_type = "group" if message.chat.type in {"group", "supergroup"} else "channel"
        async with aiosqlite.connect(DB_PATH) as db:
            res = await consume_verify_code(db, code, "tg", message.from_user.id if message.from_user else 0)
            if not res:
                await message.reply("❌ Invalid/expired code.")
                return
            owner_user_id, chat_type = res
            if chat_type != platform_chat_type:
                await message.reply("❌ Code is for a different chat type.")
                return
            await register_chat(db, owner_user_id, "tg", chat_type, message.chat.id, message.chat.title or "")
            await message.reply(f"✔ Linked this {chat_type}: chat_id={message.chat.id}")

    @router.message((F.chat.type.in_({"group", "supergroup"})))
    async def on_tg_group_forward(message: types.Message):
        # ... (function is unchanged)
        if not message.from_user or message.from_user.id == bots.tg_bot_id: return
        async with aiosqlite.connect(DB_PATH) as db:
            link = await find_group_link_by_tg(db, message.chat.id)
            if not link or not link["enabled"]: return
            bale_group_id = link["bale_group_id"]
            sender = tg_name(message.from_user)
            text = message.text or message.caption
            if text: await forward_tg_text_to_bale(bots.bale, bale_group_id, prefix_with_username(sender, text))
            # ... etc for other media

    @router.channel_post()
    async def on_tg_channel_post(message: types.Message):
        # ... (function is unchanged)
        if message.from_user and message.from_user.id == bots.tg_bot_id: return
        async with aiosqlite.connect(DB_PATH) as db:
            link = await find_channel_link_by_tg(db, message.chat.id)
            if not link or not link["enabled"]: return
            bale_channel_id = link["bale_channel_id"]
            text = message.text or message.caption
            if text: await forward_tg_text_to_bale(bots.bale, bale_channel_id, text)
            # ... etc for other media

# -------------------------
# Bale polling / dispatcher
# -------------------------

async def poll_bale_updates(bots: Bots):
    # ... (docstring and initial setup are the same) ...
    logging.info("Starting Bale long polling…")
    offset: Optional[int] = None

    while True:
        try:
            async with bots.bale:
                while True:
                    try:
                        updates = await bots.bale.get_updates(offset, 100)
                        for upd in (updates or []):
                            try:
                                # ... (offset advancement and callback query logic remains the same) ...
                                # advance offset
                                upd_id = getattr(upd, "update_id", None) or getattr(upd, "id", None)
                                if isinstance(upd_id, int):
                                    nxt = upd_id + 1
                                    offset = nxt if (offset is None or nxt > offset) else offset

                                # -------- 1) CALLBACK QUERIES (handle FIRST) --------
                                cbq = getattr(upd, "callback_query", None)
                                if cbq:
                                    try:
                                        data = getattr(cbq, "data", "") or ""
                                        cq_msg = getattr(cbq, "message", None)
                                        cq_chat = getattr(cq_msg, "chat", None) if cq_msg else None
                                        cq_chat_id = getattr(cq_chat, "id", None) if cq_chat else None
                                        cq_author = getattr(cbq, "author", None)
                                        cq_author_id = getattr(cq_author, "id", 0) if cq_author else 0

                                        async with aiosqlite.connect(DB_PATH) as db:
                                            owner_id = await get_or_create_user_by_bale(db, cq_author_id)

                                            # Menu
                                            if data == "B_MENU":
                                                await bots.bale.send_message(cq_chat_id, BALE_HELP_TEXT, reply_markup=bale_kb_main_menu())
                                                try: await cbq.answer("Menu")
                                                except Exception: pass
                                                continue

                                            # Link Bale Group
                                            if data == "B_LINK_GROUP":
                                                code = await create_verify_code(db, owner_id, "bale", "group", cq_author_id)
                                                txt = (
                                                    "🔗 <b>Link a Bale Group</b>\n"
                                                    "1) Add this bot to your Bale group as admin\n"
                                                    f"2) Send in that group: <code>/verify {code}</code>\n"
                                                    "   (expires in 10 minutes)"
                                                )
                                                await bots.bale.send_message(cq_chat_id, txt, reply_markup=bale_kb_back_menu())
                                                try: await cbq.answer("Code generated")
                                                except Exception: pass
                                                continue

                                            # Link Bale Channel
                                            if data == "B_LINK_CHANNEL":
                                                code = await create_verify_code(db, owner_id, "bale", "channel", cq_author_id)
                                                txt = (
                                                    "🔗 <b>Link a Bale Channel</b>\n"
                                                    "1) Add this bot to your Bale channel with permission to post\n"
                                                    f"2) Post in that channel: <code>/verify {code}</code>\n"
                                                    "   (expires in 10 minutes)"
                                                )
                                                await bots.bale.send_message(cq_chat_id, txt, reply_markup=bale_kb_back_menu())
                                                try: await cbq.answer("Code generated")
                                                except Exception: pass
                                                continue

                                            # Lists
                                            if data == "B_MY_GROUPS":
                                                rows = await list_owner_chats(db, owner_id, None, "group")
                                                if not rows:
                                                    await bots.bale.send_message(cq_chat_id, "No groups linked yet.", reply_markup=bale_kb_back_menu())
                                                else:
                                                    lines = ["📋 <b>Your Groups</b>:"]
                                                    for (_rid, platform, ctype, chat_id, title) in rows:
                                                        lines.append(f" • [{platform}] chat_id={chat_id}  title={title or '-'}")
                                                    await bots.bale.send_message(cq_chat_id, "\n".join(lines), reply_markup=bale_kb_back_menu())
                                                try: await cbq.answer()
                                                except Exception: pass
                                                continue

                                            if data == "B_MY_CHANNELS":
                                                rows = await list_owner_chats(db, owner_id, None, "channel")
                                                if not rows:
                                                    await bots.bale.send_message(cq_chat_id, "No channels linked yet.", reply_markup=bale_kb_back_menu())
                                                else:
                                                    lines = ["📋 <b>Your Channels</b>:"]
                                                    for (_rid, platform, ctype, chat_id, title) in rows:
                                                        lines.append(f" • [{platform}] chat_id={chat_id}  title={title or '-'}")
                                                    await bots.bale.send_message(cq_chat_id, "\n".join(lines), reply_markup=bale_kb_back_menu())
                                                try: await cbq.answer()
                                                except Exception: pass
                                                continue

                                            # Pair Groups (TG first → Bale)
                                            if data == "B_PAIR_GROUPS":
                                                rows = await list_owner_chats(db, owner_id, None, "group")
                                                tgs = [r for r in rows if r[1] == "tg" and r[2] == "group"]
                                                if not tgs:
                                                    await bots.bale.send_message(cq_chat_id, "No Telegram groups found.", reply_markup=bale_kb_back_menu())
                                                else:
                                                    await bots.bale.send_message(cq_chat_id, "Step 1/2: Select your <b>Telegram</b> group", reply_markup=bale_kb_select_tg_group(rows))
                                                try: await cbq.answer()
                                                except Exception: pass
                                                continue

                                            if data.startswith("B_PG_TG:"):
                                                tg_id = int(data.split(":")[1])
                                                BALE_WIZ[cq_author_id] = {"mode": "PAIR_G_WAIT_BALE", "tg_id": tg_id}
                                                rows = await list_owner_chats(db, owner_id, None, "group")
                                                await bots.bale.send_message(cq_chat_id, f"Step 2/2: Select your <b>Bale</b> group to pair with TG:{tg_id}", reply_markup=bale_kb_select_bale_group(rows))
                                                try: await cbq.answer()
                                                except Exception: pass
                                                continue

                                            if data.startswith("B_G_ITEM:"):
                                                bale_gid = int(data.split(":")[1])
                                                st = BALE_WIZ.get(cq_author_id)
                                                if not st or st.get("mode") != "PAIR_G_WAIT_BALE":
                                                    try: await cbq.answer("Please select a Telegram group first.", show_alert=True)
                                                    except Exception: pass
                                                    continue
                                                tg_id = int(st["tg_id"])
                                                # validate + pair
                                                cur = await db.execute("SELECT 1 FROM chats WHERE owner_user_id=? AND platform='tg' AND chat_type='group' AND chat_id=?", (owner_id, tg_id))
                                                ok_tg = await cur.fetchone(); await cur.close()
                                                cur = await db.execute("SELECT 1 FROM chats WHERE owner_user_id=? AND platform='bale' AND chat_type='group' AND chat_id=?", (owner_id, bale_gid))
                                                ok_bale = await cur.fetchone(); await cur.close()
                                                if not (ok_tg and ok_bale):
                                                    try: await cbq.answer("Those groups are not linked to you.", show_alert=True)
                                                    except Exception: pass
                                                else:
                                                    await pair_groups(db, owner_id, tg_id, bale_gid)
                                                    await bots.bale.send_message(cq_chat_id, f"✔ Paired TG group <code>{tg_id}</code> ↔ Bale group <code>{bale_gid}</code>", reply_markup=bale_kb_back_menu())
                                                    try: await cbq.answer("Paired!")
                                                    except Exception: pass
                                                BALE_WIZ.pop(cq_author_id, None)
                                                continue

                                            # Pair Channels (TG first → Bale)
                                            if data == "B_PAIR_CHANNELS":
                                                rows = await list_owner_chats(db, owner_id, None, "channel")
                                                tgs = [r for r in rows if r[1] == "tg" and r[2] == "channel"]
                                                if not tgs:
                                                    await bots.bale.send_message(cq_chat_id, "No Telegram channels found.", reply_markup=bale_kb_back_menu())
                                                else:
                                                    await bots.bale.send_message(cq_chat_id, "Step 1/2: Select your <b>Telegram</b> channel", reply_markup=bale_kb_select_tg_channel(rows))
                                                try: await cbq.answer()
                                                except Exception: pass
                                                continue

                                            if data.startswith("B_PC_TG:"):
                                                tg_id = int(data.split(":")[1])
                                                BALE_WIZ[cq_author_id] = {"mode": "PAIR_C_WAIT_BALE", "tg_id": tg_id}
                                                rows = await list_owner_chats(db, owner_id, None, "channel")
                                                await bots.bale.send_message(cq_chat_id, f"Step 2/2: Select your <b>Bale</b> channel to pair with TG:{tg_id}", reply_markup=bale_kb_select_bale_channel(rows))
                                                try: await cbq.answer()
                                                except Exception: pass
                                                continue

                                            if data.startswith("B_C_ITEM:"):
                                                bale_cid = int(data.split(":")[1])
                                                st = BALE_WIZ.get(cq_author_id)
                                                if not st or st.get("mode") != "PAIR_C_WAIT_BALE":
                                                    try: await cbq.answer("Please select a Telegram channel first.", show_alert=True)
                                                    except Exception: pass
                                                    continue
                                                tg_id = int(st["tg_id"])
                                                cur = await db.execute("SELECT 1 FROM chats WHERE owner_user_id=? AND platform='tg' AND chat_type='channel' AND chat_id=?", (owner_id, tg_id))
                                                ok_tg = await cur.fetchone(); await cur.close()
                                                cur = await db.execute("SELECT 1 FROM chats WHERE owner_user_id=? AND platform='bale' AND chat_type='channel' AND chat_id=?", (owner_id, bale_cid))
                                                ok_bale = await cur.fetchone(); await cur.close()
                                                if not (ok_tg and ok_bale):
                                                    try: await cbq.answer("Those channels are not linked to you.", show_alert=True)
                                                    except Exception: pass
                                                else:
                                                    await pair_channels(db, owner_id, tg_id, bale_cid)
                                                    await bots.bale.send_message(cq_chat_id, f"✔ Paired TG channel <code>{tg_id}</code> ↔ Bale channel <code>{bale_cid}</code>", reply_markup=bale_kb_back_menu())
                                                    try: await cbq.answer("Paired!")
                                                    except Exception: pass
                                                BALE_WIZ.pop(cq_author_id, None)
                                                continue

                                            # DM settings menu
                                            if data == "B_DM_SETTINGS":
                                                await bots.bale.send_message(cq_chat_id, "⚙️ <b>DM Settings</b>", reply_markup=bale_kb_dm_settings(True))
                                                try: await cbq.answer()
                                                except Exception: pass
                                                continue

                                            # Set/Clear DM targets
                                            if data == "B_SET_DM_TG2BALE_THIS":
                                                await db.execute("UPDATE users SET dm_target_bale_chat_id=? WHERE id=?", (cq_chat_id, owner_id))
                                                await db.commit()
                                                await bots.bale.send_message(cq_chat_id, "✔ TG→Bale DM target set to this chat.", reply_markup=bale_kb_back_menu())
                                                try: await cbq.answer("Saved")
                                                except Exception: pass
                                                continue

                                            if data == "B_CLR_DM_TG2BALE":
                                                await db.execute("UPDATE users SET dm_target_bale_chat_id=NULL WHERE id=?", (owner_id,))
                                                await db.commit()
                                                await bots.bale.send_message(cq_chat_id, "✔ TG→Bale DM target cleared.", reply_markup=bale_kb_back_menu())
                                                try: await cbq.answer("Cleared")
                                                except Exception: pass
                                                continue

                                            if data == "B_SET_DM_BALE2TG":
                                                BALE_WIZ[cq_author_id] = {"mode": "SET_DM_BALE2TG"}
                                                await bots.bale.send_message(cq_chat_id, "Please send the <b>Telegram chat ID</b> next (user or chat id).", reply_markup=bale_kb_back_menu())
                                                try: await cbq.answer("Waiting for TG id…")
                                                except Exception: pass
                                                continue

                                            if data == "B_CLR_DM_BALE2TG":
                                                await db.execute("UPDATE users SET dm_target_telegram_chat_id=NULL WHERE id=?", (owner_id,))
                                                await db.commit()
                                                await bots.bale.send_message(cq_chat_id, "✔ Bale→TG DM target cleared.", reply_markup=bale_kb_back_menu())
                                                try: await cbq.answer("Cleared")
                                                except Exception: pass
                                                continue

                                        # Always try to answer to stop spinner
                                        try: await cbq.answer()
                                        except Exception: pass

                                        continue  # callback handled

                                    except Exception:
                                        logging.exception("Error handling Bale callback query")
                                        # don't return; let message handler try if any
                                # -------- 2) MESSAGE UPDATES --------
                                msg = getattr(upd, "message", None)
                                if not msg: continue
                                chat = getattr(msg, "chat", None)
                                if not chat: continue
                                chat_id = getattr(chat, "id", None)
                                chat_type = getattr(chat, "type", "")
                                text = getattr(msg, "text", None) or getattr(msg, "caption", None)
                                author = getattr(msg, "author", None)
                                author_id = getattr(author, "id", 0) if author else 0
                                sender = bale_name(author) if author else "unknown"

                                if author_id and author_id == bots.bale_self_id: continue

                                if text and text.startswith("/verify_dm"):
                                    # ... (This logic remains the same, but is crucial) ...
                                    parts = text.split(maxsplit=1)
                                    code = parts[1].strip() if len(parts) == 2 else ""
                                    async with aiosqlite.connect(DB_PATH) as db:
                                        try:
                                            owner_user_id = await consume_dm_verify_code(db, code, "bale", chat_id)
                                            if not owner_user_id:
                                                await bots.bale.send_message(chat_id, "❌ Invalid/expired DM verification code.")
                                            else:
                                                await db.execute("UPDATE users SET dm_target_bale_chat_id=? WHERE id=?", (chat_id, owner_user_id))
                                                await db.commit()
                                                await bots.bale.send_message(chat_id, f"✔ TG→Bale DM target set to this chat (<code>{chat_id}</code>).")
                                        except Exception:
                                            logging.exception("Bale /verify_dm failed")
                                    continue
                                
                                if chat_type == "private":
                                    async with aiosqlite.connect(DB_PATH) as db:
                                        owner_id = await get_or_create_user_by_bale(db, author_id)
                                        
                                        # ==== NEW: Handle /myid command ====
                                        if text and text.strip().lower() == "/myid":
                                            await bots.bale.send_message(chat_id, f"Your Bale User ID is: <code>{author_id}</code>")
                                            continue
                                        # ==== END of new block ====

                                        if text and text.strip().lower() in {"/start", "/help"}:
                                            await bots.bale.send_message(chat_id, BALE_HELP_TEXT, reply_markup=bale_kb_main_menu())
                                            continue

                                        # ... (The rest of the private chat logic for wizards and forwarding remains the same) ...
                                        st = BALE_WIZ.get(author_id)
                                        if st and st.get("mode") == "SET_DM_BALE2TG":
                                            val = (text or "").strip() if text else ""
                                            if val and val.lstrip("-").isdigit():
                                                target = int(val)
                                                await db.execute("UPDATE users SET dm_target_telegram_chat_id=? WHERE id=?", (target, owner_id))
                                                await db.commit()
                                                BALE_WIZ.pop(author_id, None)
                                                await bots.bale.send_message(chat_id, f"✔ Bale→TG DM target set to <code>{target}</code>.", reply_markup=bale_kb_back_menu())
                                            else:
                                                await bots.bale.send_message(chat_id, "❌ Please send a valid integer Telegram chat ID.")
                                            continue

                                        # Operator mirror (optional)
                                        if MIRROR_DMS_TO_OWNER and OWNER_BALE_CHAT_ID:
                                            try:
                                                await bots.bale.send_message(OWNER_BALE_CHAT_ID, f"[Bale DM] {sender}: {text or '[non-text]'}")
                                            except Exception:
                                                logging.exception("Mirror Bale DM → owner failed")

                                        # Per-user DM bridge (Bale → Telegram)
                                        cur = await db.execute("SELECT dm_target_telegram_chat_id FROM users WHERE id=?", (owner_id,))
                                        row = await cur.fetchone()
                                        if row and row[0]:
                                            target_tg = int(row[0])
                                            if text:
                                                await forward_bale_text_to_tg(bots.tg_bot, target_tg, f"[From Bale DM] {sender}: {text}")
                                            elif getattr(msg, "photo", None):
                                                await forward_bale_photo_to_tg(bots.tg_bot, target_tg, msg.photo.id, bots.bale, caption=f"[From Bale DM] {sender}: {getattr(msg, 'caption', '') or ''}")
                                            elif getattr(msg, "document", None):
                                                name = getattr(getattr(msg, "document", None), "file_name", "document.bin")
                                                await forward_bale_document_to_tg(bots.tg_bot, target_tg, msg.document.id, bots.bale, filename=name, caption=f"[From Bale DM] {sender}: {getattr(msg, 'caption', '') or ''}")
                                            elif getattr(msg, "video", None):
                                                await forward_bale_video_to_tg(bots.tg_bot, target_tg, msg.video.id, bots.bale, caption=f"[From Bale DM] {sender}: {getattr(msg, 'caption', '') or ''}")
                                            else:
                                                await forward_bale_text_to_tg(bots.tg_bot, target_tg, f"[From Bale DM] {sender}: [unsupported content]")
                                    continue

                                # ... (The rest of the function for /verify in groups, and forwarding remains the same) ...
                                if text and text.startswith("/verify"):
                                    parts = text.split(maxsplit=1)
                                    code = parts[1].strip() if len(parts) == 2 else ""
                                    expected = "group" if chat_type == "group" else ("channel" if chat_type == "channel" else None)
                                    if expected is None:
                                        continue
                                    async with aiosqlite.connect(DB_PATH) as db:
                                        res = await consume_verify_code(db, code, "bale", author_id)
                                        if not res:
                                            try: await bots.bale.send_message(chat_id, "❌ Invalid/expired code, or not yours.")
                                            except Exception: logging.exception("Failed to notify invalid code on Bale")
                                            continue
                                        owner_user_id, code_chat_type = res
                                        if code_chat_type != expected:
                                            try: await bots.bale.send_message(chat_id, "❌ This code is for a different chat type.")
                                            except Exception: logging.exception("Failed to notify wrong chat type on Bale")
                                            continue
                                        title = getattr(chat, "title", "") or ""
                                        await register_chat(db, owner_user_id, "bale", expected, chat_id, title)
                                        try: await bots.bale.send_message(chat_id, f"✔ Linked this {expected}: chat_id={chat_id}")
                                        except Exception: logging.exception("Failed to confirm link on Bale")
                                    continue

                                # group → TG group
                                if chat_type == "group":
                                    async with aiosqlite.connect(DB_PATH) as db:
                                        link = await find_group_link_by_bale(db, chat_id)
                                        if not link or not link.get("enabled"):
                                            continue
                                        tg_group_id = link["tg_group_id"]
                                    if text:
                                        await forward_bale_text_to_tg(bots.tg_bot, tg_group_id, prefix_with_username(sender, text))
                                    if getattr(msg, "photo", None):
                                        await forward_bale_photo_to_tg(bots.tg_bot, tg_group_id, msg.photo.id, bots.bale, caption=prefix_with_username(sender, getattr(msg, "caption", "") or ""))
                                    if getattr(msg, "document", None):
                                        name = getattr(getattr(msg, "document", None), "file_name", "document.bin")
                                        await forward_bale_document_to_tg(bots.tg_bot, tg_group_id, msg.document.id, bots.bale, filename=name, caption=prefix_with_username(sender, getattr(msg, "caption", "") or ""))
                                    if getattr(msg, "video", None):
                                        await forward_bale_video_to_tg(bots.tg_bot, tg_group_id, msg.video.id, bots.bale, caption=prefix_with_username(sender, getattr(msg, "caption", "") or ""))
                                    continue

                                # channel → TG channel
                                if chat_type == "channel":
                                    async with aiosqlite.connect(DB_PATH) as db:
                                        link = await find_channel_link_by_bale(db, chat_id)
                                        if not link or not link.get("enabled"):
                                            continue
                                        tg_channel_id = link["tg_channel_id"]
                                    if text:
                                        await forward_bale_text_to_tg(bots.tg_bot, tg_channel_id, text)
                                    if getattr(msg, "photo", None):
                                        await forward_bale_photo_to_tg(bots.tg_bot, tg_channel_id, msg.photo.id, bots.bale, caption=getattr(msg, "caption", "") or "")
                                    if getattr(msg, "document", None):
                                        name = getattr(getattr(msg, "document", None), "file_name", "document.bin")
                                        await forward_bale_document_to_tg(bots.tg_bot, tg_channel_id, msg.document.id, bots.bale, filename=name, caption=getattr(msg, "caption", "") or "")
                                    if getattr(msg, "video", None):
                                        await forward_bale_video_to_tg(bots.tg_bot, tg_channel_id, msg.video.id, bots.bale, caption=getattr(msg, "caption", "") or "")
                                    continue
                            except Exception:
                                logging.exception("Error handling a Bale update/message")
                        await asyncio.sleep(BALE_POLL_INTERVAL)
                    except Exception:
                        logging.exception("Bale polling iteration error; reconnecting soon…")
                        break
        except Exception:
            logging.exception("Bale client context error; retrying in 5s…")
            await asyncio.sleep(5.0)

# ----------------
# Startup / main
# ----------------

async def main():
    # DB
    await init_db()

    # Bots & ids
    tg_bot = TgBot(
    TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
    tg_me = await tg_bot.get_me()
    tg_bot_id = tg_me.id

    bale = BaleClient(BALE_TOKEN)
    # Get Bale self id (requires a short client open)
    async with bale:
        me = await bale.get_me()
        bale_self_id = getattr(me, "id", 0)

    bots = Bots(tg_bot=tg_bot, tg_bot_id=tg_bot_id, bale=bale, bale_self_id=bale_self_id)

    # Telegram router
    dp = Dispatcher()
    router = Router()
    dp.include_router(router)
    setup_telegram_handlers(router, bots)

    # Run both loops concurrently
    await asyncio.gather(
    dp.start_polling(tg_bot, allowed_updates=["message", "channel_post", "callback_query"]),
    poll_bale_updates(bots),
)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\nShutting down…")

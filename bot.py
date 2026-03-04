# bot.py
# python-telegram-bot >= 21 (async)
# Railway polling + SQLite
# IMPORTANT: сохраняем текущую архитектуру (1 файл), меню, БД (users/bookings/reviews) и логику.
# ФИКСЫ (ВАЖНО):
# 1) Мобильный Telegram: после выбора времени ВСЕГДА показываем экран подтверждения (не молчим).
# 2) Ручной ввод времени (HH:MM) + проверка диапазона/прошедшего времени.
# 3) Регистрация не повторяется и не сбрасывает выбор.
# 4) Уведомление админу всегда (try/except + лог).
# 5) В "Обо мне" смайлик вниз маленький (👇🏻).
# 6) Модерация отзывов админом: pending -> approve/decline, в списке показываем только approved.
# 7) Добавлены команды (/start /menu /book /prices /about /address /my /reviews /admin).

import os
import re
import html
import sqlite3
import logging
import calendar as pycal
from datetime import datetime, timedelta, date, time as dtime
from zoneinfo import ZoneInfo
from typing import Optional, List, Tuple

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError, BadRequest, Forbidden
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("nails-booking-bot")

# -------------------------
# ENV
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int((os.getenv("ADMIN_ID", "0") or "0").strip() or "0")

TZ_NAME = (os.getenv("TZ", "Europe/Moscow") or "Europe/Moscow").strip()
TZ = ZoneInfo(TZ_NAME)

AUTO_CLEAN = (os.getenv("AUTO_CLEAN", "1").strip() or "1") == "1"
SALON_TITLE = (os.getenv("SALON_TITLE", "Beauty Lounge") or "Beauty Lounge").strip()
MAPS_URL = (os.getenv("MAPS_URL", "https://yandex.ru/maps/") or "https://yandex.ru/maps/").strip()
ADDRESS_TEXT = (
    os.getenv("ADDRESS_TEXT", "Дальневосточный проспект 19 к 1, кв 69, этаж 10")
    or "Дальневосточный проспект 19 к 1, кв 69, этаж 10"
).strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required (Railway Variables).")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is required (Railway Variables).")

DB_PATH = (os.getenv("DB_PATH", "bot.db") or "bot.db").strip()

# -------------------------
# UI constants
# -------------------------
TRACK_KEEP = 6  # keep last N message_ids (both user + bot) per chat

PHOTO_URLS: List[str] = [
    # Можно добавить ссылки на фото (https://...) или оставить пустым
]

SERVICES = [
    ("💅 Маникюр", "manicure"),
    ("🦶 Педикюр", "pedicure"),
    ("✨ Наращивание", "extension"),
    ("🔧 Коррекция", "correction"),
]
SERVICE_LABEL_BY_KEY = {k: v for v, k in SERVICES}

WEEKDAYS_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
MONTHS_RU = [
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
]

PRICE_TEXT = (
    "💰 <b>Прайс</b>\n\n"
    "✨ <b>Маникюр без покрытия</b> — 1300 ₽\n"
    "💅 <b>Маникюр с покрытием</b> — 2500 ₽\n"
    "🎨 <b>Маникюр с покрытием + дизайн</b> — 3000 ₽\n\n"
    "🦶 <b>Педикюр без покрытия</b> — 2000 ₽\n"
    "💖 <b>Педикюр + покрытие</b> — 2800 ₽\n"
    "👣 <b>Педикюр пальчики</b> — 1800 ₽\n"
    "🦶 <b>Обработка стоп</b> — 1500 ₽\n\n"
    "✨ <b>Наращивание ногтей</b> — от 3500 ₽\n"
    "🔧 <b>Коррекция ногтей</b> — от 2800 ₽\n"
    "🎨 <b>Дизайн</b> — от 50 ₽ / ноготь\n\n"
    "Подсказка: нажмите <b>📅 Записаться</b>, чтобы выбрать услугу, дату и время."
)

ABOUT_TEXT = (
    "👩‍🎨 <b>О мастере</b>\n\n"
    "Привет! Я <b>Ира</b> 💛\n"
    "Стаж: <b>7+ лет</b> в маникюре и педикюре.\n\n"
    "✅ Стерильность и безопасность: одноразовые расходники, обработка инструментов.\n"
    "✅ Качественные материалы и аккуратная работа.\n"
    "✅ Комфортная атмосфера и бережный подход.\n\n"
    "Хочу, чтобы вы уходили с идеальными ногтями и отличным настроением ✨"
)

FIND_US_TEXT = (
    "📍 <b>Как нас найти</b>\n\n"
    f"🏠 Адрес:\n<b>{html.escape(ADDRESS_TEXT)}</b>\n\n"
    f"🗺 Ссылка на карту:\n{html.escape(MAPS_URL)}\n\n"
    "Если нужно — напишите, подскажу ориентиры 😊"
)

WELCOME_TEXT = (
    f"✨ <b>{html.escape(SALON_TITLE)}</b> — запись на маникюр/педикюр в пару кликов!\n\n"
    "Как это работает:\n"
    "1) Нажмите <b>📅 Записаться</b>\n"
    "2) Выберите услугу\n"
    "3) Выберите дату и время\n"
    "4) Подтвердите запись ✅\n\n"
    "После этого мастер подтвердит запись, и вы получите уведомление.\n"
    "Для первого раза потребуется короткая регистрация (имя + телефон)."
)

# -------------------------
# DB helpers (НЕ меняем структуру users/bookings/reviews)
# + добавляем отдельную таблицу для модерации отзывов (не ломает старые данные)
# -------------------------
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def now_tz() -> datetime:
    return datetime.now(tz=TZ)

def db_init() -> None:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER PRIMARY KEY,
                name        TEXT NOT NULL,
                phone       TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bookings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                service     TEXT NOT NULL,
                date        TEXT NOT NULL,
                time        TEXT NOT NULL,
                comment     TEXT,
                status      TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                reminded    INTEGER NOT NULL DEFAULT 0,
                admin_note  TEXT,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_bookings_user_datetime
            ON bookings (user_id, date, time);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_bookings_status_reminded
            ON bookings (status, reminded);
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                text        TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
        """)
        # separate moderation table (no schema change in reviews)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS review_moderation (
                review_id    INTEGER PRIMARY KEY,
                status       TEXT NOT NULL, -- pending/approved/declined
                moderated_by INTEGER,
                moderated_at TEXT,
                FOREIGN KEY(review_id) REFERENCES reviews(id)
            );
        """)
        # optional kv
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kv (
                key         TEXT PRIMARY KEY,
                value       TEXT NOT NULL
            );
        """)
        conn.commit()
    finally:
        conn.close()

def db_get_user(user_id: int) -> Optional[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cur.fetchone()
    finally:
        conn.close()

def db_upsert_user(user_id: int, name: str, phone: str) -> None:
    now_iso = now_tz().isoformat()
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users(user_id, name, phone, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                name=excluded.name,
                phone=excluded.phone
        """, (user_id, name, phone, now_iso))
        conn.commit()
    finally:
        conn.close()

def db_create_booking(user_id: int, service_key: str, d_ymd: str, t_hm: str, comment: str, status: str = "pending") -> int:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bookings(user_id, service, date, time, comment, status, created_at, reminded)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
        """, (user_id, service_key, d_ymd, t_hm, comment, status, now_tz().isoformat()))
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()

def db_get_booking(booking_id: int) -> Optional[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT b.*, u.name AS user_name, u.phone AS user_phone
            FROM bookings b
            JOIN users u ON u.user_id = b.user_id
            WHERE b.id = ?
        """, (booking_id,))
        return cur.fetchone()
    finally:
        conn.close()

def db_update_booking_status(booking_id: int, status: str) -> None:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE bookings SET status = ? WHERE id = ?", (status, booking_id))
        conn.commit()
    finally:
        conn.close()

def db_set_booking_reminded(booking_id: int) -> None:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE bookings SET reminded = 1 WHERE id = ?", (booking_id,))
        conn.commit()
    finally:
        conn.close()

def db_add_review(user_id: int, text: str) -> int:
    """Добавляем отзыв как pending (через review_moderation). Возвращает review_id."""
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO reviews(user_id, text, created_at)
            VALUES (?, ?, ?)
        """, (user_id, text, now_tz().isoformat()))
        review_id = int(cur.lastrowid)
        cur.execute("""
            INSERT OR REPLACE INTO review_moderation(review_id, status, moderated_by, moderated_at)
            VALUES (?, 'pending', NULL, NULL)
        """, (review_id,))
        conn.commit()
        return review_id
    finally:
        conn.close()

def db_get_review_with_user(review_id: int) -> Optional[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT r.*, u.name AS user_name, u.phone AS user_phone,
                   COALESCE(m.status, 'pending') AS mod_status
            FROM reviews r
            JOIN users u ON u.user_id = r.user_id
            LEFT JOIN review_moderation m ON m.review_id = r.id
            WHERE r.id = ?
        """, (review_id,))
        return cur.fetchone()
    finally:
        conn.close()

def db_set_review_status(review_id: int, status: str, moderated_by: int) -> None:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO review_moderation(review_id, status, moderated_by, moderated_at)
            VALUES (?, ?, ?, ?)
        """, (review_id, status, moderated_by, now_tz().isoformat()))
        conn.commit()
    finally:
        conn.close()

def db_list_last_approved_reviews(limit: int = 5) -> List[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT r.*, u.name AS user_name
            FROM reviews r
            JOIN users u ON u.user_id = r.user_id
            JOIN review_moderation m ON m.review_id = r.id
            WHERE m.status = 'approved'
            ORDER BY r.id DESC
            LIMIT ?
        """, (limit,))
        return cur.fetchall()
    finally:
        conn.close()

def booking_dt(d_ymd: str, t_hm: str) -> datetime:
    y, m, d = map(int, d_ymd.split("-"))
    hh, mm = map(int, t_hm.split(":"))
    return datetime(y, m, d, hh, mm, tzinfo=TZ)

def booking_dt_from_row(row: sqlite3.Row) -> datetime:
    return booking_dt(row["date"], row["time"])

def fmt_date_ru(d_ymd: str) -> str:
    y, m, d = map(int, d_ymd.split("-"))
    return f"{d:02d}.{m:02d}.{y}"

def fmt_datetime_ru(d_ymd: str, t_hm: str) -> str:
    return f"{fmt_date_ru(d_ymd)} {t_hm}"

def db_list_user_future_bookings(user_id: int) -> List[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT b.*, u.name AS user_name, u.phone AS user_phone
            FROM bookings b
            JOIN users u ON u.user_id = b.user_id
            WHERE b.user_id = ?
              AND b.status != 'cancelled'
            ORDER BY b.date ASC, b.time ASC
        """, (user_id,))
        rows = cur.fetchall()
    finally:
        conn.close()

    now_ = now_tz()
    out: List[sqlite3.Row] = []
    for r in rows:
        try:
            dt = booking_dt_from_row(r)
        except Exception:
            continue
        if dt >= now_:
            out.append(r)
    return out

def db_cancel_booking(booking_id: int) -> None:
    db_update_booking_status(booking_id, "cancelled")

def db_find_reminder_candidates(window_start: datetime, window_end: datetime) -> List[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT b.*, u.name AS user_name, u.phone AS user_phone
            FROM bookings b
            JOIN users u ON u.user_id = b.user_id
            WHERE b.reminded = 0
              AND b.status IN ('pending', 'confirmed')
        """)
        rows = cur.fetchall()
    finally:
        conn.close()

    out: List[sqlite3.Row] = []
    for r in rows:
        try:
            dt = booking_dt_from_row(r)
        except Exception:
            continue
        if window_start <= dt < window_end:
            out.append(r)
    return out

# -------------------------
# Menu + normalize (НЕ меняем названия кнопок)
# -------------------------
def is_admin_user(user_id: int) -> bool:
    return user_id == ADMIN_ID

def build_reply_kb(is_admin: bool) -> ReplyKeyboardMarkup:
    # 2 в ряд; структура меню не меняется
    if is_admin:
        buttons = [
            ["📅 Записаться", "💰 Цены"],
            ["👩‍🎨 Обо мне", "📍 Как нас найти"],
            ["📋 Мои записи", "⭐ Отзывы"],
            ["🏠 Меню", "🛠 Админ панель"],
        ]
    else:
        buttons = [
            ["📅 Записаться", "💰 Цены"],
            ["👩‍🎨 Обо мне", "📍 Как нас найти"],
            ["📋 Мои записи", "⭐ Отзывы"],
            ["🏠 Меню"],
        ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True, is_persistent=True)

def normalize_button(text: str) -> str:
    """
    FIX: Reply-кнопки могут приходить с разными emoji/variation selectors.
    Нормализуем надежно: выкидываем все, кроме букв/цифр/пробелов, и матчим по ключевым словам.
    """
    if not text:
        return ""

    # lower + trim
    raw = " ".join(text.strip().lower().split())

    # оставляем только буквы/цифры/пробелы (emoji и спецсимволы уйдут)
    cleaned = re.sub(r"[^0-9a-zа-яё\s]", " ", raw, flags=re.IGNORECASE)
    cleaned = " ".join(cleaned.split())

    aliases = {
        "записаться": "book",
        "запись": "book",
        "цены": "prices",
        "прайс": "prices",
        "обо мне": "about",
        "о мастере": "about",
        "как нас найти": "find",
        "адрес": "find",
        "мои записи": "my",
        "отзывы": "reviews",
        "отзыв": "reviews",
        "админ панель": "admin",
        "админка": "admin",
        "меню": "menu",
        "главное меню": "menu",
        "домой": "menu",
    }

    # точное совпадение
    if cleaned in aliases:
        return aliases[cleaned]

    # совпадение по окончанию (на случай “📋 Мои записи” и т.п.)
    for k, v in aliases.items():
        if cleaned.endswith(k):
            return v

    return ""

# -------------------------
# Chat cleanup
# -------------------------
def track_message_id(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int) -> None:
    if not AUTO_CLEAN:
        return
    cd = context.chat_data
    key = "tracked_message_ids"
    if key not in cd or not isinstance(cd.get(key), list):
        cd[key] = []
    cd[key].append(int(message_id))
    if len(cd[key]) > 60:
        cd[key] = cd[key][-30:]

async def cleanup_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, keep_last: int = TRACK_KEEP) -> None:
    if not AUTO_CLEAN:
        return
    ids = context.chat_data.get("tracked_message_ids", [])
    if not ids or len(ids) <= keep_last:
        return
    to_delete = ids[:-keep_last]
    context.chat_data["tracked_message_ids"] = ids[-keep_last:]
    for mid in to_delete:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except (BadRequest, Forbidden, TelegramError):
            continue

async def safe_send(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    *,
    reply_markup=None,
    parse_mode: Optional[str] = ParseMode.HTML,
    disable_web_page_preview: bool = True,
) -> Optional[int]:
    chat = update.effective_chat
    if not chat:
        return None
    msg = await context.bot.send_message(
        chat_id=chat.id,
        text=text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        disable_web_page_preview=disable_web_page_preview,
    )
    track_message_id(context, chat.id, msg.message_id)
    await cleanup_chat(context, chat.id)
    return msg.message_id

async def safe_edit_or_send(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    query=None,
    text: str,
    reply_markup=None,
    parse_mode: Optional[str] = ParseMode.HTML,
    disable_web_page_preview: bool = True,
) -> None:
    # FIX mobile: edit может падать -> fallback send, чтобы не было "тишины"
    chat = update.effective_chat
    if query and query.message:
        try:
            await query.message.edit_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            )
            if chat:
                track_message_id(context, chat.id, query.message.message_id)
                await cleanup_chat(context, chat.id)
            return
        except (BadRequest, TelegramError):
            pass
    await safe_send(update, context, text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=disable_web_page_preview)

# -------------------------
# Modes + booking keys
# -------------------------
def set_mode(context: ContextTypes.DEFAULT_TYPE, mode: Optional[str]) -> None:
    if mode:
        context.user_data["mode"] = mode
    else:
        context.user_data.pop("mode", None)

def get_mode(context: ContextTypes.DEFAULT_TYPE) -> str:
    return str(context.user_data.get("mode", "") or "")

def clear_booking_keys(context: ContextTypes.DEFAULT_TYPE) -> None:
    for k in ("service", "date", "time", "comment"):
        context.user_data.pop(k, None)

# -------------------------
# Time helpers + manual time
# -------------------------
def try_accept_manual_time(text: str) -> Tuple[bool, str]:
    if not text:
        return False, ""
    s = text.strip()
    if not re.fullmatch(r"\d{2}:\d{2}", s):
        return False, ""
    hh, mm = map(int, s.split(":"))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return False, ""
    # 08:00—23:00 (включительно 23:00)
    if (hh < 8) or (hh > 23) or (hh == 23 and mm > 0):
        return False, ""
    return True, f"{hh:02d}:{mm:02d}"

def time_slots_for_date(selected_date: date, now_dt: datetime) -> List[str]:
    slots: List[str] = []
    start = dtime(8, 0)
    end = dtime(23, 0)
    cur = datetime.combine(selected_date, start, tzinfo=TZ)
    last = datetime.combine(selected_date, end, tzinfo=TZ)
    while cur <= last:
        if selected_date == now_dt.date():
            if cur > now_dt:
                slots.append(cur.strftime("%H:%M"))
        else:
            slots.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=30)
    return slots

# -------------------------
# Inline keyboards
# -------------------------
def kb_services() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for label, key in SERVICES:
        row.append(InlineKeyboardButton(label, callback_data=f"service:{key}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")])
    return InlineKeyboardMarkup(rows)

def month_title_ru(year: int, month: int) -> str:
    return f"{MONTHS_RU[month-1]} {year}"

def clamp_month(year: int, month: int) -> Tuple[int, int]:
    if month < 1:
        return year - 1, 12
    if month > 12:
        return year + 1, 1
    return year, month

def kb_calendar(year: int, month: int, today: date) -> InlineKeyboardMarkup:
    cal = pycal.Calendar(firstweekday=0)  # Monday
    month_days = list(cal.itermonthdates(year, month))

    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton(f"📅 {month_title_ru(year, month)}", callback_data="noop")])
    rows.append([InlineKeyboardButton(d, callback_data="noop") for d in WEEKDAYS_RU])

    grid: List[List[InlineKeyboardButton]] = []
    week: List[InlineKeyboardButton] = []
    for d in month_days:
        if d.month != month:
            week.append(InlineKeyboardButton(" ", callback_data="noop"))
        else:
            if d < today:
                week.append(InlineKeyboardButton(f"{d.day}", callback_data="noop"))
            else:
                week.append(InlineKeyboardButton(f"{d.day}", callback_data=f"day:{d.isoformat()}"))
        if len(week) == 7:
            grid.append(week)
            week = []
    if week:
        while len(week) < 7:
            week.append(InlineKeyboardButton(" ", callback_data="noop"))
        grid.append(week)

    while len(grid) < 6:
        grid.append([InlineKeyboardButton(" ", callback_data="noop") for _ in range(7)])

    rows.extend(grid[:6])

    prev_y, prev_m = clamp_month(year, month - 1)
    next_y, next_m = clamp_month(year, month + 1)
    rows.append([
        InlineKeyboardButton("◀️", callback_data=f"cal:{prev_y}-{prev_m:02d}"),
        InlineKeyboardButton("Сегодня", callback_data="cal:today"),
        InlineKeyboardButton("▶️", callback_data=f"cal:{next_y}-{next_m:02d}"),
    ])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="nav:services")])
    return InlineKeyboardMarkup(rows)

def kb_time_picker(date_iso: str, slots: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton(f"🕒 Время: {fmt_date_ru(date_iso)}", callback_data="noop")])

    row: List[InlineKeyboardButton] = []
    for t in slots:
        row.append(InlineKeyboardButton(t, callback_data=f"time:{t}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if not slots:
        rows.append([InlineKeyboardButton("Нет доступного времени 🙈", callback_data="noop")])

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="nav:calendar")])
    return InlineKeyboardMarkup(rows)

def kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data="confirm:yes"),
            InlineKeyboardButton("✏️ Комментарий", callback_data="confirm:comment"),
        ],
        [
            InlineKeyboardButton("❌ Отменить", callback_data="confirm:cancel"),
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="nav:time"),
        ],
    ])

def admin_booking_controls(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"admin:confirm:{booking_id}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"admin:cancel:{booking_id}"),
        ],
        [
            InlineKeyboardButton("💬 Написать клиенту", callback_data=f"admin:msg:{booking_id}"),
        ],
    ])

def kb_prices_actions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Записаться", callback_data="nav:book")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")],
    ])

def kb_about_actions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📷 Фотогалерея", callback_data="about:photos")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")],
    ])

def kb_reviews_actions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✍️ Оставить отзыв", callback_data="reviews:write")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")],
    ])

def kb_review_moderation(review_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Одобрить", callback_data=f"rev:approve:{review_id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"rev:decline:{review_id}"),
        ]
    ])

# -------------------------
# Booking summary
# -------------------------
def booking_summary_text(user: sqlite3.Row, service_key: str, d_ymd: str, t_hm: str, comment: str) -> str:
    service_label = SERVICE_LABEL_BY_KEY.get(service_key, service_key or "—")
    text = (
        "🧾 <b>Подтверждение записи</b>\n\n"
        f"💎 Услуга: <b>{html.escape(service_label)}</b>\n"
        f"📅 Дата: <b>{html.escape(fmt_date_ru(d_ymd))}</b>\n"
        f"🕒 Время: <b>{html.escape(t_hm)}</b>\n\n"
        f"👤 Имя: <b>{html.escape(user['name'])}</b>\n"
        f"📞 Телефон: <b>{html.escape(user['phone'])}</b>\n"
    )
    if comment:
        text += f"\n💬 Комментарий: <i>{html.escape(comment)}</i>\n"
    text += "\nВыберите действие ниже 👇"
    return text

# -------------------------
# Registration helpers
# -------------------------
def normalize_phone(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    digits = re.sub(r"\D", "", s)
    if len(digits) < 10 or len(digits) > 12:
        return None
    if digits.startswith("8") and len(digits) == 11:
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    if digits.startswith("7") and len(digits) == 11:
        return "+" + digits
    return "+" + digits

async def ask_phone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    contact_kb = ReplyKeyboardMarkup(
        [
            [KeyboardButton("📲 Отправить номер", request_contact=True)],
            ["🏠 Меню"],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )
    await safe_send(
        update,
        context,
        "📞 Теперь отправьте <b>номер телефона</b>:\n\n"
        "• Нажмите <b>📲 Отправить номер</b>\n"
        "или\n"
        "• Введите номер вручную (например: <code>+79991234567</code>)",
        reply_markup=contact_kb,
    )
    set_mode(context, "await_phone")

async def resume_after_registration(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    user = db_get_user(uid)
    if not user:
        return

    service = context.user_data.get("service")
    d_ymd = context.user_data.get("date")
    t_hm = context.user_data.get("time")

    if service and d_ymd and t_hm:
        comment = (context.user_data.get("comment") or "").strip()
        set_mode(context, None)
        await safe_send(update, context, booking_summary_text(user, service, d_ymd, t_hm, comment), reply_markup=kb_confirm())
        return

    if service and d_ymd and not t_hm:
        selected = date.fromisoformat(d_ymd)
        slots = time_slots_for_date(selected, now_tz())
        set_mode(context, "await_time_text")
        await safe_send(
            update,
            context,
            (
                f"📅 Дата выбрана: <b>{html.escape(fmt_date_ru(d_ymd))}</b>\n\n"
                "Выберите время кнопками ниже или отправьте время вручную, например: <code>17:45</code>"
            ),
            reply_markup=kb_time_picker(d_ymd, slots),
        )
        return

    if service and not d_ymd:
        today = now_tz().date()
        set_mode(context, None)
        await safe_send(update, context, "Выберите дату 👇", reply_markup=kb_calendar(today.year, today.month, today))
        return

    set_mode(context, None)
    await safe_send(update, context, "Выберите услугу 👇", reply_markup=kb_services())

# -------------------------
# Registration check
# -------------------------
async def ensure_registered_or_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[sqlite3.Row]:
    uid = update.effective_user.id if update.effective_user else 0
    user = db_get_user(uid)
    if user:
        return user

    mode = get_mode(context)
    if mode in ("await_name", "await_phone"):
        return None

    kb = build_reply_kb(is_admin_user(uid))
    set_mode(context, "await_name")
    await safe_send(
        update,
        context,
        "📝 Для записи нужна регистрация (один раз).\n\n"
        "Пожалуйста, напишите <b>ваше имя</b> 👇",
        reply_markup=kb,
    )
    return None

# -------------------------
# Admin notify (КРИТИЧНО)
# -------------------------
async def notify_admin_new_booking(context: ContextTypes.DEFAULT_TYPE, booking_id: int) -> bool:
    row = db_get_booking(booking_id)
    if not row:
        log.error("notify_admin_new_booking: booking not found id=%s", booking_id)
        return False

    service_label = SERVICE_LABEL_BY_KEY.get(row["service"], row["service"])
    comment = (row["comment"] or "").strip()

    admin_text = (
        "🆕 <b>Новая запись</b>\n\n"
        f"🆔 <b>#{booking_id}</b>\n"
        f"💎 Услуга: <b>{html.escape(service_label)}</b>\n"
        f"📅 Дата/время: <b>{html.escape(fmt_datetime_ru(row['date'], row['time']))}</b>\n\n"
        f"👤 Клиент: <b>{html.escape(row['user_name'])}</b>\n"
        f"📞 Телефон: <b>{html.escape(row['user_phone'])}</b>\n"
    )
    if comment:
        admin_text += f"\n💬 Комментарий: <i>{html.escape(comment)}</i>\n"

    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=admin_text,
            parse_mode=ParseMode.HTML,
            reply_markup=admin_booking_controls(booking_id),
            disable_web_page_preview=True,
        )
        return True
    except TelegramError as e:
        log.exception("Failed to notify admin for booking_id=%s: %s", booking_id, e)
        return False

async def notify_admin_new_review(context: ContextTypes.DEFAULT_TYPE, review_id: int) -> bool:
    row = db_get_review_with_user(review_id)
    if not row:
        log.error("notify_admin_new_review: review not found id=%s", review_id)
        return False

    text = (
        "🆕 <b>Новый отзыв на модерацию</b>\n\n"
        f"🆔 <b>#{review_id}</b>\n"
        f"👤 Клиент: <b>{html.escape(row['user_name'])}</b>\n"
        f"📞 Телефон: <b>{html.escape(row['user_phone'])}</b>\n\n"
        f"🗣 Отзыв:\n<i>{html.escape(row['text'])}</i>"
    )
    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_review_moderation(review_id),
        )
        return True
    except TelegramError as e:
        log.exception("Failed to notify admin for review_id=%s: %s", review_id, e)
        return False

# -------------------------
# Menu actions
# -------------------------
async def send_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    kb = build_reply_kb(is_admin_user(uid))
    await safe_send(update, context, "🏠 <b>Главное меню</b>\n\nВыберите раздел кнопками ниже 👇", reply_markup=kb)

async def menu_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_registered_or_prompt(update, context)
    if not user:
        return
    set_mode(context, None)
    if not context.user_data.get("service"):
        clear_booking_keys(context)
        await safe_send(update, context, "📅 <b>Запись</b>\n\nВыберите услугу:", reply_markup=kb_services())
        return
    await resume_after_registration(update, context)

async def menu_prices(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    kb = build_reply_kb(is_admin_user(uid))
    await safe_send(update, context, PRICE_TEXT, reply_markup=kb, parse_mode=ParseMode.HTML)
    await safe_send(update, context, "👇 Быстрые действия:", reply_markup=kb_prices_actions())

async def menu_about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    kb = build_reply_kb(is_admin_user(uid))
    await safe_send(update, context, ABOUT_TEXT, reply_markup=kb, parse_mode=ParseMode.HTML)
    # FIX: маленький смайлик вниз
    await safe_send(update, context, "👇🏻", reply_markup=kb_about_actions())

async def menu_find(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    kb = build_reply_kb(is_admin_user(uid))
    await safe_send(update, context, FIND_US_TEXT, reply_markup=kb, parse_mode=ParseMode.HTML)

async def menu_my_bookings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_registered_or_prompt(update, context)
    if not user:
        return
    uid = update.effective_user.id if update.effective_user else 0
    kb = build_reply_kb(is_admin_user(uid))
    rows = db_list_user_future_bookings(user["user_id"])
    if not rows:
        await safe_send(update, context, "📋 <b>Мои записи</b>\n\nПока нет будущих записей.", reply_markup=kb)
        return

    text_lines = ["📋 <b>Мои записи</b>\n"]
    for r in rows[:10]:
        service_label = SERVICE_LABEL_BY_KEY.get(r["service"], r["service"])
        status = r["status"]
        status_emoji = "🟡" if status == "pending" else ("✅" if status == "confirmed" else "❌")
        text_lines.append(
            f"{status_emoji} <b>#{r['id']}</b> — {html.escape(service_label)}\n"
            f"📅 {html.escape(fmt_date_ru(r['date']))}  🕒 {html.escape(r['time'])}\n"
        )
    await safe_send(update, context, "\n".join(text_lines), reply_markup=kb)

    for r in rows[:6]:
        await safe_send(
            update,
            context,
            f"Действия для <b>#{r['id']}</b>:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отменить", callback_data=f"user:cancel:{r['id']}")],
                [InlineKeyboardButton("⬅️ Назад в меню", callback_data="nav:menu")],
            ]),
        )

async def menu_reviews(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    kb = build_reply_kb(is_admin_user(uid))
    rows = db_list_last_approved_reviews(5)
    if not rows:
        text = "⭐ <b>Отзывы</b>\n\nПока нет опубликованных отзывов. Хотите оставить? 😊"
    else:
        parts = ["⭐ <b>Отзывы</b>\n"]
        for r in rows:
            name = r["user_name"]
            created = (r["created_at"] or "")[:16].replace("T", " ")
            parts.append(f"🗣 <b>{html.escape(name)}</b> <i>({html.escape(created)})</i>\n{html.escape(r['text'])}\n")
        text = "\n".join(parts)
    await safe_send(update, context, text, reply_markup=kb)
    await safe_send(update, context, "👇", reply_markup=kb_reviews_actions())

async def menu_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin_user(uid):
        await safe_send(update, context, "⛔ Доступ только для администратора.")
        return

    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM bookings WHERE status='pending'")
        pending = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) AS c FROM bookings WHERE status='confirmed'")
        confirmed = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) AS c FROM bookings WHERE status='cancelled'")
        cancelled = cur.fetchone()["c"]
        today_ymd = now_tz().date().isoformat()
        cur.execute("SELECT COUNT(*) AS c FROM bookings WHERE date=? AND status IN ('pending','confirmed')", (today_ymd,))
        today_cnt = cur.fetchone()["c"]
    finally:
        conn.close()

    text = (
        "🛠 <b>Админ панель</b>\n\n"
        f"🟡 Ожидают подтверждения: <b>{pending}</b>\n"
        f"✅ Подтверждено: <b>{confirmed}</b>\n"
        f"❌ Отменено: <b>{cancelled}</b>\n"
        f"📅 Записей на сегодня: <b>{today_cnt}</b>\n\n"
        "Выберите действие:"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Записи на 7 дней", callback_data="adm:list:7")],
        [InlineKeyboardButton("📅 Записи на 14 дней", callback_data="adm:list:14")],
        [InlineKeyboardButton("📋 Все записи", callback_data="adm:list:all")],
        [InlineKeyboardButton("🟡 Ожидают подтверждения", callback_data="adm:list:pending")],
        [InlineKeyboardButton("✅ Подтвержденные", callback_data="adm:list:confirmed")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")],
    ])
    await safe_send(update, context, text, reply_markup=kb)

# -------------------------
# Reply dispatcher (ПЕРВЫМ)
# -------------------------
async def dispatch_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, key: str) -> bool:
    if not key:
        return False
    if key == "menu":
        await send_menu(update, context)
        return True
    if key == "book":
        await menu_book(update, context)
        return True
    if key == "prices":
        await menu_prices(update, context)
        return True
    if key == "about":
        await menu_about(update, context)
        return True
    if key == "find":
        await menu_find(update, context)
        return True
    if key == "my":
        await menu_my_bookings(update, context)
        return True
    if key == "reviews":
        await menu_reviews(update, context)
        return True
    if key == "admin":
        await menu_admin(update, context)
        return True
    return False

# -------------------------
# Commands (красиво)
# -------------------------
async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await send_menu(update, context)

async def cmd_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await menu_book(update, context)

async def cmd_prices(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await menu_prices(update, context)

async def cmd_about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await menu_about(update, context)

async def cmd_address(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await menu_find(update, context)

async def cmd_my(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await menu_my_bookings(update, context)

async def cmd_reviews(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await menu_reviews(update, context)

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await menu_admin(update, context)

async def setup_commands(app: Application) -> None:
    cmds = [
        BotCommand("start", "Запуск"),
        BotCommand("menu", "Главное меню"),
        BotCommand("book", "Записаться"),
        BotCommand("prices", "Цены"),
        BotCommand("about", "Обо мне"),
        BotCommand("address", "Как нас найти"),
        BotCommand("my", "Мои записи"),
        BotCommand("reviews", "Отзывы"),
        BotCommand("admin", "Админ панель"),
    ]
    try:
        await app.bot.set_my_commands(cmds)
    except TelegramError:
        log.exception("Failed to set bot commands")

# -------------------------
# /start
# -------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        track_message_id(context, update.effective_chat.id, update.message.message_id)

    uid = update.effective_user.id if update.effective_user else 0
    kb = build_reply_kb(is_admin_user(uid))

    await safe_send(update, context, WELCOME_TEXT, reply_markup=kb)

    user = db_get_user(uid)
    if user:
        set_mode(context, None)
        await safe_send(update, context, "✅ Вы уже зарегистрированы!\n\nНажмите <b>📅 Записаться</b> — и выберите удобное время 👇", reply_markup=kb)
        return

    if get_mode(context) not in ("await_name", "await_phone"):
        set_mode(context, "await_name")
        await safe_send(update, context, "📝 Давайте зарегистрируемся (один раз).\n\nНапишите <b>ваше имя</b> 👇", reply_markup=kb)

# -------------------------
# Contact handler
# -------------------------
async def on_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        track_message_id(context, update.effective_chat.id, update.message.message_id)

    uid = update.effective_user.id if update.effective_user else 0
    mode = get_mode(context)
    if mode != "await_phone":
        kb = build_reply_kb(is_admin_user(uid))
        await safe_send(update, context, "📲 Контакт получен, но сейчас он не требуется. Выберите раздел в меню 👇", reply_markup=kb)
        return

    contact = update.message.contact
    phone_raw = contact.phone_number if contact else ""
    phone = normalize_phone(phone_raw)
    if not phone:
        await safe_send(update, context, "❌ Не смог распознать номер. Попробуйте ещё раз или введите вручную (+7/8...).")
        return

    name = (context.user_data.get("reg_name") or "").strip()
    if not name:
        set_mode(context, "await_name")
        await safe_send(update, context, "📝 Похоже, имя не сохранено. Напишите, пожалуйста, <b>ваше имя</b> 👇")
        return

    db_upsert_user(uid, name=name, phone=phone)
    context.user_data.pop("reg_name", None)
    set_mode(context, None)

    kb = build_reply_kb(is_admin_user(uid))
    await safe_send(update, context, "✅ Готово! Регистрация завершена.", reply_markup=kb)
    await resume_after_registration(update, context)

# -------------------------
# Text handler (reply routing FIRST + manual time)
# -------------------------
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    chat_id = update.effective_chat.id
    track_message_id(context, chat_id, update.message.message_id)

    uid = update.effective_user.id if update.effective_user else 0
    text = (update.message.text or "").strip()

    # 1) Reply кнопки — ПЕРВЫМИ
    key = normalize_button(text)
    if key:
        handled = await dispatch_reply(update, context, key)
        if handled:
            return

    mode = get_mode(context)

    # ручной ввод времени
    if mode == "await_time_text":
        service = context.user_data.get("service")
        d_ymd = context.user_data.get("date")
        if not (service and d_ymd):
            set_mode(context, None)
        else:
            ok, tm = try_accept_manual_time(text)
            if not ok:
                await safe_send(update, context, "Введите время в формате <b>HH:MM</b>, например <code>17:45</code>")
                return

            now_ = now_tz()
            try:
                dt = booking_dt(d_ymd, tm)
            except Exception:
                await safe_send(update, context, "Введите время в формате <b>HH:MM</b>, например <code>17:45</code>")
                return

            if dt <= now_:
                await safe_send(update, context, "⛔ Это время уже прошло.\nВведите другое время: <code>17:45</code>")
                return

            context.user_data["time"] = tm
            set_mode(context, None)

            user = db_get_user(uid)
            if not user:
                await ensure_registered_or_prompt(update, context)
                return

            comment = (context.user_data.get("comment") or "").strip()
            await safe_send(update, context, booking_summary_text(user, service, d_ymd, tm, comment), reply_markup=kb_confirm())
            return

    # admin msg mode
    if mode == "admin_msg":
        if not is_admin_user(uid):
            set_mode(context, None)
            await safe_send(update, context, "⛔ Режим админ-сообщения недоступен.")
            return
        payload = context.user_data.get("admin_msg_payload") or {}
        booking_id = int(payload.get("booking_id") or 0)
        target_user_id = int(payload.get("user_id") or 0)
        if not booking_id or not target_user_id:
            set_mode(context, None)
            context.user_data.pop("admin_msg_payload", None)
            await safe_send(update, context, "⚠️ Не удалось определить получателя. Откройте действие заново в админ-панели.")
            return

        msg_text = text
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=f"💬 Сообщение от мастера:\n\n{html.escape(msg_text)}",
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            await safe_send(update, context, "❌ Не удалось отправить сообщение клиенту (возможно, он не начинал чат с ботом).")
        else:
            await safe_send(update, context, f"✅ Отправлено клиенту (booking #{booking_id}).")

        set_mode(context, None)
        context.user_data.pop("admin_msg_payload", None)
        return

    # registration: await_name
    if mode == "await_name":
        name = text
        if len(name) < 2:
            await safe_send(update, context, "❌ Имя слишком короткое. Напишите, пожалуйста, имя ещё раз 👇")
            return
        context.user_data["reg_name"] = name
        await ask_phone(update, context)
        return

    # registration: await_phone
    if mode == "await_phone":
        phone = normalize_phone(text)
        if not phone:
            await safe_send(update, context, "❌ Номер выглядит некорректно.\nПример: <code>+79991234567</code> или <code>89991234567</code>\nПопробуйте ещё раз 👇")
            return
        name = (context.user_data.get("reg_name") or "").strip()
        if not name:
            set_mode(context, "await_name")
            await safe_send(update, context, "📝 Сначала нужно имя. Напишите, пожалуйста, <b>ваше имя</b> 👇")
            return

        db_upsert_user(uid, name=name, phone=phone)
        context.user_data.pop("reg_name", None)
        set_mode(context, None)

        kb = build_reply_kb(is_admin_user(uid))
        await safe_send(update, context, "✅ Готово! Регистрация завершена.", reply_markup=kb)
        await resume_after_registration(update, context)
        return

    # comment mode
    if mode == "await_comment":
        context.user_data["comment"] = text
        set_mode(context, None)

        user = db_get_user(uid)
        if not user:
            await ensure_registered_or_prompt(update, context)
            return

        service = context.user_data.get("service")
        d_ymd = context.user_data.get("date")
        t_hm = context.user_data.get("time")
        if not (service and d_ymd and t_hm):
            await safe_send(update, context, "⚠️ Не вижу выбранные услугу/дату/время. Нажмите 📅 Записаться.")
            return

        comment = (context.user_data.get("comment") or "").strip()
        await safe_send(update, context, booking_summary_text(user, service, d_ymd, t_hm, comment), reply_markup=kb_confirm())
        return

    # review mode
    if mode == "await_review":
        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return
        if len(text) < 4:
            await safe_send(update, context, "❌ Отзыв слишком короткий. Напишите чуть подробнее 🙏")
            return

        review_id = db_add_review(uid, text)
        set_mode(context, None)

        await safe_send(update, context, "Спасибо! 💛\nВаш отзыв отправлен на модерацию ✅")
        await notify_admin_new_review(context, review_id)
        await menu_reviews(update, context)
        return

    kb = build_reply_kb(is_admin_user(uid))
    await safe_send(update, context, "Я на связи 😊\n\nВыберите нужный раздел в меню ниже 👇", reply_markup=kb)

# -------------------------
# Callback handler (FIX mobile)
# -------------------------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return

    # always answer (mobile)
    try:
        await query.answer()
    except TelegramError:
        pass

    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id and query.message:
        track_message_id(context, chat_id, query.message.message_id)

    data = query.data or ""
    uid = update.effective_user.id if update.effective_user else 0

    if data == "noop":
        try:
            await query.answer("Недоступно", show_alert=False)
        except TelegramError:
            pass
        return

    # NAV
    if data.startswith("nav:"):
        dest = data.split(":", 1)[1]

        if dest == "menu":
            set_mode(context, None)
            clear_booking_keys(context)
            await send_menu(update, context)
            return

        if dest == "book":
            await menu_book(update, context)
            return

        if dest == "services":
            user = await ensure_registered_or_prompt(update, context)
            if not user:
                return
            set_mode(context, None)
            context.user_data.pop("date", None)
            context.user_data.pop("time", None)
            await safe_edit_or_send(update, context, query=query, text="📅 <b>Запись</b>\n\nВыберите услугу:", reply_markup=kb_services())
            return

        if dest == "calendar":
            user = await ensure_registered_or_prompt(update, context)
            if not user:
                return
            if not context.user_data.get("service"):
                await safe_edit_or_send(update, context, query=query, text="Сначала выберите услугу 👇", reply_markup=kb_services())
                return
            today = now_tz().date()
            set_mode(context, None)
            await safe_edit_or_send(update, context, query=query, text="Выберите дату 👇", reply_markup=kb_calendar(today.year, today.month, today))
            return

        if dest == "time":
            user = await ensure_registered_or_prompt(update, context)
            if not user:
                return
            service = context.user_data.get("service")
            d_ymd = context.user_data.get("date")
            if not service:
                await safe_edit_or_send(update, context, query=query, text="Сначала выберите услугу 👇", reply_markup=kb_services())
                return
            if not d_ymd:
                today = now_tz().date()
                await safe_edit_or_send(update, context, query=query, text="Выберите дату 👇", reply_markup=kb_calendar(today.year, today.month, today))
                return

            selected = date.fromisoformat(d_ymd)
            slots = time_slots_for_date(selected, now_tz())
            set_mode(context, "await_time_text")
            await safe_edit_or_send(
                update, context, query=query,
                text=(
                    f"📅 Дата выбрана: <b>{html.escape(fmt_date_ru(d_ymd))}</b>\n\n"
                    "Выберите время кнопками ниже или отправьте время вручную, например: <code>17:45</code>"
                ),
                reply_markup=kb_time_picker(d_ymd, slots),
            )
            return

    # SERVICE
    if data.startswith("service:"):
        service_key = data.split(":", 1)[1]
        context.user_data["service"] = service_key
        context.user_data.pop("date", None)
        context.user_data.pop("time", None)

        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return

        today = now_tz().date()
        set_mode(context, None)
        await safe_edit_or_send(
            update, context, query=query,
            text=f"💎 Услуга: <b>{html.escape(SERVICE_LABEL_BY_KEY.get(service_key, service_key))}</b>\n\nВыберите дату 👇",
            reply_markup=kb_calendar(today.year, today.month, today),
        )
        return

    # CAL NAV
    if data.startswith("cal:"):
        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return

        today = now_tz().date()
        token = data.split(":", 1)[1]
        if token == "today":
            y, m = today.year, today.month
        else:
            try:
                y_s, m_s = token.split("-", 1)
                y, m = int(y_s), int(m_s)
            except Exception:
                y, m = today.year, today.month

        if (y, m) < (today.year, today.month):
            y, m = today.year, today.month

        await safe_edit_or_send(update, context, query=query, text="Выберите дату 👇", reply_markup=kb_calendar(y, m, today))
        return

    # DAY SELECT
    if data.startswith("day:"):
        d_iso = data.split(":", 1)[1]
        try:
            selected = date.fromisoformat(d_iso)
        except Exception:
            await safe_edit_or_send(update, context, query=query, text="❌ Некорректная дата. Попробуйте ещё раз.", reply_markup=None)
            return

        context.user_data["date"] = d_iso
        context.user_data.pop("time", None)

        if not context.user_data.get("service"):
            await safe_edit_or_send(update, context, query=query, text="Сначала выберите услугу 👇", reply_markup=kb_services())
            return

        now_ = now_tz()
        if selected < now_.date():
            try:
                await query.answer("Недоступно", show_alert=False)
            except TelegramError:
                pass
            return

        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return

        slots = time_slots_for_date(selected, now_)
        set_mode(context, "await_time_text")
        await safe_edit_or_send(
            update, context, query=query,
            text=(
                f"📅 Дата выбрана: <b>{html.escape(fmt_date_ru(d_iso))}</b>\n\n"
                "Выберите время кнопками ниже или отправьте время вручную, например: <code>17:45</code>"
            ),
            reply_markup=kb_time_picker(d_iso, slots),
        )
        return

    # TIME SELECT  (КРИТИЧНО: после выбора времени НЕ молчим — всегда отправляем подтверждение отдельным сообщением)
    if data.startswith("time:"):
        t_hm = data.split(":", 1)[1]
        if not re.fullmatch(r"\d{2}:\d{2}", t_hm):
            await safe_edit_or_send(update, context, query=query, text="❌ Некорректное время. Выберите ещё раз.", reply_markup=None)
            return

        context.user_data["time"] = t_hm

        service = context.user_data.get("service")
        d_ymd = context.user_data.get("date")
        if not (service and d_ymd):
            await safe_send(update, context, "⚠️ Начните заново: нажмите 📅 Записаться")
            return

        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return

        now_ = now_tz()
        try:
            dt = booking_dt(d_ymd, t_hm)
        except Exception:
            context.user_data.pop("time", None)
            await safe_send(update, context, "❌ Ошибка времени. Попробуйте ещё раз.")
            return

        if dt <= now_:
            context.user_data.pop("time", None)
            try:
                await query.answer("Недоступно", show_alert=False)
            except TelegramError:
                pass
            return

        # ВАЖНО: на мобильном edit часто "не виден" — поэтому подтверждение всегда отдельным сообщением
        set_mode(context, None)
        comment = (context.user_data.get("comment") or "").strip()
        await safe_send(update, context, booking_summary_text(user, service, d_ymd, t_hm, comment), reply_markup=kb_confirm())
        return

    # CONFIRM FLOW
    if data.startswith("confirm:"):
        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return

        action = data.split(":", 1)[1]
        service = context.user_data.get("service")
        d_ymd = context.user_data.get("date")
        t_hm = context.user_data.get("time")
        comment = (context.user_data.get("comment") or "").strip()

        if action == "cancel":
            clear_booking_keys(context)
            set_mode(context, None)
            await safe_send(update, context, "❌ Запись отменена.\n\nВозвращаю в меню 👇")
            await send_menu(update, context)
            return

        if action == "comment":
            if not (service and d_ymd and t_hm):
                await safe_send(update, context, "⚠️ Сначала выберите услугу, дату и время.")
                return
            set_mode(context, "await_comment")
            await safe_edit_or_send(
                update, context, query=query,
                text="✏️ Напишите комментарий к записи (пожелания, дизайн, нюансы) 👇\n\nМожно коротко 🙂",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="nav:time")]]),
            )
            return

        if action == "yes":
            if not (service and d_ymd and t_hm):
                await safe_send(update, context, "⚠️ Не хватает данных записи. Начните заново: 📅 Записаться")
                clear_booking_keys(context)
                return

            booking_id = db_create_booking(user["user_id"], service, d_ymd, t_hm, comment, status="pending")

            ok_admin = await notify_admin_new_booking(context, booking_id)
            extra = ""
            if not ok_admin:
                extra = "\n\n⚠️ Если мастер не ответит в течение 10 минут — напишите нам."

            await safe_edit_or_send(
                update, context, query=query,
                text=(
                    "✅ <b>Запись создана!</b>\n\n"
                    "Ожидайте подтверждения мастера 🟡\n\n"
                    f"Номер записи: <b>#{booking_id}</b>\n"
                    f"Услуга: <b>{html.escape(SERVICE_LABEL_BY_KEY.get(service, service))}</b>\n"
                    f"Дата/время: <b>{html.escape(fmt_datetime_ru(d_ymd, t_hm))}</b>\n"
                    f"{extra}"
                ),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="nav:menu")]]),
            )

            clear_booking_keys(context)
            set_mode(context, None)
            return

    # ABOUT PHOTOS
    if data == "about:photos":
        if not PHOTO_URLS:
            await safe_edit_or_send(
                update, context, query=query,
                text="📷 Фото скоро добавлю ✨",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]]),
            )
            return

        await safe_edit_or_send(
            update, context, query=query,
            text="📷 <b>Фотогалерея</b>\n\nЛистайте фото ниже 👇",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]]),
        )
        if chat_id:
            for url in PHOTO_URLS[:10]:
                try:
                    msg = await context.bot.send_photo(chat_id=chat_id, photo=url)
                    track_message_id(context, chat_id, msg.message_id)
                    await cleanup_chat(context, chat_id)
                except TelegramError:
                    continue
        return

    # REVIEWS: write
    if data == "reviews:write":
        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return
        set_mode(context, "await_review")
        await safe_edit_or_send(
            update, context, query=query,
            text="✍️ Напишите ваш отзыв одним сообщением 👇\n\n(Reply-кнопки меню по-прежнему работают.)",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]]),
        )
        return

    # REVIEWS: moderation (admin)
    if data.startswith("rev:"):
        if not is_admin_user(uid):
            await safe_edit_or_send(update, context, query=query, text="⛔ Доступ только для администратора.", reply_markup=None)
            return
        parts = data.split(":")
        if len(parts) != 3:
            await safe_edit_or_send(update, context, query=query, text="⚠️ Некорректная команда.", reply_markup=None)
            return
        action, rid_s = parts[1], parts[2]
        try:
            review_id = int(rid_s)
        except Exception:
            await safe_edit_or_send(update, context, query=query, text="⚠️ Некорректный ID отзыва.", reply_markup=None)
            return

        row = db_get_review_with_user(review_id)
        if not row:
            await safe_edit_or_send(update, context, query=query, text="⚠️ Отзыв не найден.", reply_markup=None)
            return

        if action == "approve":
            db_set_review_status(review_id, "approved", uid)
            await safe_edit_or_send(update, context, query=query, text=f"✅ Отзыв <b>#{review_id}</b> одобрен.", reply_markup=None)
            return
        if action == "decline":
            db_set_review_status(review_id, "declined", uid)
            await safe_edit_or_send(update, context, query=query, text=f"❌ Отзыв <b>#{review_id}</b> отклонён.", reply_markup=None)
            return

    # USER CANCEL BOOKING
    if data.startswith("user:cancel:"):
        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return
        try:
            booking_id = int(data.split(":")[2])
        except Exception:
            await safe_edit_or_send(update, context, query=query, text="❌ Некорректный номер записи.", reply_markup=None)
            return

        row = db_get_booking(booking_id)
        if not row or int(row["user_id"]) != int(user["user_id"]):
            await safe_edit_or_send(update, context, query=query, text="⚠️ Запись не найдена.", reply_markup=None)
            return

        db_cancel_booking(booking_id)
        await safe_edit_or_send(update, context, query=query, text=f"❌ Запись <b>#{booking_id}</b> отменена.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Меню", callback_data="nav:menu")],
        ]))

        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    "❌ <b>Отмена записи клиентом</b>\n\n"
                    f"🆔 <b>#{booking_id}</b>\n"
                    f"👤 Клиент: <b>{html.escape(row['user_name'])}</b>\n"
                    f"📅 Дата/время: <b>{html.escape(fmt_datetime_ru(row['date'], row['time']))}</b>\n"
                    f"💎 Услуга: <b>{html.escape(SERVICE_LABEL_BY_KEY.get(row['service'], row['service']))}</b>\n"
                ),
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            pass
        return

    # ADMIN LIST
    if data.startswith("adm:list:"):
        if not is_admin_user(uid):
            await safe_edit_or_send(update, context, query=query, text="⛔ Доступ только для администратора.", reply_markup=None)
            return

        token = data.split(":", 2)[2]
        now_ = now_tz()
        conn = db_connect()
        try:
            cur = conn.cursor()
            rows = []
            if token == "7":
                end_date = (now_.date() + timedelta(days=7)).isoformat()
                cur.execute("""
                    SELECT b.*, u.name AS user_name, u.phone AS user_phone
                    FROM bookings b
                    JOIN users u ON u.user_id = b.user_id
                    WHERE b.status IN ('pending','confirmed')
                      AND b.date >= ?
                      AND b.date <= ?
                    ORDER BY b.date ASC, b.time ASC
                    LIMIT 50
                """, (now_.date().isoformat(), end_date))
                rows = cur.fetchall()
                title = "📅 Записи на 7 дней"
            elif token in ("pending", "confirmed"):
                cur.execute("""
                    SELECT b.*, u.name AS user_name, u.phone AS user_phone
                    FROM bookings b
                    JOIN users u ON u.user_id = b.user_id
                    WHERE b.status = ?
                    ORDER BY b.date ASC, b.time ASC
                    LIMIT 50
                """, (token,))
                rows = cur.fetchall()
                title = f"🧾 Список: {token}"
            else:
                rows = []
                title = "🧾 Список"
        finally:
            conn.close()

        if not rows:
            await safe_edit_or_send(update, context, query=query, text=f"<b>{html.escape(title)}</b>\n\nПока пусто.", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]
            ]))
            return

        lines = [f"<b>{html.escape(title)}</b>\n"]
        for r in rows[:20]:
            status = r["status"]
            status_emoji = "🟡" if status == "pending" else ("✅" if status == "confirmed" else "❌")
            lines.append(
                f"{status_emoji} <b>#{r['id']}</b> — {html.escape(SERVICE_LABEL_BY_KEY.get(r['service'], r['service']))}\n"
                f"📅 {html.escape(fmt_date_ru(r['date']))}  🕒 {html.escape(r['time'])}\n"
                f"👤 {html.escape(r['user_name'])}  📞 {html.escape(r['user_phone'])}\n"
            )

        await safe_edit_or_send(update, context, query=query, text="\n".join(lines), reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]
        ]))
        return

    # ADMIN CONFIRM/CANCEL/MSG
    if data.startswith("adm:list:"):
        if not is_admin_user(uid):
            await safe_edit_or_send(update, context, query=query, text="⛔ Доступ только для администратора.", reply_markup=None)
            return

        token = data.split(":", 2)[2]
        now_ = now_tz()
        conn = db_connect()
        try:
            cur = conn.cursor()
            rows = []
            title = "📋 Записи"

            if token in ("7", "14"):
                days = int(token)
                end_date = (now_.date() + timedelta(days=days)).isoformat()
                cur.execute("""
                    SELECT b.*, u.name AS user_name, u.phone AS user_phone
                    FROM bookings b
                    JOIN users u ON u.user_id = b.user_id
                    WHERE b.status IN ('pending','confirmed')
                      AND b.date >= ?
                      AND b.date <= ?
                    ORDER BY b.date ASC, b.time ASC
                    LIMIT 80
                """, (now_.date().isoformat(), end_date))
                rows = cur.fetchall()
                title = f"📅 Записи на {days} дней"

            elif token == "all":
                cur.execute("""
                    SELECT b.*, u.name AS user_name, u.phone AS user_phone
                    FROM bookings b
                    JOIN users u ON u.user_id = b.user_id
                    WHERE b.status IN ('pending','confirmed','cancelled')
                    ORDER BY b.date ASC, b.time ASC
                    LIMIT 100
                """)
                rows = cur.fetchall()
                title = "📋 Все записи"

            elif token in ("pending", "confirmed"):
                cur.execute("""
                    SELECT b.*, u.name AS user_name, u.phone AS user_phone
                    FROM bookings b
                    JOIN users u ON u.user_id = b.user_id
                    WHERE b.status = ?
                    ORDER BY b.date ASC, b.time ASC
                    LIMIT 80
                """, (token,))
                rows = cur.fetchall()
                title = "🟡 Ожидают подтверждения" if token == "pending" else "✅ Подтвержденные"

            else:
                rows = []
                title = "📋 Записи"
        finally:
            conn.close()

        if not rows:
            await safe_edit_or_send(
                update, context, query=query,
                text=f"<b>{html.escape(title)}</b>\n\nПока пусто.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]])
            )
            return

        lines = [f"<b>{html.escape(title)}</b>\n"]
        for r in rows[:30]:
            status = r["status"]
            status_emoji = "🟡" if status == "pending" else ("✅" if status == "confirmed" else "❌")
            lines.append(
                f"{status_emoji} <b>#{r['id']}</b> — {html.escape(SERVICE_LABEL_BY_KEY.get(r['service'], r['service']))}\n"
                f"📅 {html.escape(fmt_date_ru(r['date']))}  🕒 {html.escape(r['time'])}\n"
                f"👤 {html.escape(r['user_name'])}  📞 {html.escape(r['user_phone'])}\n"
            )

        await safe_edit_or_send(
            update, context, query=query,
            text="\n".join(lines),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]])
        )
        return
        if action == "msg":
            set_mode(context, "admin_msg")
            context.user_data["admin_msg_payload"] = {"booking_id": booking_id, "user_id": int(row["user_id"])}
            await safe_edit_or_send(
                update, context, query=query,
                text=(
                    f"💬 <b>Сообщение клиенту</b>\n\n"
                    f"Запись <b>#{booking_id}</b>\n"
                    f"Клиент: <b>{html.escape(row['user_name'])}</b>\n\n"
                    "Напишите текст одним сообщением 👇\n"
                    "(Reply-кнопки админа продолжают работать.)"
                ),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]]),
            )
            return

    await safe_edit_or_send(update, context, query=query, text="⚠️ Не понял действие. Вернитесь в меню 👇", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 Меню", callback_data="nav:menu")]
    ]))

# -------------------------
# Reminders job
# -------------------------
async def reminders_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    now_ = now_tz()
    window_start = now_ + timedelta(hours=23)
    window_end = now_ + timedelta(hours=25)

    candidates = db_find_reminder_candidates(window_start, window_end)
    if not candidates:
        return

    for r in candidates:
        booking_id = int(r["id"])
        try:
            dt = booking_dt_from_row(r)
        except Exception:
            db_set_booking_reminded(booking_id)
            continue

        delta = dt - now_
        label = "завтра" if 20 <= delta.total_seconds() / 3600 <= 30 else "через 24 часа"

        text = (
            f"⏰ <b>Напоминание о записи {label}</b>\n\n"
            f"💎 Услуга: <b>{html.escape(SERVICE_LABEL_BY_KEY.get(r['service'], r['service']))}</b>\n"
            f"📅 Дата/время: <b>{html.escape(fmt_datetime_ru(r['date'], r['time']))}</b>\n\n"
            f"📍 Адрес: <b>{html.escape(ADDRESS_TEXT)}</b>\n"
            f"🗺 Ссылка на карту: {html.escape(MAPS_URL)}\n\n"
            "До встречи! 💛"
        )
        try:
            await context.bot.send_message(
                chat_id=int(r["user_id"]),
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except TelegramError:
            db_set_booking_reminded(booking_id)
            continue

        db_set_booking_reminded(booking_id)

# -------------------------
# Main
# -------------------------
def main() -> None:
    db_init()

    app = Application.builder().token(BOT_TOKEN).build()

    # Job queue reminder every 15 minutes
    if app.job_queue:
        app.job_queue.run_repeating(reminders_job, interval=15 * 60, first=30)

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("book", cmd_book))
    app.add_handler(CommandHandler("prices", cmd_prices))
    app.add_handler(CommandHandler("about", cmd_about))
    app.add_handler(CommandHandler("address", cmd_address))
    app.add_handler(CommandHandler("my", cmd_my))
    app.add_handler(CommandHandler("reviews", cmd_reviews))
    app.add_handler(CommandHandler("admin", cmd_admin))

    # Один CallbackQueryHandler (чтобы один callback не попадал в два обработчика)
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.CONTACT, on_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # set commands once on startup
    async def _post_init(application: Application) -> None:
        await setup_commands(application)

    app.post_init = _post_init

    log.info("Starting bot (polling). TZ=%s AUTO_CLEAN=%s ADMIN_ID=%s", TZ_NAME, AUTO_CLEAN, ADMIN_ID)
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()


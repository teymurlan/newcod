import os
import sys
import sqlite3
import asyncio
import logging
import re
import calendar
from datetime import datetime, timedelta, date
import pytz
from typing import List, Optional, Dict

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
    Message,
    InputMediaPhoto,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    JobQueue,
)
from telegram.error import BadRequest

# --- CONFIGURATION ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    print("CRITICAL ERROR: BOT_TOKEN is not set!")
    sys.exit(1)

def get_env_int(name, default):
    val = os.getenv(name, "")
    if not val or not val.strip():
        return int(default)
    # Remove all spaces and common typos
    clean_val = val.replace(" ", "").replace("−", "-").strip()
    if not clean_val:
        return int(default)
    try:
        return int(clean_val)
    except ValueError:
        print(f"WARNING: Invalid value for {name}: '{val}'. Using default: {default}")
        return int(default)

ADMIN_ID = get_env_int("ADMIN_ID", "0")
NOTIFICATION_CHAT_ID = get_env_int("NOTIFICATION_CHAT_ID", str(ADMIN_ID))
TZ_NAME = os.getenv("TZ", "Europe/Moscow")
AUTO_CLEAN = os.getenv("AUTO_CLEAN", "1") == "1"
SALON_TITLE = os.getenv("SALON_TITLE", "Beauty Lounge")
MAPS_URL = os.getenv("MAPS_URL", "https://yandex.ru/maps/")
ADDRESS_TEXT = os.getenv("ADDRESS_TEXT", "Дальневосточный проспект 19 к 1, кв 69, этаж 10")
MASTER_PHONE = os.getenv("MASTER_PHONE", "+79990000000")
MASTER_TG = os.getenv("MASTER_TG", "teymurlannn")

SERVICE_PRICES = {
    "Маникюр с покрытием": 2500,
    "Маникюр без покрытия": 1300,
    "Маникюр + дизайн": 3000,
    "Педикюр с покрытием": 2800,
    "Педикюр без покрытия": 2000,
    "Педикюр (пальчики)": 1800,
    "Обработка стоп": 1500,
    "Наращивание": 3500,
    "Коррекция": 2800
}

DISCOUNT_PERCENT = 7
DISCOUNT_END_DATE = date(2026, 4, 4)

MOSCOW_TZ = pytz.timezone(TZ_NAME)

FAQ_DATA = [
    ("Сколько держится покрытие?", "В среднем 3-4 недели. Мы даем гарантию 7 дней на любые отслойки."),
    ("Стерильны ли инструменты?", "Абсолютно. Инструменты проходят 3 этапа стерилизации, включая сухожар. Крафт-пакет вскрывается при вас."),
    ("Сколько времени занимает процедура?", "Маникюр с покрытием занимает 1.5 - 2 часа в зависимости от сложности."),
    ("Как отменить запись?", "Вы можете сделать это через раздел 'Мои записи' не позднее чем за 24 часа."),
    ("Есть ли парковка?", "Да, рядом с домом всегда есть свободные места для парковки.")
]

PHOTO_URLS = [
    f"https://picsum.photos/seed/nails{i}/800/600" for i in range(1, 21)
]

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- DATABASE ---
DB_PATH = os.getenv("DB_PATH", "bot.db")

def db_init():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            name TEXT,
            phone TEXT,
            username TEXT,
            created_at TEXT
        )
    """)
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN username TEXT")
    except:
        pass
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            service TEXT,
            date TEXT,
            time TEXT,
            comment TEXT,
            status TEXT,
            created_at TEXT,
            reminded_24h INTEGER DEFAULT 0,
            reminded_2h INTEGER DEFAULT 0,
            UNIQUE(date, time) ON CONFLICT ABORT
        )
    """)
    # Migration for old tables
    try:
        cursor.execute("ALTER TABLE bookings ADD COLUMN reminded_24h INTEGER DEFAULT 0")
    except: pass
    try:
        cursor.execute("ALTER TABLE bookings ADD COLUMN reminded_2h INTEGER DEFAULT 0")
    except: pass
    # Добавляем индекс для существующих баз данных
    try:
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_bookings_date_time ON bookings(date, time) WHERE status != 'cancelled'")
    except Exception as e:
        logger.warning(f"Could not create unique index: {e}")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            text TEXT,
            created_at TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gallery (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()

def db_get_user(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = cursor.fetchone()
    conn.close()
    return user

def db_save_user(user_id: int, name: str, phone: str, username: str = None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.now(MOSCOW_TZ).isoformat()
    cursor.execute("INSERT OR REPLACE INTO users (user_id, name, phone, username, created_at) VALUES (?, ?, ?, ?, ?)",
                   (user_id, name, phone, username, now))
    conn.commit()
    conn.close()

def db_save_booking(user_id: int, service: str, b_date: str, b_time: str, comment: str = ""):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.now(MOSCOW_TZ).isoformat()
    try:
        cursor.execute("""
            INSERT INTO bookings (user_id, service, date, time, comment, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user_id, service, b_date, b_time, comment, "pending", now))
        booking_id = cursor.lastrowid
        conn.commit()
    except sqlite3.IntegrityError:
        booking_id = None
    finally:
        conn.close()
    return booking_id

def db_update_booking_status(booking_id: int, status: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE bookings SET status = ? WHERE id = ?", (status, booking_id))
    conn.commit()
    conn.close()

def db_get_booking(booking_id: int):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings WHERE id = ?", (booking_id,))
    booking = cursor.fetchone()
    conn.close()
    return booking

def db_get_user_bookings(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    today = date.today().isoformat()
    cursor.execute("""
        SELECT * FROM bookings 
        WHERE user_id = ? AND date >= ? AND status != 'cancelled'
        ORDER BY date ASC, time ASC
    """, (user_id, today))
    bookings = cursor.fetchall()
    conn.close()
    return bookings

def db_save_review(user_id: int, text: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.now(MOSCOW_TZ).isoformat()
    cursor.execute("INSERT INTO reviews (user_id, text, created_at) VALUES (?, ?, ?)", (user_id, text, now))
    conn.commit()
    conn.close()

def db_has_previous_bookings(user_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM bookings WHERE user_id = ? AND status != 'cancelled'", (user_id,))
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0

def db_save_gallery_photo(file_id: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.now(MOSCOW_TZ).isoformat()
    cursor.execute("INSERT INTO gallery (file_id, created_at) VALUES (?, ?)", (file_id, now))
    conn.commit()
    conn.close()

def db_get_gallery_photos():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT file_id FROM gallery ORDER BY id DESC")
    photos = [r[0] for r in cursor.fetchall()]
    conn.close()
    return photos

def db_delete_last_photo():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM gallery WHERE id = (SELECT MAX(id) FROM gallery)")
    conn.commit()
    conn.close()

def db_get_booked_times(date_str: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT time FROM bookings WHERE date = ? AND status != 'cancelled'", (date_str,))
    times = [r[0] for r in cursor.fetchall()]
    conn.close()
    return times

def db_get_latest_reviews(limit: int = 5):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT r.text, u.name 
        FROM reviews r
        JOIN users u ON r.user_id = u.user_id
        ORDER BY r.created_at DESC LIMIT ?
    """, (limit,))
    reviews = cursor.fetchall()
    conn.close()
    return reviews

def db_get_filtered_bookings(days: int, past: bool = False):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    if past:
        # Прошедшие за последние X дней
        cursor.execute("""
            SELECT b.*, u.name, u.phone 
            FROM bookings b
            LEFT JOIN users u ON b.user_id = u.user_id
            WHERE b.date < date('now', 'localtime') 
              AND b.date >= date('now', 'localtime', ?)
            ORDER BY b.date DESC, b.time DESC
        """, (f"-{days} days",))
    else:
        # Будущие на X дней (9999 для "Все")
        cursor.execute("""
            SELECT b.*, u.name, u.phone 
            FROM bookings b
            LEFT JOIN users u ON b.user_id = u.user_id
            WHERE b.date >= date('now', 'localtime') 
              AND b.date <= date('now', 'localtime', ?)
              AND b.status != 'cancelled'
            ORDER BY b.date ASC, b.time ASC
        """, (f"+{days} days",))
    bookings = cursor.fetchall()
    conn.close()
    return bookings

def db_get_stats():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    today = datetime.now(MOSCOW_TZ).date().isoformat()
    
    # Активные (будущие или сегодняшние подтвержденные)
    cursor.execute("SELECT COUNT(*) FROM bookings WHERE status = 'confirmed' AND date >= ?", (today,))
    active_count = cursor.fetchone()[0]
    
    # Завершенные (прошедшие подтвержденные)
    cursor.execute("SELECT service FROM bookings WHERE status = 'confirmed' AND date < ?", (today,))
    completed_bookings = cursor.fetchall()
    completed_count = len(completed_bookings)
    
    # Расчет выручки
    total_revenue = 0
    for (service,) in completed_bookings:
        total_revenue += SERVICE_PRICES.get(service, 2500)
        
    conn.close()
    return active_count, completed_count, total_revenue

# --- HELPERS ---

def format_dt(date_str: str, time_str: str = "") -> str:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")
        if time_str:
            return f"{d} {time_str}"
        return d
    except:
        return f"{date_str} {time_str}".strip()

def normalize_button(text: str) -> str:
    if not text: return None
    t = text.lower().strip()
    
    # Логируем для отладки
    logger.info(f"Normalizing text: '{t}'")
    
    # 1. Прямое сопоставление ключевых слов (самое надежное)
    if any(k in t for k in ["записаться", "запись на"]): return "book"
    if any(k in t for k in ["цены", "прайс", "стоимость"]): return "prices"
    if any(k in t for k in ["обо мне", "мастер", "кто ты"]): return "about"
    if any(k in t for k in ["найти", "адрес", "локация", "где вы"]): return "location"
    if any(k in t for k in ["мои записи", "моя запись", "записи"]): 
        # Важно: "записи" может быть в "мои записи", но мы проверяем это после "записаться"
        if "записаться" not in t:
            return "my_bookings"
    if any(k in t for k in ["отзывы", "фидбек", "мнения"]): return "reviews"
    if any(k in t for k in ["админ", "поддержка", "связь"]): return "admin"
    if any(k in t for k in ["вопросы", "ответы", "faq", "помощь", "инфо"]): return "faq"
    if any(k in t for k in ["рекомендация", "советы", "уход"]): return "recommendation"
    if any(k in t for k in ["меню", "главная", "старт"]): return "menu"
    if "назад" in t: return "back"
    
    # 2. Очистка от спецсимволов и эмодзи и повторная проверка
    clean = re.sub(r'[^\w\s]', '', t).strip()
    if clean:
        if any(k in clean for k in ["записаться", "запись"]): return "book"
        if any(k in clean for k in ["цены", "прайс"]): return "prices"
        if "обо мне" in clean: return "about"
        if any(k in clean for k in ["найти", "адрес"]): return "location"
        if "мои записи" in clean or "моизаписи" in clean: return "my_bookings"
        if "отзывы" in clean: return "reviews"
        if "админ" in clean: return "admin"
        if any(k in clean for k in ["вопросы", "ответы", "faq"]): return "faq"
        if "рекомендация" in clean: return "recommendation"
        if "меню" in clean: return "menu"
        if "назад" in clean: return "back"
    
    return None

async def track_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    if not AUTO_CLEAN: return
    if "msg_history" not in context.user_data:
        context.user_data["msg_history"] = []
    context.user_data["msg_history"].append(message_id)
    # Keep only last 6 messages
    if len(context.user_data["msg_history"]) > 8:
        to_delete = context.user_data["msg_history"][:-6]
        context.user_data["msg_history"] = context.user_data["msg_history"][-6:]
        for mid in to_delete:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except:
                pass

async def safe_send(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None, **kwargs):
    chat_id = update.effective_chat.id
    msg = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode="HTML", **kwargs)
    await track_message(context, chat_id, msg.message_id)
    return msg

# --- PHOTO GALLERY ---

async def send_gallery(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    db_photos = db_get_gallery_photos()
    all_photos = db_photos if db_photos else PHOTO_URLS
    
    if not all_photos:
        await safe_send(update, context, "Фото скоро добавлю! 📸")
        return
    
    page = page % len(all_photos)
    url_or_id = all_photos[page]
    
    kb = [
        [
            InlineKeyboardButton("◀️ Назад", callback_data=f"gal_{page-1}"),
            InlineKeyboardButton(f"{page+1} / {len(all_photos)}", callback_data="noop"),
            InlineKeyboardButton("Вперед ▶️", callback_data=f"gal_{page+1}")
        ],
        [InlineKeyboardButton("🏠 В меню", callback_data="to_menu")]
    ]
    
    try:
        if update.callback_query and update.callback_query.message.photo:
            await update.callback_query.edit_message_media(
                media=InputMediaPhoto(url_or_id, caption="Мои работы:"),
                reply_markup=InlineKeyboardMarkup(kb)
            )
        else:
            msg = await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=url_or_id,
                caption="Мои работы:",
                reply_markup=InlineKeyboardMarkup(kb)
            )
            await track_message(context, update.effective_chat.id, msg.message_id)
    except Exception as e:
        logger.error(f"Gallery error: {e}")
        await safe_send(update, context, "Ошибка при загрузке фото.")

async def send_notification(context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    """Отправляет уведомление в чат уведомлений или админу."""
    if NOTIFICATION_CHAT_ID == 0:
        print("WARNING: NOTIFICATION_CHAT_ID is 0. Cannot send notification.")
        return

    try:
        await context.bot.send_message(
            chat_id=NOTIFICATION_CHAT_ID,
            text=text,
            reply_markup=reply_markup,
            parse_mode="HTML"
        )
    except Exception as e:
        error_msg = str(e)
        print(f"Error sending notification to {NOTIFICATION_CHAT_ID}: {error_msg}")
        
        # Если не удалось отправить в группу, пробуем отправить админу напрямую
        if ADMIN_ID != 0 and NOTIFICATION_CHAT_ID != ADMIN_ID:
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"⚠️ <b>Ошибка уведомления в чат {NOTIFICATION_CHAT_ID}:</b>\n<code>{error_msg}</code>\n\n{text}",
                    reply_markup=reply_markup,
                    parse_mode="HTML"
                )
            except Exception as e2:
                print(f"Error sending emergency notification to admin {ADMIN_ID}: {e2}")

# --- KEYBOARDS ---

def get_main_menu_keyboard(user_id: int):
    buttons = [
        [KeyboardButton("📅 Записаться"), KeyboardButton("💰 Цены")],
        [KeyboardButton("👩🎨 Обо мне"), KeyboardButton("📍 Как нас найти")],
        [KeyboardButton("📋 Мои записи"), KeyboardButton("⭐ Отзывы")],
        [KeyboardButton("❓ Вопросы и ответы")]
    ]
    if user_id == ADMIN_ID:
        buttons.append([KeyboardButton("🛠 Админ панель")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_services_keyboard():
    keyboard = [
        [InlineKeyboardButton("💅 Маникюр + покрытие", callback_data="svc_man_full")],
        [InlineKeyboardButton("✨ Маникюр без покрытия", callback_data="svc_man_simple"),
         InlineKeyboardButton("🎨 Маникюр + дизайн", callback_data="svc_man_design")],
        [InlineKeyboardButton("💖 Педикюр + покрытие", callback_data="svc_ped_full")],
        [InlineKeyboardButton("🦶 Педикюр без покрытия", callback_data="svc_ped_simple"),
         InlineKeyboardButton("👣 Педикюр (пальчики)", callback_data="svc_ped_fingers")],
        [InlineKeyboardButton("✨ Наращивание", callback_data="svc_ext"),
         InlineKeyboardButton("🔧 Коррекция", callback_data="svc_corr")],
        [InlineKeyboardButton("🏠 В меню", callback_data="to_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_calendar_keyboard(year: int, month: int):
    now = datetime.now(MOSCOW_TZ)
    
    month_names = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", 
                   "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
    header = f"{month_names[month-1]} {year}"
    
    keyboard = []
    keyboard.append([InlineKeyboardButton(header, callback_data="noop")])
    keyboard.append([InlineKeyboardButton(d, callback_data="noop") for d in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]])
    
    cal = calendar.monthcalendar(year, month)
    for week in cal:
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="noop"))
            else:
                d_obj = date(year, month, day)
                if d_obj < now.date():
                    row.append(InlineKeyboardButton("❌", callback_data="noop_past"))
                else:
                    row.append(InlineKeyboardButton(str(day), callback_data=f"date_{year}_{month}_{day}"))
        keyboard.append(row)
    
    prev_m = month - 1 if month > 1 else 12
    prev_y = year if month > 1 else year - 1
    next_m = month + 1 if month < 12 else 1
    next_y = year if month < 12 else year + 1
    
    nav_row = [
        InlineKeyboardButton("◀️", callback_data=f"cal_{prev_y}_{prev_m}"),
        InlineKeyboardButton("Сегодня", callback_data=f"cal_{now.year}_{now.month}"),
        InlineKeyboardButton("▶️", callback_data=f"cal_{next_y}_{next_m}")
    ]
    keyboard.append(nav_row)
    keyboard.append([InlineKeyboardButton("⬅️ Назад к услугам", callback_data="book_start")])
    
    return InlineKeyboardMarkup(keyboard)

def get_time_keyboard(b_date_str: str):
    now = datetime.now(MOSCOW_TZ)
    b_date = date.fromisoformat(b_date_str)
    booked_times = db_get_booked_times(b_date_str)
    
    keyboard = []
    start_h = 8
    end_h = 23
    
    times_data = []
    for h in range(start_h, end_h):
        for m in [0, 30]:
            t_str = f"{h:02d}:{m:02d}"
            if b_date == now.date():
                if h < now.hour or (h == now.hour and m <= now.minute):
                    continue
            
            if t_str in booked_times:
                times_data.append((f"❌ {t_str}", "noop_booked"))
            else:
                times_data.append((t_str, f"time_{t_str}"))
            
    for i in range(0, len(times_data), 4):
        row = [InlineKeyboardButton(text, callback_data=cb) for text, cb in times_data[i:i+4]]
        keyboard.append(row)
        
    keyboard.append([InlineKeyboardButton("⬅️ Назад к календарю", callback_data="back_to_cal")])
    return InlineKeyboardMarkup(keyboard)

def get_confirm_keyboard():
    keyboard = [
        [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_booking")],
        [InlineKeyboardButton("✏️ Комментарий", callback_data="add_comment")],
        [InlineKeyboardButton("❌ Отказаться", callback_data="cancel_booking_step")]
    ]
    return InlineKeyboardMarkup(keyboard)

# --- HANDLERS ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    user_id = update.effective_user.id
    user = db_get_user(user_id)
    
    welcome_text = (
        f"👋 Добро пожаловать в <b>{SALON_TITLE}</b>!\n\n"
        "Я ваш личный помощник для записи на процедуры.\n"
        "Здесь вы можете выбрать удобное время, посмотреть цены и оставить отзыв.\n\n"
        "🎁 <b>Акция!</b> Скидка <b>7%</b> на первую запись через бота!\n"
        "<i>(Акция действует 1 раз до 04.04.2026)</i>\n\n"
    )
    
    if not user:
        welcome_text += "Для начала работы, пожалуйста, пройдите короткую регистрацию."
        if ADMIN_ID == 0:
            welcome_text += (
                "\n\n⚠️ <b>Внимание:</b> Бот еще не настроен.\n"
                f"Ваш ID: <code>{user_id}</code>\n"
                "Добавьте его в переменную <code>ADMIN_ID</code> в Railway."
            )
        await safe_send(update, context, welcome_text, reply_markup=get_main_menu_keyboard(user_id))
        await safe_send(update, context, "Как мне к вам обращаться? Введите ваше имя:")
        context.user_data["mode"] = "await_name"
    else:
        welcome_text += f"Рады видеть вас снова, {user[1]}!"
        await safe_send(update, context, welcome_text, reply_markup=get_main_menu_keyboard(user_id))
    
    if update.message:
        await track_message(context, update.effective_chat.id, update.message.message_id)

async def on_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        try: await update.message.delete()
        except: pass
        return
    if context.user_data.get("mode") != "await_phone": 
        try: await update.message.delete()
        except: pass
        return
    
    contact = update.message.contact
    phone = contact.phone_number
    name = context.user_data.get("reg_name", update.effective_user.first_name)
    
    db_save_user(update.effective_user.id, name, phone)
    context.user_data["mode"] = None
    
    await safe_send(update, context, f"✅ Регистрация завершена!\nПриятно познакомиться, {name}.\n\nНажмите 📅 <b>Записаться</b>, чтобы выбрать время.", reply_markup=get_main_menu_keyboard(update.effective_user.id))
    await track_message(context, update.effective_chat.id, update.message.message_id)

async def on_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        try: await update.message.delete()
        except: pass
        return
    if update.effective_user.id != ADMIN_ID: 
        try: await update.message.delete()
        except: pass
        return
    if context.user_data.get("mode") != "admin_add_photo": 
        try: await update.message.delete()
        except: pass
        return
    
    try:
        photo = update.message.photo[-1] # Best quality
        db_save_gallery_photo(photo.file_id)
        
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Готово", callback_data="adm_gal_manage")]])
        await safe_send(update, context, "✅ Фото добавлено! Вы можете отправить еще или нажать «Готово».", reply_markup=kb)
    except Exception as e:
        logger.error(f"Error saving photo: {e}")
        await safe_send(update, context, "❌ Ошибка при сохранении фото. Попробуйте еще раз.")

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat = update.effective_chat
        user_id = update.effective_user.id
        text = update.message.text
        
        if chat.type != "private":
            if not text.startswith('/id'):
                try: await update.message.delete()
                except: pass
            return

        norm = normalize_button(text)
        
        # Fallback для кнопок, если normalize_button не сработал
        if not norm:
            t_low = text.lower()
            if "мои записи" in t_low: norm = "my_bookings"
            elif "вопросы" in t_low or "ответы" in t_low: norm = "faq"
            elif "записаться" in t_low: norm = "book"
            elif "цены" in t_low: norm = "prices"
            elif "отзывы" in t_low: norm = "reviews"
            elif "обо мне" in t_low: norm = "about"
            elif "найти" in t_low: norm = "location"

        mode = context.user_data.get("mode")
        
        await track_message(context, chat.id, update.message.message_id)
        
        # 1. Если нажата кнопка меню
        if norm:
            context.user_data["mode"] = None
            
            if norm == "book":
                user = db_get_user(user_id)
                if not user:
                    await safe_send(update, context, "Для записи нужно сначала представиться. Как мне к вам обращаться?")
                    context.user_data["mode"] = "await_name"
                    return
                await safe_send(update, context, "Выберите услугу:", reply_markup=get_services_keyboard())
                return
            elif norm == "prices":
                await prices_command(update, context)
                return
            elif norm == "about":
                about_text = (
                    "<b>✨ Искусство преображения ваших рук ✨</b>\n\n"
                    "Привет! Я Ирина — топ-мастер с 7-летним стажем, который влюблен в свое дело. "
                    "Моя миссия — не просто сделать маникюр, а подчеркнуть вашу индивидуальность и подарить уверенность в себе.\n\n"
                    "<b>Почему выбирают меня:</b>\n"
                    "💎 <b>Безопасность:</b> Стерилизация по медицинским стандартам (СанПиН). Крафт-пакет вскрываю только при вас.\n"
                    "🎨 <b>Качество:</b> Работаю на премиум-материалах, которые носятся без сколов до 4-х недель.\n"
                    "☕️ <b>Комфорт:</b> Уютная студия, любимые сериалы, ароматный кофе и время только для себя.\n\n"
                    "<i>Ваши руки заслуживают лучшего ухода!</i>"
                )
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📷 Фотогалерея", callback_data="show_gallery")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="to_menu")]
                ])
                await safe_send(update, context, about_text, reply_markup=kb)
                return
            elif norm == "location":
                loc_text = (
                    "<b>📍 Как нас найти:</b>\n\n"
                    f"🏠 Адрес: {ADDRESS_TEXT}\n"
                    "Ориентир: 10 этаж, направо от лифта.\n\n"
                    f"🔗 <a href='{MAPS_URL}'>Открыть на картах</a>"
                )
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="to_menu")]])
                await safe_send(update, context, loc_text, reply_markup=kb)
                return
            elif norm == "my_bookings":
                try:
                    bookings = db_get_user_bookings(user_id)
                    if not bookings:
                        await safe_send(update, context, "У вас пока нет активных записей.")
                    else:
                        await safe_send(update, context, "<b>📋 Ваши активные записи:</b>")
                        for b in bookings:
                            # b[0] = id, b[1] = user_id, b[2] = service, b[3] = date, b[4] = time, b[5] = comment, b[6] = status
                            status_emoji = "⏳" if b[6] == "pending" else "✅"
                            f_dt = format_dt(b[3], b[4])
                            b_text = (
                                f"{status_emoji} <b>{b[2]}</b>\n"
                                f"📅 {f_dt}\n"
                                f"Статус: {'Ожидает подтверждения' if b[6] == 'pending' else 'Подтверждена'}\n"
                            )
                            kb = InlineKeyboardMarkup([
                                [InlineKeyboardButton("❌ Отменить запись", callback_data=f"cancel_b_{b[0]}")],
                                [
                                    InlineKeyboardButton("💬 Связаться с мастером", url=f"https://t.me/{MASTER_TG}")
                                ]
                            ])
                            await safe_send(update, context, b_text, reply_markup=kb)
                except Exception as e:
                    logger.error(f"Error in my_bookings: {e}", exc_info=True)
                    await safe_send(update, context, f"❌ Произошла ошибка при получении записей. Попробуйте позже. ({e})")
                return
            elif norm == "reviews":
                reviews = db_get_latest_reviews()
                text = "<b>⭐ Последние отзывы:</b>\n\n"
                if not reviews:
                    text += "Отзывов пока нет. Будьте первыми!"
                else:
                    for r in reviews:
                        text += f"👤 {r[1]}:\n«{r[0]}»\n\n"
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✍️ Оставить отзыв", callback_data="add_review")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="to_menu")]
                ])
                await safe_send(update, context, text, reply_markup=kb)
                return
            elif norm == "admin" and user_id == ADMIN_ID:
                await show_admin_filters(update, context)
                return
            elif norm == "faq":
                await faq_command(update, context)
                return
            elif norm == "menu":
                await menu_command(update, context)
                return
            elif norm == "recommendation":
                await recommendation_command(update, context)
                return

        # 2. Handle Input Modes
        if mode == "await_name":
            if len(text) > 40:
                await safe_send(update, context, "Слишком длинное имя. Попробуйте еще раз:")
                return
            context.user_data["reg_name"] = text
            context.user_data["mode"] = "await_phone"
            kb = ReplyKeyboardMarkup([[KeyboardButton("📲 Отправить номер", request_contact=True)]], resize_keyboard=True)
            await safe_send(update, context, f"Приятно познакомиться, {text}! Теперь введите ваш номер телефона или нажмите кнопку ниже:", reply_markup=kb)
            return
        
        elif mode == "await_phone":
            clean_phone = re.sub(r'[^\d+]', '', text)
            if len(clean_phone) < 10 or len(clean_phone) > 13:
                await safe_send(update, context, "❌ Неверный формат номера. Попробуйте еще раз (например, +79991234567):")
                return
            
            name = context.user_data.get("reg_name", update.effective_user.first_name)
            username = update.effective_user.username
            db_save_user(user_id, name, clean_phone, username)
            context.user_data["mode"] = None
            await safe_send(update, context, f"✅ Регистрация завершена!\nПриятно познакомиться, {name}.", reply_markup=get_main_menu_keyboard(user_id))
            
            admin_msg = (
                f"👤 <b>Новый пользователь зарегистрирован!</b>\n\n"
                f"Имя: {name}\n"
                f"Телефон: {clean_phone}\n"
                f"Username: @{username if username else 'отсутствует'}\n"
            )
            await send_notification(context, admin_msg)
            return

        elif mode == "await_comment":
            context.user_data["b_comment"] = text
            context.user_data["mode"] = None
            await show_booking_summary(update, context)
            return

        elif mode == "await_review":
            db_save_review(user_id, text)
            context.user_data["mode"] = None
            await safe_send(update, context, "🙏 Спасибо за ваш отзыв! Он появится в списке после обновления.")
            return

        elif mode == "admin_msg":
            target_user_id = context.user_data.get("admin_target_user")
            if target_user_id:
                try:
                    await context.bot.send_message(chat_id=target_user_id, text=f"💬 <b>Сообщение от мастера:</b>\n\n{text}", parse_mode="HTML")
                    await safe_send(update, context, "✅ Сообщение отправлено клиенту.")
                except:
                    await safe_send(update, context, "❌ Не удалось отправить сообщение.")
            context.user_data["mode"] = None
            return

        # 3. Анти-спам
        if not norm and not mode:
            # Если текст похож на кнопку, но не распознан - даем подсказку
            t_low = text.lower()
            if any(k in t_low for k in ["запис", "вопрос", "ответ", "цен", "отзыв", "меню"]):
                await safe_send(update, context, "🤖 Я не совсем понял команду. Пожалуйста, используйте кнопки меню ниже.")
            else:
                try: await update.message.delete()
                except: pass

    except Exception as e:
        logger.error(f"Error in on_text: {e}", exc_info=True)
        try:
            await safe_send(update, context, "⚠️ Произошла ошибка при обработке сообщения. Пожалуйста, попробуйте еще раз или используйте /start.")
        except:
            pass

async def recommendation_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "<b>💅 Рекомендации по уходу:</b>\n\n"
        "1. Используйте масло для кутикулы ежедневно.\n"
        "2. Надевайте перчатки при работе с бытовой химией.\n"
        "3. Не используйте ногти как инструмент (не открывайте ими банки).\n"
        "4. Обновляйте покрытие каждые 3-4 недели.\n"
        "5. Не снимайте покрытие самостоятельно!"
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="to_menu")]])
    await safe_send(update, context, text, reply_markup=kb)

async def prices_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    prices_text = (
        "<b>💰 Наши цены:</b>\n\n"
        "✨ Маникюр без покрытия — 1300 ₽\n"
        "💅 Маникюр с покрытием — 2500 ₽\n"
        "🎨 Маникюр с покрытием + дизайн — 3000 ₽\n\n"
        "🦶 Педикюр без покрытия — 2000 ₽\n"
        "💖 Педикюр + покрытие — 2800 ₽\n"
        "👣 Педикюр пальчики — 1800 ₽\n"
        "🦶 Обработка стоп — 1500 ₽\n\n"
        "✨ Наращивание ногтей — от 3500 ₽\n"
        "🔧 Коррекция ногтей — от 2800 ₽\n"
        "🎨 Дизайн — от 50 ₽ / ноготь\n\n"
        "<i>Нажмите кнопку ниже для записи</i>"
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("📅 Записаться", callback_data="book_start")]])
    await safe_send(update, context, prices_text, reply_markup=kb)

async def faq_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "<b>❓ Часто задаваемые вопросы:</b>\n\n"
    for i, (q, a) in enumerate(FAQ_DATA, 1):
        text += f"<b>{i}. {q}</b>\n— {a}\n\n"
    
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📱 SMS", url=f"sms:{MASTER_PHONE}"),
            InlineKeyboardButton("💬 Telegram", url=f"https://t.me/{MASTER_TG}")
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="to_menu")]
    ])
    await safe_send(update, context, text, reply_markup=kb)

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = db_get_user(user_id)
    if user:
        await safe_send(update, context, "Главное меню:", reply_markup=get_main_menu_keyboard(user_id))
    else:
        await start(update, context)

async def show_admin_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    active, completed, revenue = db_get_stats()
    
    text = (
        "🛠 <b>Админ-панель</b>\n\n"
        "📊 <b>Статистика:</b>\n"
        f"✅ Активные записи: {active}\n"
        f"🏁 Завершенные: {completed}\n"
        f"💰 Выручка (заверш.): {revenue} ₽\n\n"
        "Выберите период для просмотра записей или управление контентом:"
    )
    kb = [
        [InlineKeyboardButton("📅 На 7 дней", callback_data="adm_view_7"),
         InlineKeyboardButton("📅 На 14 дней", callback_data="adm_view_14")],
        [InlineKeyboardButton("📅 Все записи", callback_data="adm_view_9999")],
        [InlineKeyboardButton("🕒 Прошедшие (7 дней)", callback_data="adm_view_past_7")],
        [InlineKeyboardButton("🖼 Управление галереей", callback_data="adm_gallery")],
        [InlineKeyboardButton("🏠 В меню", callback_data="to_menu")]
    ]
    await safe_send(update, context, text, reply_markup=InlineKeyboardMarkup(kb))

async def show_gallery_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    photos = db_get_gallery_photos()
    text = f"🖼 <b>Управление галереей</b>\n\nВсего фото в базе: {len(photos)}"
    kb = [
        [InlineKeyboardButton("➕ Добавить фото", callback_data="adm_gal_add")],
        [InlineKeyboardButton("🗑 Удалить последнее", callback_data="adm_gal_del")],
        [InlineKeyboardButton("⬅️ Назад в админку", callback_data="adm_back")]
    ]
    await safe_send(update, context, text, reply_markup=InlineKeyboardMarkup(kb))

async def show_booking_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data
    user_id = update.effective_user.id
    user = db_get_user(user_id)
    
    is_first = not db_has_previous_bookings(user_id)
    now_date = datetime.now(MOSCOW_TZ).date()
    can_apply_discount = is_first and now_date <= DISCOUNT_END_DATE
    
    service = data.get('b_service')
    base_price = SERVICE_PRICES.get(service, 0)
    final_price = base_price
    
    f_dt = format_dt(data.get('b_date'), data.get('b_time'))
    
    summary = (
        "<b>🏁 Подтверждение записи:</b>\n\n"
        f"💅 Услуга: {service}\n"
        f"📅 Дата и время: {f_dt}\n"
        f"👤 Имя: {user[1]}\n"
        f"📞 Тел: {user[2]}\n"
    )
    
    if can_apply_discount:
        discount_amount = int(base_price * DISCOUNT_PERCENT / 100)
        final_price = base_price - discount_amount
        summary += f"\n💰 Стоимость: <s>{base_price}</s> <b>{final_price} ₽</b> (-7%)\n"
        summary += "🎁 <b>Акция:</b> Ваша первая запись через бота! ✨\n"
    else:
        summary += f"\n💰 Стоимость: <b>{final_price} ₽</b>\n"
        
    if data.get("b_comment"):
        summary += f"\n💬 Комментарий: {data.get('b_comment')}\n"
    
    await safe_send(update, context, summary, reply_markup=get_confirm_keyboard())

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = update.effective_user.id
    
    await query.answer()
    
    if data == "noop": return
    if data == "noop_past":
        await query.answer("Эта дата уже прошла или недоступна", show_alert=True)
        return
    if data == "noop_booked":
        await query.answer("Это время уже занято. Пожалуйста, выберите другое.", show_alert=True)
        return

    if data == "to_menu":
        context.user_data["mode"] = None
        await safe_send(update, context, "Главное меню:", reply_markup=get_main_menu_keyboard(user_id))
    
    elif data == "cancel_booking_step":
        context.user_data.clear()
        await query.edit_message_text("❌ Вы отказались от записи. Если передумаете — мы всегда рады вам! 😊")
        await safe_send(update, context, "Главное меню:", reply_markup=get_main_menu_keyboard(user_id))
    
    elif data == "book_start":
        await query.edit_message_text("Выберите услугу:", reply_markup=get_services_keyboard())
    
    elif data == "show_gallery":
        await send_gallery(update, context)
    
    elif data.startswith("svc_"):
        services = {
            "svc_man_full": "Маникюр с покрытием",
            "svc_man_simple": "Маникюр без покрытия",
            "svc_man_design": "Маникюр + дизайн",
            "svc_ped_full": "Педикюр с покрытием",
            "svc_ped_simple": "Педикюр без покрытия",
            "svc_ped_fingers": "Педикюр (пальчики)",
            "svc_ext": "Наращивание",
            "svc_corr": "Коррекция"
        }
        context.user_data["b_service"] = services.get(data)
        now = datetime.now(MOSCOW_TZ)
        await query.edit_message_text("Выберите дату:", reply_markup=get_calendar_keyboard(now.year, now.month))
    
    elif data.startswith("cal_"):
        _, y, m = data.split("_")
        await query.edit_message_text("Выберите дату:", reply_markup=get_calendar_keyboard(int(y), int(m)))
        
    elif data.startswith("date_"):
        _, y, m, d = data.split("_")
        b_date = f"{y}-{int(m):02d}-{int(d):02d}"
        context.user_data["b_date"] = b_date
        f_date = format_dt(b_date)
        await query.edit_message_text(f"Выбрана дата: {f_date}\nВыберите время:", reply_markup=get_time_keyboard(b_date))
        
    elif data == "back_to_cal":
        now = datetime.now(MOSCOW_TZ)
        await query.edit_message_text("Выберите дату:", reply_markup=get_calendar_keyboard(now.year, now.month))
        
    elif data.startswith("time_"):
        context.user_data["b_time"] = data.split("_")[1]
        await query.delete_message()
        await show_booking_summary(update, context)
        
    elif data == "add_comment":
        context.user_data["mode"] = "await_comment"
        await safe_send(update, context, "Введите ваш комментарий к записи (или любую reply-кнопку для отмены):")
        
    elif data == "confirm_booking":
        user_id = update.effective_user.id
        b_date = context.user_data.get("b_date")
        b_time = context.user_data.get("b_time")
        service = context.user_data.get("b_service")
        
        if not b_date or not b_time:
            await query.answer("⚠️ Сессия устарела. Начните запись заново.", show_alert=True)
            return

        # Сразу убираем кнопки, чтобы нельзя было нажать дважды
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except:
            pass

        # Проверка на занятость (защита от одновременных кликов)
        booked_times = db_get_booked_times(b_date)
        if b_time in booked_times:
            await query.answer("⚠️ Это время уже занято.", show_alert=True)
            await query.edit_message_text("😔 К сожалению, это время уже занято. Пожалуйста, выберите другое:", reply_markup=get_time_keyboard(b_date))
            return

        is_first = not db_has_previous_bookings(user_id)
        now_date = datetime.now(MOSCOW_TZ).date()
        applied_discount = is_first and now_date <= DISCOUNT_END_DATE
        
        base_price = SERVICE_PRICES.get(service, 0)
        final_price = base_price
        if applied_discount:
            final_price = base_price - int(base_price * DISCOUNT_PERCENT / 100)
        
        b_id = db_save_booking(
            user_id, 
            service,
            b_date,
            b_time,
            context.user_data.get("b_comment", "")
        )
        
        if b_id is None:
            await query.answer("⚠️ К сожалению, это время только что заняли.", show_alert=True)
            await query.edit_message_text(
                "😔 К сожалению, это время только что заняли.\n\n"
                "Пожалуйста, выберите другое время или другой день:", 
                reply_markup=get_time_keyboard(b_date)
            )
            return
        
        # Обновляем сообщение резюме, превращая его в сообщение об успехе
        success_msg = f"\n\n✅ <b>Запись создана!</b>\nОжидайте подтверждения мастера.\n💰 К оплате: <b>{final_price} ₽</b>"
        if applied_discount:
            success_msg += "\n🎁 Скидка 7% на первый визит применена!"
        
        success_msg += "\n\n📍 Управлять записями можно в разделе <b>Мои записи</b>."
        
        await query.edit_message_text(f"{query.message.text_html}{success_msg}", parse_mode="HTML")
        
        user = db_get_user(user_id)
        f_dt = format_dt(b_date, b_time)
        
        user_link = f"@{user[3]}" if user[3] else f"<a href='tg://user?id={user_id}'>{user[1]}</a>"
        
        admin_text = (
            f"<b>🆕 Новая запись!</b>\n"
            f"{'🎁 АКЦИЯ: ПЕРВЫЙ ВИЗИТ (-7%)' if applied_discount else ''}\n\n"
            f"👤 Клиент: {user[1]} ({user[2]}) {user_link}\n"
            f"💅 Услуга: {service}\n"
            f"📅 Дата и время: {f_dt}\n"
            f"💰 Сумма: {final_price} ₽\n"
        )
        if context.user_data.get("b_comment"):
            admin_text += f"💬 Комментарий: {context.user_data.get('b_comment')}\n"
            
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить", callback_data=f"adm_conf_{b_id}"),
             InlineKeyboardButton("❌ Отклонить", callback_data=f"adm_rejc_{b_id}")],
            [InlineKeyboardButton("💬 Написать клиенту", callback_data=f"adm_msg_{user_id}")]
        ])
        await send_notification(context, admin_text, reply_markup=kb)
        context.user_data.clear()

    elif data.startswith("gal_"):
        page = int(data.split("_")[1])
        await send_gallery(update, context, page)
        
    if data.startswith("adm_"):
        # Разрешаем кликать админу ИЛИ любому участнику в чате уведомлений
        if user_id != ADMIN_ID and query.message.chat_id != NOTIFICATION_CHAT_ID:
            await query.answer("❌ У вас нет прав администратора.", show_alert=True)
            return

    if data.startswith("adm_view_"):
        parts = data.split("_")
        is_past = "past" in parts
        days = int(parts[-1])
        bookings = db_get_filtered_bookings(days, is_past)
        
        title = "Прошедшие" if is_past else "Будущие"
        if not bookings:
            await safe_send(update, context, f"🛠 <b>Админ-панель</b>\n\n{title} записей на {days} дн. не найдено.")
        else:
            await safe_send(update, context, f"🛠 <b>Админ-панель</b>\n{title} записей ({days if days < 9000 else 'все'} дн.): {len(bookings)}")
            for b in bookings:
                status_emoji = "⏳" if b[6] == "pending" else "✅"
                f_dt = format_dt(b[3], b[4])
                text = (
                    f"{status_emoji} <b>Запись #{b[0]}</b>\n"
                    f"👤 Клиент: {b[9] if b[9] else 'Неизвестно'} ({b[10] if b[10] else 'нет номера'})\n"
                    f"💅 Услуга: {b[2]}\n"
                    f"📅 Дата: {f_dt}\n"
                    f"💬: {b[5] if b[5] else 'нет'}\n"
                )
                kb = []
                if b[6] == "pending":
                    kb.append([InlineKeyboardButton("✅ Подтвердить", callback_data=f"adm_conf_{b[0]}")])
                if b[6] != "cancelled":
                    kb.append([InlineKeyboardButton("❌ Отменить", callback_data=f"adm_rejc_{b[0]}")])
                kb.append([InlineKeyboardButton("💬 Написать", callback_data=f"adm_msg_{b[1]} text")])
                await safe_send(update, context, text, reply_markup=InlineKeyboardMarkup(kb))

    elif data == "adm_back":
        await show_admin_filters(update, context)

    elif data == "adm_gallery":
        await show_gallery_admin(update, context)
        
    elif data == "adm_gal_add":
        context.user_data["mode"] = "admin_add_photo"
        await safe_send(update, context, "📸 Пожалуйста, пришлите боту фотографию (или несколько), которую хотите добавить в галерею.")
        
    elif data == "adm_gal_manage":
        context.user_data["mode"] = None
        await show_gallery_admin(update, context)
        
    elif data == "adm_gal_del":
        db_delete_last_photo()
        await query.answer("Последнее фото удалено", show_alert=True)
        await show_gallery_admin(update, context)

    elif data.startswith("adm_conf_"):
        b_id = int(data.split("_")[2])
        db_update_booking_status(b_id, "confirmed")
        booking = db_get_booking(b_id)
        f_dt = format_dt(booking[3], booking[4])
        await query.edit_message_text(query.message.text + "\n\n✅ ПОДТВЕРЖДЕНО")
        
        conf_msg = (
            f"✅ Ваша запись на <b>{f_dt}</b> подтверждена мастером! Ждем вас.\n\n"
            f"📍 <b>Как нас найти:</b>\n{ADDRESS_TEXT}\n"
            f"🔗 <a href='{MAPS_URL}'>Открыть на картах</a>"
        )
        await context.bot.send_message(chat_id=booking[1], text=conf_msg, parse_mode="HTML")
        
    elif data.startswith("adm_rejc_"):
        b_id = int(data.split("_")[2])
        db_update_booking_status(b_id, "cancelled")
        booking = db_get_booking(b_id)
        f_dt = format_dt(booking[3], booking[4])
        await query.edit_message_text(query.message.text + "\n\n❌ ОТКЛОНЕНО")
        await context.bot.send_message(chat_id=booking[1], text=f"❌ К сожалению, мастер не может принять вас <b>{f_dt}</b>. Попробуйте выбрать другое время.", parse_mode="HTML")

    elif data.startswith("adm_msg_"):
        target_id = int(data.split("_")[2])
        context.user_data["mode"] = "admin_msg"
        context.user_data["admin_target_user"] = target_id
        await safe_send(update, context, "Введите сообщение клиенту:")

    elif data.startswith("cancel_b_"):
        try:
            b_id = int(data.split("_")[2])
            booking = db_get_booking(b_id)
            if not booking:
                await query.edit_message_text("❌ Запись не найдена.")
                return
                
            if booking[1] != user_id:
                await query.edit_message_text("❌ Это не ваша запись.")
                return

            if booking[6] == "cancelled":
                await query.edit_message_text("❌ Запись уже отменена.")
                return
                
            # Check 24h rule
            try:
                booking_dt = datetime.strptime(f"{booking[3]} {booking[4]}", "%Y-%m-%d %H:%M")
                booking_dt = MOSCOW_TZ.localize(booking_dt)
                now = datetime.now(MOSCOW_TZ)
                
                if (booking_dt - now) < timedelta(hours=24):
                    await query.edit_message_text(
                        f"⚠️ <b>Отмена невозможна.</b>\n\n"
                        f"До записи на {format_dt(booking[3], booking[4])} осталось менее 24 часов.\n"
                        "Пожалуйста, свяжитесь с мастером напрямую для решения вопроса.",
                        parse_mode="HTML"
                    )
                    return
            except Exception as e:
                logger.error(f"Error checking cancellation time: {e}")

            db_update_booking_status(b_id, "cancelled")
            await query.edit_message_text("✅ Запись успешно отменена.")
            
            user = db_get_user(user_id)
            f_dt = format_dt(booking[3], booking[4])
            
            user_link = f"@{user[3]}" if user[3] else f"<a href='tg://user?id={user_id}'>{user[1]}</a>"
            
            admin_cancel_msg = (
                f"⚠️ <b>Клиент отменил запись!</b>\n\n"
                f"👤 Клиент: {user[1]} ({user[2]}) {user_link}\n"
                f"💅 Услуга: {booking[2]}\n"
                f"📅 Дата: {f_dt}"
            )
            await send_notification(context, admin_cancel_msg)
        except Exception as e:
            logger.error(f"Error in cancel_b: {e}")
            await query.edit_message_text("❌ Произошла ошибка при отмене записи.")

    elif data == "add_review":
        context.user_data["mode"] = "await_review"
        await safe_send(update, context, "Напишите ваш отзыв:")

# --- JOBS ---

async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(MOSCOW_TZ)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Напоминание за 24 часа (строго)
    # Ищем записи, до которых осталось от 23 до 25 часов
    cursor.execute("""
        SELECT * FROM bookings 
        WHERE status = 'confirmed' AND reminded_24h = 0
    """)
    bookings = cursor.fetchall()
    
    for b in bookings:
        try:
            b_dt = datetime.strptime(f"{b[3]} {b[4]}", "%Y-%m-%d %H:%M")
            b_dt = MOSCOW_TZ.localize(b_dt)
            diff = b_dt - now
            
            if timedelta(hours=23) <= diff <= timedelta(hours=25):
                f_dt = format_dt(b[3], b[4])
                text = (
                    "🔔 <b>Напоминание: ваша запись завтра!</b>\n\n"
                    f"💅 Услуга: {b[2]}\n"
                    f"📅 Дата и время: {f_dt}\n"
                    f"🏠 Адрес: {ADDRESS_TEXT}\n"
                    f"🔗 <a href='{MAPS_URL}'>Открыть на картах</a>\n\n"
                    "Ждем вас! 💛"
                )
                await context.bot.send_message(chat_id=b[1], text=text, parse_mode="HTML")
                cursor.execute("UPDATE bookings SET reminded_24h = 1 WHERE id = ?", (b[0],))
        except Exception as e:
            logger.error(f"24h reminder error: {e}")

    # 2. Напоминание за 2 часа (строго)
    # Ищем записи, до которых осталось от 1 до 3 часов
    cursor.execute("""
        SELECT * FROM bookings 
        WHERE status = 'confirmed' AND reminded_2h = 0
    """)
    bookings = cursor.fetchall()
    
    for b in bookings:
        try:
            b_dt = datetime.strptime(f"{b[3]} {b[4]}", "%Y-%m-%d %H:%M")
            b_dt = MOSCOW_TZ.localize(b_dt)
            diff = b_dt - now
            
            if timedelta(minutes=110) <= diff <= timedelta(minutes=130):
                f_dt = format_dt(b[3], b[4])
                text = (
                    "⏰ <b>Напоминание: запись через 2 часа!</b>\n\n"
                    f"💅 Услуга: {b[2]}\n"
                    f"📅 Время: {b[4]}\n"
                    f"🏠 Адрес: {ADDRESS_TEXT}\n\n"
                    "Скоро увидимся! ✨"
                )
                await context.bot.send_message(chat_id=b[1], text=text, parse_mode="HTML")
                cursor.execute("UPDATE bookings SET reminded_2h = 1 WHERE id = ?", (b[0],))
        except Exception as e:
            logger.error(f"2h reminder error: {e}")
            
    conn.commit()
    conn.close()

# --- MAIN ---

async def post_init(application: Application):
    await application.bot.set_my_commands([
        ("start", "Запустить бота"),
        ("menu", "Главное меню"),
        ("recommendation", "Рекомендации по уходу"),
        ("faq", "Вопросы и ответы"),
        ("prices", "Цены на услуги"),
        ("id", "Узнать ID чата"),
        ("debug", "Проверить настройки (админ)"),
        ("test", "Тест уведомлений")
    ])

async def test_notify_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестовое уведомление."""
    user_id = update.effective_user.id
    if user_id != ADMIN_ID and ADMIN_ID != 0:
        return
    await update.message.reply_text("🔔 Отправляю тестовое уведомление...")
    await send_notification(context, "🔔 Это тестовое уведомление! Если вы его видите, значит всё настроено верно.")

async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для проверки конфигурации (только для админа)."""
    user_id = update.effective_user.id
    if user_id != ADMIN_ID and ADMIN_ID != 0:
        await update.message.reply_text("⛔️ У вас нет прав для этой команды.")
        return
        
    text = (
        "⚙️ <b>Текущая конфигурация:</b>\n\n"
        f"👤 <b>ADMIN_ID:</b> <code>{ADMIN_ID}</code>\n"
        f"📢 <b>NOTIFICATION_CHAT_ID:</b> <code>{NOTIFICATION_CHAT_ID}</code>\n"
        f"🕒 <b>Timezone:</b> <code>{TZ_NAME}</code>\n"
        f"🏠 <b>Salon:</b> <code>{SALON_TITLE}</code>\n\n"
        "<i>Если ID равен 0, значит переменная не настроена в Railway.</i>"
    )
    await update.message.reply_text(text, parse_mode="HTML")

async def id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для получения ID текущего чата."""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    await update.message.reply_text(
        f"🆔 <b>Информация об ID:</b>\n\n"
        f"📍 <b>ID этого чата:</b> <code>{chat_id}</code>\n"
        f"👤 <b>Ваш личный ID:</b> <code>{user_id}</code>\n\n"
        "📝 <b>Инструкция:</b>\n"
        "1. Если вы хотите получать уведомления в <b>ЭТОТ</b> чат (группу), скопируйте 📍 <b>ID этого чата</b> и вставьте его в <code>NOTIFICATION_CHAT_ID</code>.\n"
        "2. Обязательно вставьте 👤 <b>Ваш личный ID</b> в переменную <code>ADMIN_ID</code>, чтобы вы могли управлять ботом.",
        parse_mode="HTML"
    )

def main():
    if not BOT_TOKEN:
        print("Error: BOT_TOKEN not found.")
        return

    db_init()
    
    print(f"Bot started. Admin ID: {ADMIN_ID}, Notification Chat ID: {NOTIFICATION_CHAT_ID}")
    
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("id", id_command))
    application.add_handler(CommandHandler("debug", debug_command))
    application.add_handler(CommandHandler("test", test_notify_command))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler("recommendation", recommendation_command))
    application.add_handler(CommandHandler("faq", faq_command))
    application.add_handler(CommandHandler("prices", prices_command))
    application.add_handler(CallbackQueryHandler(on_callback))
    application.add_handler(MessageHandler(filters.CONTACT, on_contact))
    application.add_handler(MessageHandler(filters.PHOTO, on_photo))
    application.add_handler(MessageHandler(filters.TEXT, on_text))
    
    # Анти-спам для всего остального (стикеры, видео, гифки и т.д.)
    async def cleanup_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try: await update.message.delete()
        except: pass
    application.add_handler(MessageHandler(~filters.COMMAND, cleanup_all))

    job_queue = application.job_queue
    job_queue.run_repeating(reminder_job, interval=300, first=10)

    print("Bot started...")
    application.run_polling()

if __name__ == "__main__":
    main()

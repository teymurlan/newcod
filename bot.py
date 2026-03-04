import os
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
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
TZ_NAME = os.getenv("TZ", "Europe/Moscow")
AUTO_CLEAN = os.getenv("AUTO_CLEAN", "1") == "1"
SALON_TITLE = os.getenv("SALON_TITLE", "Beauty Lounge")
MAPS_URL = os.getenv("MAPS_URL", "https://yandex.ru/maps/")
ADDRESS_TEXT = os.getenv("ADDRESS_TEXT", "Дальневосточный проспект 19 к 1, кв 69, этаж 10")

MOSCOW_TZ = pytz.timezone(TZ_NAME)

FAQ_DATA = [
    ("Сколько держится покрытие?", "В среднем 3-4 недели. Мы даем гарантию 7 дней на любые отслойки."),
    ("Как подготовиться к педикюру?", "Специальная подготовка не нужна, просто приходите в удобной обуви."),
    ("Есть ли у вас мужской маникюр?", "Да, конечно! Мы делаем качественный гигиенический маникюр для мужчин."),
    ("Стерильны ли инструменты?", "Абсолютно. Инструменты проходят 3 этапа стерилизации, включая сухожар. Крафт-пакет вскрывается при вас."),
    ("Можно ли прийти с ребенком?", "Да, если ребенок может посидеть спокойно, у нас есть зона ожидания."),
    ("Как отменить запись?", "Вы можете сделать это через раздел 'Мои записи' не позднее чем за 24 часа."),
    ("Делаете ли вы наращивание?", "Да, мы делаем наращивание на формы и гелевые типсы."),
    ("Сколько времени занимает процедура?", "Маникюр с покрытием занимает 1.5 - 2 часа в зависимости от сложности."),
    ("Есть ли парковка?", "Да, рядом с домом всегда есть свободные места для парковки."),
    ("Как оплатить?", "Мы принимаем наличные и переводы на карту.")
]

PHOTO_URLS = [
    "https://picsum.photos/seed/nails1/800/600",
    "https://picsum.photos/seed/nails2/800/600",
    "https://picsum.photos/seed/nails3/800/600",
    "https://picsum.photos/seed/nails4/800/600",
    "https://picsum.photos/seed/nails5/800/600",
    "https://picsum.photos/seed/nails6/800/600",
    "https://picsum.photos/seed/nails7/800/600",
    "https://picsum.photos/seed/nails8/800/600"
]

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- DATABASE ---
DB_PATH = "bot.db"

def db_init():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            name TEXT,
            phone TEXT,
            created_at TEXT
        )
    """)
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
            reminded INTEGER DEFAULT 0
        )
    """)
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

def db_save_user(user_id: int, name: str, phone: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.now(MOSCOW_TZ).isoformat()
    cursor.execute("INSERT OR REPLACE INTO users (user_id, name, phone, created_at) VALUES (?, ?, ?, ?)",
                   (user_id, name, phone, now))
    conn.commit()
    conn.close()

def db_save_booking(user_id: int, service: str, b_date: str, b_time: str, comment: str = ""):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.now(MOSCOW_TZ).isoformat()
    cursor.execute("""
        INSERT INTO bookings (user_id, service, date, time, comment, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, service, b_date, b_time, comment, "pending", now))
    booking_id = cursor.lastrowid
    conn.commit()
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
    
    if "записаться" in t: return "book"
    if "цены" in t: return "prices"
    if "обо мне" in t: return "about"
    if "как нас найти" in t: return "location"
    if "мои записи" in t: return "my_bookings"
    if "отзывы" in t: return "reviews"
    if "админ" in t: return "admin"
    if "вопросы" in t or "faq" in t: return "faq"
    if "меню" in t: return "menu"
    if "назад" in t: return "back"
    
    return None

async def track_message(context: ContextTypes.DEFAULT_TYPE, message_id: int):
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
                await context.bot.delete_message(chat_id=context._chat_id, message_id=mid)
            except:
                pass

async def safe_send(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None, **kwargs):
    chat_id = update.effective_chat.id
    msg = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode="HTML", **kwargs)
    await track_message(context, msg.message_id)
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
            await track_message(context, msg.message_id)
    except Exception as e:
        logger.error(f"Gallery error: {e}")
        await safe_send(update, context, "Ошибка при загрузке фото.")

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
        [InlineKeyboardButton("💅 Маникюр", callback_data="svc_manicure"),
         InlineKeyboardButton("🦶 Педикюр", callback_data="svc_pedicure")],
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
    
    keyboard = []
    start_h = 8
    end_h = 23
    
    times = []
    for h in range(start_h, end_h):
        for m in [0, 30]:
            t_str = f"{h:02d}:{m:02d}"
            if b_date == now.date():
                if h < now.hour or (h == now.hour and m <= now.minute):
                    continue
            times.append(t_str)
            
    for i in range(0, len(times), 4):
        row = [InlineKeyboardButton(t, callback_data=f"time_{t}") for t in times[i:i+4]]
        keyboard.append(row)
        
    keyboard.append([InlineKeyboardButton("⬅️ Назад к календарю", callback_data="back_to_cal")])
    return InlineKeyboardMarkup(keyboard)

def get_confirm_keyboard():
    keyboard = [
        [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_booking")],
        [InlineKeyboardButton("✏️ Комментарий", callback_data="add_comment")],
        [InlineKeyboardButton("❌ Отменить", callback_data="to_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

# --- HANDLERS ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = db_get_user(user_id)
    
    welcome_text = (
        f"👋 Добро пожаловать в <b>{SALON_TITLE}</b>!\n\n"
        "Я ваш личный помощник для записи на процедуры.\n"
        "Здесь вы можете выбрать удобное время, посмотреть цены и оставить отзыв.\n\n"
    )
    
    if not user:
        welcome_text += "Для начала работы, пожалуйста, пройдите короткую регистрацию."
        await safe_send(update, context, welcome_text, reply_markup=get_main_menu_keyboard(user_id))
        await safe_send(update, context, "Как мне к вам обращаться? Введите ваше имя:")
        context.user_data["mode"] = "await_name"
    else:
        welcome_text += f"Рады видеть вас снова, {user[1]}!"
        await safe_send(update, context, welcome_text, reply_markup=get_main_menu_keyboard(user_id))
    
    if update.message:
        await track_message(context, update.message.message_id)

async def on_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("mode") != "await_phone": return
    
    contact = update.message.contact
    phone = contact.phone_number
    name = context.user_data.get("reg_name", update.effective_user.first_name)
    
    db_save_user(update.effective_user.id, name, phone)
    context.user_data["mode"] = None
    
    await safe_send(update, context, f"✅ Регистрация завершена!\nПриятно познакомиться, {name}.\n\nНажмите 📅 <b>Записаться</b>, чтобы выбрать время.", reply_markup=get_main_menu_keyboard(update.effective_user.id))
    await track_message(context, update.message.message_id)

async def on_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if context.user_data.get("mode") != "admin_add_photo": return
    
    try:
        photo = update.message.photo[-1] # Best quality
        db_save_gallery_photo(photo.file_id)
        context.user_data["mode"] = None
        
        await safe_send(update, context, "✅ Фото успешно добавлено в галерею!")
        await show_gallery_admin(update, context)
    except Exception as e:
        logger.error(f"Error saving photo: {e}")
        await safe_send(update, context, "❌ Ошибка при сохранении фото. Попробуйте еще раз.")

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text
    norm = normalize_button(text)
    
    await track_message(context, update.message.message_id)
    
    # 1. Если нажата кнопка меню - ВСЕГДА сбрасываем режим и выполняем команду
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
            return
        elif norm == "about":
            about_text = (
                "<b>👩🎨 О мастере:</b>\n\n"
                "Меня зовут Ирина, я сертифицированный мастер с опытом более 7 лет.\n"
                "✅ Стерильность по СанПиН (3 этапа)\n"
                "✅ Качественные материалы\n"
                "✅ Уютная атмосфера и вкусный кофе\n\n"
                "Буду рада видеть вас у себя!"
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
            bookings = db_get_user_bookings(user_id)
            if not bookings:
                await safe_send(update, context, "У вас пока нет активных записей.")
            else:
                for b in bookings:
                    status_emoji = "⏳" if b[6] == "pending" else "✅"
                    f_dt = format_dt(b[3], b[4])
                    b_text = f"<b>📋 Запись:</b>\n\n{status_emoji} {b[2]}\n📅 {f_dt}\n"
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отменить запись", callback_data=f"cancel_b_{b[0]}")]])
                    await safe_send(update, context, b_text, reply_markup=kb)
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
            text = "<b>❓ Часто задаваемые вопросы:</b>\n\n"
            for i, (q, a) in enumerate(FAQ_DATA, 1):
                text += f"<b>{i}. {q}</b>\n— {a}\n\n"
            await safe_send(update, context, text)
            return
            
        elif norm == "menu":
            await safe_send(update, context, "Главное меню:", reply_markup=get_main_menu_keyboard(user_id))
            return

    # 2. Handle Input Modes
    mode = context.user_data.get("mode")
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
        db_save_user(user_id, name, clean_phone)
        context.user_data["mode"] = None
        await safe_send(update, context, f"✅ Регистрация завершена!\nПриятно познакомиться, {name}.", reply_markup=get_main_menu_keyboard(user_id))

    elif mode == "await_comment":
        context.user_data["b_comment"] = text
        context.user_data["mode"] = None
        await show_booking_summary(update, context)

    elif mode == "await_review":
        db_save_review(user_id, text)
        context.user_data["mode"] = None
        await safe_send(update, context, "🙏 Спасибо за ваш отзыв! Он появится в списке после обновления.")

    elif mode == "admin_msg":
        target_user_id = context.user_data.get("admin_target_user")
        if target_user_id:
            try:
                await context.bot.send_message(chat_id=target_user_id, text=f"💬 <b>Сообщение от мастера:</b>\n\n{text}", parse_mode="HTML")
                await safe_send(update, context, "✅ Сообщение отправлено клиенту.")
            except:
                await safe_send(update, context, "❌ Не удалось отправить сообщение.")
        context.user_data["mode"] = None

async def show_admin_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "🛠 <b>Админ-панель</b>\n\nВыберите период для просмотра записей или управление контентом:"
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
        [InlineKeyboardButton("⬅️ Назад в админку", callback_data="admin_back")]
    ]
    await safe_send(update, context, text, reply_markup=InlineKeyboardMarkup(kb))

async def show_booking_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data
    user_id = update.effective_user.id
    user = db_get_user(user_id)
    
    is_first = not db_has_previous_bookings(user_id)
    f_dt = format_dt(data.get('b_date'), data.get('b_time'))
    
    summary = (
        "<b>🏁 Подтверждение записи:</b>\n\n"
        f"💅 Услуга: {data.get('b_service')}\n"
        f"📅 Дата и время: {f_dt}\n"
        f"👤 Имя: {user[1]}\n"
        f"📞 Тел: {user[2]}\n"
    )
    
    if is_first:
        summary += "\n🎁 <b>Акция:</b> Ваша первая запись через бота! Скидка <b>7%</b> применена. ✨\n"
        
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

    if data == "to_menu":
        context.user_data["mode"] = None
        await safe_send(update, context, "Главное меню:", reply_markup=get_main_menu_keyboard(user_id))
    
    elif data == "book_start":
        await query.edit_message_text("Выберите услугу:", reply_markup=get_services_keyboard())
    
    elif data == "show_gallery":
        await send_gallery(update, context)
    
    elif data.startswith("svc_"):
        services = {"svc_manicure": "Маникюр", "svc_pedicure": "Педикюр", "svc_ext": "Наращивание", "svc_corr": "Коррекция"}
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
        await query.edit_message_text(f"Выбрана дата: {b_date}\nВыберите время:", reply_markup=get_time_keyboard(b_date))
        
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
        is_first = not db_has_previous_bookings(user_id)
        
        b_id = db_save_booking(
            user_id, 
            context.user_data.get("b_service"),
            context.user_data.get("b_date"),
            context.user_data.get("b_time"),
            context.user_data.get("b_comment", "")
        )
        
        msg = "✅ Запись создана! Ожидайте подтверждения мастера."
        if is_first:
            msg += "\n\n🎁 Скидка 7% на первый визит зафиксирована!"
        await safe_send(update, context, msg)
        
        user = db_get_user(user_id)
        admin_text = (
            f"<b>🆕 Новая запись!</b>\n"
            f"{'🎁 АКЦИЯ: ПЕРВЫЙ ВИЗИТ (-7%)' if is_first else ''}\n\n"
            f"👤 Клиент: {user[1]} ({user[2]})\n"
            f"💅 Услуга: {context.user_data.get('b_service')}\n"
            f"📅 Дата: {context.user_data.get('b_date')}\n"
            f"⏰ Время: {context.user_data.get('b_time')}\n"
        )
        if context.user_data.get("b_comment"):
            admin_text += f"💬 Комментарий: {context.user_data.get('b_comment')}\n"
            
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить", callback_data=f"adm_conf_{b_id}"),
             InlineKeyboardButton("❌ Отклонить", callback_data=f"adm_rejc_{b_id}")],
            [InlineKeyboardButton("💬 Написать клиенту", callback_data=f"adm_msg_{user_id}")]
        ])
        await context.bot.send_message(chat_id=ADMIN_ID, text=admin_text, reply_markup=kb, parse_mode="HTML")
        context.user_data.clear()

    elif data.startswith("gal_"):
        page = int(data.split("_")[1])
        await send_gallery(update, context, page)
        
    elif data.startswith("adm_view_"):
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

    elif data == "admin_back":
        await show_admin_filters(update, context)

    elif data == "adm_gallery":
        await show_gallery_admin(update, context)
        
    elif data == "adm_gal_add":
        context.user_data["mode"] = "admin_add_photo"
        await safe_send(update, context, "📸 Пожалуйста, пришлите боту фотографию, которую хотите добавить в галерею.")
        
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
        b_id = int(data.split("_")[2])
        booking = db_get_booking(b_id)
        db_update_booking_status(b_id, "cancelled")
        await query.edit_message_text("❌ Запись отменена.")
        user = db_get_user(user_id)
        f_dt = format_dt(booking[3], booking[4])
        await context.bot.send_message(chat_id=ADMIN_ID, text=f"⚠️ Клиент {user[1]} отменил запись на {f_dt}.")

    elif data == "add_review":
        context.user_data["mode"] = "await_review"
        await safe_send(update, context, "Напишите ваш отзыв:")

# --- JOBS ---

async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(MOSCOW_TZ)
    tomorrow = (now + timedelta(days=1)).date().isoformat()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings WHERE date = ? AND status = 'confirmed' AND reminded = 0", (tomorrow,))
    bookings = cursor.fetchall()
    
    for b in bookings:
        try:
            text = (
                "🔔 <b>Напоминание о записи завтра!</b>\n\n"
                f"💅 Услуга: {b[2]}\n"
                f"⏰ Время: {b[4]}\n"
                f"🏠 Адрес: {ADDRESS_TEXT}\n"
                f"🔗 <a href='{MAPS_URL}'>Открыть на картах</a>\n\n"
                "До встречи! 💛"
            )
            await context.bot.send_message(chat_id=b[1], text=text, parse_mode="HTML")
            cursor.execute("UPDATE bookings SET reminded = 1 WHERE id = ?", (b[0],))
        except Exception as e:
            logger.error(f"Failed to send reminder to {b[1]}: {e}")
            
    conn.commit()
    conn.close()

# --- MAIN ---

def main():
    if not BOT_TOKEN:
        print("Error: BOT_TOKEN not found.")
        return

    db_init()
    
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(on_callback))
    application.add_handler(MessageHandler(filters.CONTACT, on_contact))
    application.add_handler(MessageHandler(filters.PHOTO, on_photo))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    job_queue = application.job_queue
    job_queue.run_repeating(reminder_job, interval=600, first=10)

    print("Bot started...")
    application.run_polling()

if __name__ == "__main__":
    main()

import logging
import os
import re
import math
import json
from datetime import datetime, timedelta, time as dt_time
from io import BytesIO
from typing import Dict, List, Tuple, Optional

import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackContext,
    ConversationHandler,
    CallbackQueryHandler,
    JobQueue,
)

# -------------------------
# CONFIGURATION
# -------------------------
TOKEN = "8379494063:AAFl7DYe3yXXGcNA7GJIS9I37CG1VfXG4hw"
ADMIN_ID = 7734124159

# Files
APPLICATIONS_FILE = "applications.xlsx"
HOTELS_FILE = "hotels.xlsx"
USERS_FILE = "users.xlsx"
ARCHIVE_FILE = "archive.xlsx"
COMMENTS_FILE = "comments.xlsx"
TEMPLATES_FILE = "templates.xlsx"
LOG_FILE = "bot_activity.log"

# Constants
ITEMS_PER_PAGE = 5
DATE_RE = re.compile(r"(\d{1,2}\.\d{1,2}\.\d{4})")
TIME_OF_DAY_OPTIONS = {"утром", "днём", "днем", "вечером", "ночью"}

# Status system
STATUSES = {
    "pending": "🕒 На рассмотрении",
    "waiting_payment": "💰 Ожидает оплаты",
    "approved": "✅ Одобрено",
    "ticket_issued": "🎫 Билет выдан",
    "in_progress": "🚉 В пути",
    "completed": "✅ Завершено",
    "rejected": "❌ Отклонено",
    "cancelled": "🚫 Отменено"
}

STATUS_COLORS = {
    "pending": "🟡",
    "waiting_payment": "🟠",
    "approved": "🟢",
    "ticket_issued": "🔵",
    "in_progress": "🟣",
    "completed": "🟤",
    "rejected": "🔴",
    "cancelled": "⚫"
}

# Regions of Uzbekistan for route selection
UZBEKISTAN_REGIONS = {
    "tashkent": "Ташкент",
    "samarkand": "Самарканд",
    "bukhara": "Бухара",
    "khiva": "Хива",
    "andijan": "Андижан",
    "fergana": "Фергана",
    "namangan": "Наманган",
    "nukus": "Нукус",
    "urgench": "Ургенч",
    "karshi": "Карши",
    "jizzakh": "Джизак",
    "gulistan": "Гулистан",
    "termez": "Термез",
    "navoi": "Навои"
}

# Popular routes
POPULAR_ROUTES = [
    "Самарканд - Ташкент",
    "Ташкент - Самарканд",
    "Самарканд - Бухара",
    "Бухара - Самарканд",
    "Самарканд - Ургенч",
    "Ургенч - Самарканд",
    "Самарканд - Карши",
    "Карши - Самарканд"
]

# Conversation states
(
    NAME, PASSPORT, ROUTE, DATE_STR, REASON, CONFIRM,
    HOTEL_CITY, HOTEL_DATERANGE, HOTEL_ROOM_TYPE,
    COMMENT, BROADCAST, TEMPLATE_NAME, TEMPLATE_DATA,
    GROUP_MEMBERS, GROUP_CONFIRM, RETURN_DATE
) = range(16)

# Admin forwarding state
admin_forwarding = {}

# Simple in-memory cache
cache = {}

# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# -------------------------
# FILE MANAGEMENT
# -------------------------
def init_files():
    """Initialize all required files with proper columns"""
    files_config = {
        APPLICATIONS_FILE: [
            "ID", "Timestamp", "UserID", "Username", "FirstName", "LastName",
            "FIO", "PassportFileID", "Route", "Date", "TimeOfDay", "Reason", "Status", "ReturnRoute", "ReturnDate", "IsRoundTrip"
        ],
        HOTELS_FILE: [
            "ID", "Timestamp", "UserID", "Username", "FirstName", "LastName",
            "FIO", "HotelCity", "CheckIn", "CheckOut", "RoomType", "Status"
        ],
        USERS_FILE: ["UserID", "FIO", "PassportFileID", "Username", "FirstName", "LastName", "Registered"],
        ARCHIVE_FILE: ["Type", "ID", "Timestamp", "UserID", "Data", "ArchivedAt"],
        COMMENTS_FILE: ["ID", "Timestamp", "ItemType", "ItemID", "UserID", "Comment", "IsInternal"],
        TEMPLATES_FILE: ["ID", "UserID", "Name", "Data", "Type", "Created"]
    }

    for file_path, columns in files_config.items():
        if not os.path.exists(file_path):
            df = pd.DataFrame(columns=columns)
            safe_write(df, file_path)
            logger.info("Created %s", file_path)


def safe_read(path: str) -> pd.DataFrame:
    """Safe read from Excel with simple caching"""
    try:
        # Try to get from cache first
        if path in cache:
            cached_data = cache[path]
            if datetime.now() - cached_data['timestamp'] < timedelta(minutes=5):
                return cached_data['data']

        if os.path.exists(path):
            df = pd.read_excel(path)
            # Cache for 5 minutes
            cache[path] = {'data': df, 'timestamp': datetime.now()}
            return df
        return pd.DataFrame()
    except Exception as e:
        logger.error("safe_read failed for %s: %s", path, e)
        return pd.DataFrame()


def safe_write(df: pd.DataFrame, path: str):
    """Safe write to Excel with cache invalidation"""
    try:
        df.to_excel(path, index=False)
        # Invalidate cache
        if path in cache:
            del cache[path]
        logger.info("Data written to %s", path)
    except Exception as e:
        logger.error("safe_write failed for %s: %s", path, e)
        raise


def next_id(df: pd.DataFrame) -> int:
    """Generate next ID for DataFrame"""
    try:
        return 1 if df.empty or 'ID' not in df.columns else int(df["ID"].max()) + 1
    except:
        return len(df) + 1


# -------------------------
# DATE HANDLING
# -------------------------
def parse_single_date(text: str) -> Tuple[datetime.date, Optional[str]]:
    """Parse date with optional time of day"""
    m = DATE_RE.search(text)
    if not m:
        raise ValueError("Не найден формат ДД.MM.ГГГГ")

    date_part = m.group(1)
    d = datetime.strptime(date_part, "%d.%m.%Y").date()
    tod = None

    for token in TIME_OF_DAY_OPTIONS:
        if token in text.lower():
            tod = token
            break

    if tod == "днем":
        tod = "днём"

    return d, tod


def parse_date_range(text: str) -> Tuple[datetime.date, datetime.date]:
    """Parse date range"""
    dates = DATE_RE.findall(text)
    if len(dates) < 2:
        raise ValueError("Нужно 2 даты: заезд и выезд (DD.MM.YYYY - DD.MM.YYYY)")

    checkin = datetime.strptime(dates[0], "%d.%m.%Y").date()
    checkout = datetime.strptime(dates[1], "%d.%m.%Y").date()

    return checkin, checkout


def is_future_or_today(d: datetime.date) -> bool:
    """Check if date is today or in future"""
    return d >= datetime.now().date()


# -------------------------
# USER MANAGEMENT
# -------------------------
def save_user_profile(user, user_data: Dict):
    """Save or update user profile"""
    try:
        df = safe_read(USERS_FILE)
        uid = user.id

        existing = df[df["UserID"] == uid]
        if existing.empty:
            new_user = {
                "UserID": uid,
                "FIO": user_data.get("name", ""),
                "PassportFileID": user_data.get("passport", ""),
                "Username": user.username,
                "FirstName": user.first_name,
                "LastName": user.last_name,
                "Registered": datetime.now().isoformat()
            }
            df = pd.concat([df, pd.DataFrame([new_user])], ignore_index=True)
        else:
            idx = existing.index[0]
            if user_data.get("name"):
                df.at[idx, "FIO"] = user_data["name"]
            if user_data.get("passport"):
                df.at[idx, "PassportFileID"] = user_data["passport"]

        safe_write(df, USERS_FILE)
        logger.info("User profile saved for %s", uid)
    except Exception as e:
        logger.error("save_user_profile failed: %s", e)


# -------------------------
# APPLICATION MANAGEMENT
# -------------------------
def save_application(user, user_data: Dict, status: str = "🕒 На рассмотрении") -> Optional[int]:
    """Save new application"""
    try:
        df = safe_read(APPLICATIONS_FILE)
        nid = next_id(df)

        new_app = {
            "ID": nid,
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "UserID": user.id,
            "Username": user.username,
            "FirstName": user.first_name,
            "LastName": user.last_name,
            "FIO": user_data.get("name", ""),
            "PassportFileID": user_data.get("passport", ""),
            "Route": user_data.get("route", ""),
            "Date": user_data.get("date", ""),
            "TimeOfDay": user_data.get("time_of_day", ""),
            "Reason": user_data.get("reason", ""),
            "Status": status,
            "ReturnRoute": user_data.get("return_route", ""),
            "ReturnDate": user_data.get("return_date", ""),
            "IsRoundTrip": user_data.get("is_round_trip", False)
        }

        df = pd.concat([df, pd.DataFrame([new_app])], ignore_index=True)
        safe_write(df, APPLICATIONS_FILE)

        logger.info("Application #%s saved for user %s", nid, user.id)
        return nid
    except Exception as e:
        logger.error("save_application failed: %s", e)
        return None


def save_hotel(user, user_data: Dict, status: str = "🕒 На рассмотрении") -> Optional[int]:
    """Save new hotel booking"""
    try:
        df = safe_read(HOTELS_FILE)
        nid = next_id(df)

        new_hotel = {
            "ID": nid,
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "UserID": user.id,
            "Username": user.username,
            "FirstName": user.first_name,
            "LastName": user.last_name,
            "FIO": user_data.get("name", ""),
            "HotelCity": user_data.get("hotel_city", ""),
            "CheckIn": user_data.get("hotel_checkin_raw", ""),
            "CheckOut": user_data.get("hotel_checkout_raw", ""),
            "RoomType": user_data.get("hotel_room_type", ""),
            "Status": status
        }

        df = pd.concat([df, pd.DataFrame([new_hotel])], ignore_index=True)
        safe_write(df, HOTELS_FILE)

        logger.info("Hotel booking #%s saved for user %s", nid, user.id)
        return nid
    except Exception as e:
        logger.error("save_hotel failed: %s", e)
        return None


# -------------------------
# NOTIFICATION SYSTEM
# -------------------------
def notify_user_status_change(context: CallbackContext, user_id: int,
                              item_type: str, item_id: int, new_status: str,
                              comment: str = ""):
    """Notify user about status change"""
    try:
        item_name = "заявки на билет" if item_type == "app" else "бронирования отеля"
        text = f"🔔 Статус вашей {item_name} №{item_id} изменён: <b>{new_status}</b>"

        if comment:
            text += f"\n💬 Комментарий: {comment}"

        context.bot.send_message(chat_id=user_id, text=text, parse_mode="HTML")
        logger.info("Notification sent to user %s for %s #%s", user_id, item_type, item_id)
    except Exception as e:
        logger.error("notify_user_status_change failed: %s", e)


def send_reminder(context: CallbackContext, user_id: int,
                  item_type: str, item_id: int, trip_date: str):
    """Send reminder about upcoming trip"""
    try:
        item_name = "поездки" if item_type == "app" else "заезда в отель"
        text = f"⏰ Напоминание: до {item_name} №{item_id} осталось 3 дня!\n📅 Дата: {trip_date}"

        context.bot.send_message(chat_id=user_id, text=text)
        logger.info("Reminder sent to user %s for %s #%s", user_id, item_type, item_id)
    except Exception as e:
        logger.error("send_reminder failed: %s", e)


# -------------------------
# COMMENT SYSTEM
# -------------------------
def add_comment(item_type: str, item_id: int, user_id: int,
                comment: str, is_internal: bool = False) -> int:
    """Add comment to item"""
    try:
        df = safe_read(COMMENTS_FILE)
        comment_id = next_id(df)

        new_comment = {
            "ID": comment_id,
            "Timestamp": datetime.now().isoformat(),
            "ItemType": item_type,
            "ItemID": item_id,
            "UserID": user_id,
            "Comment": comment,
            "IsInternal": is_internal
        }

        df = pd.concat([df, pd.DataFrame([new_comment])], ignore_index=True)
        safe_write(df, COMMENTS_FILE)

        logger.info("Comment #%s added to %s #%s", comment_id, item_type, item_id)
        return comment_id
    except Exception as e:
        logger.error("add_comment failed: %s", e)
        return -1


def get_comments(item_type: str, item_id: int, include_internal: bool = False) -> List[Dict]:
    """Get comments for item"""
    try:
        df = safe_read(COMMENTS_FILE)
        if df.empty:
            return []

        comments = df[(df["ItemType"] == item_type) & (df["ItemID"] == item_id)]
        if not include_internal:
            comments = comments[~comments["IsInternal"]]

        return comments.to_dict("records")
    except Exception as e:
        logger.error("get_comments failed: %s", e)
        return []


# -------------------------
# TEMPLATE SYSTEM
# -------------------------
def save_template(user_id: int, name: str, template_type: str, data: Dict) -> int:
    """Save template for quick access"""
    try:
        df = safe_read(TEMPLATES_FILE)
        template_id = next_id(df)

        template = {
            "ID": template_id,
            "UserID": user_id,
            "Name": name,
            "Type": template_type,
            "Data": json.dumps(data, ensure_ascii=False),
            "Created": datetime.now().isoformat()
        }

        df = pd.concat([df, pd.DataFrame([template])], ignore_index=True)
        safe_write(df, TEMPLATES_FILE)

        logger.info("Template '%s' saved for user %s", name, user_id)
        return template_id
    except Exception as e:
        logger.error("save_template failed: %s", e)
        return -1


def get_templates(user_id: int, template_type: str = None) -> List[Dict]:
    """Get user templates"""
    try:
        df = safe_read(TEMPLATES_FILE)
        if df.empty:
            return []

        user_templates = df[df["UserID"] == user_id]
        if template_type:
            user_templates = user_templates[user_templates["Type"] == template_type]

        templates = []
        for _, row in user_templates.iterrows():
            template = row.to_dict()
            template["Data"] = json.loads(template["Data"])
            templates.append(template)

        return templates
    except Exception as e:
        logger.error("get_templates failed: %s", e)
        return []


# -------------------------
# ARCHIVATION SYSTEM
# -------------------------
def archive_item(item_type: str, item_id: int) -> bool:
    """Archive item"""
    try:
        if item_type == "app":
            df = safe_read(APPLICATIONS_FILE)
            source_file = APPLICATIONS_FILE
        else:
            df = safe_read(HOTELS_FILE)
            source_file = HOTELS_FILE

        item = df[df["ID"] == item_id]
        if item.empty:
            return False

        # Save to archive
        archive_df = safe_read(ARCHIVE_FILE)
        archived_item = {
            "Type": item_type,
            "ID": item_id,
            "Timestamp": item.iloc[0]["Timestamp"],
            "UserID": item.iloc[0]["UserID"],
            "Data": item.iloc[0].to_json(),
            "ArchivedAt": datetime.now().isoformat()
        }

        archive_df = pd.concat([archive_df, pd.DataFrame([archived_item])], ignore_index=True)
        safe_write(archive_df, ARCHIVE_FILE)

        # Remove from source
        df = df[df["ID"] != item_id]
        safe_write(df, source_file)

        logger.info("%s #%s archived", item_type, item_id)
        return True
    except Exception as e:
        logger.error("archive_item failed: %s", e)
        return False


# -------------------------
# FORMATTING FUNCTIONS
# -------------------------
def format_application_card(record: Dict, for_admin: bool = False) -> Tuple[str, InlineKeyboardMarkup]:
    """Format application card with enhanced information"""
    id_ = record.get("ID", "?")
    fio = record.get("FIO", record.get("FirstName", ""))
    route = record.get("Route", "—")
    date = record.get("Date", "—")
    tod = record.get("TimeOfDay", "")
    reason = record.get("Reason", "—")
    status = record.get("Status", STATUSES["pending"])
    username = record.get("Username", "")
    is_round_trip = record.get("IsRoundTrip", False)
    return_route = record.get("ReturnRoute", "")
    return_date = record.get("ReturnDate", "")

    # Get comments count
    comments_count = len(get_comments("app", id_))

    # Get status color
    status_key = next((k for k, v in STATUSES.items() if v == status), "pending")
    status_color = STATUS_COLORS.get(status_key, "⚪")

    card = (
        f"✈️ <b>Заявка №{id_}</b> {status_color}\n"
        f"👤 <b>{fio}</b> {f'(@{username})' if username else ''}\n"
        f"🛤 Маршрут: <i>{route}</i>\n"
        f"📅 Дата: <code>{date}</code> {tod}\n"
    )

    if is_round_trip:
        card += f"🔄 Обратный маршрут: <i>{return_route}</i>\n"
        card += f"📅 Дата возврата: <code>{return_date}</code>\n"

    card += (
        f"📝 Причина: {reason}\n"
        f"📌 Статус: <b>{status}</b>\n"
        f"💬 Комментарии: {comments_count}"
    )

    # Admin buttons - ALWAYS show for admin
    if for_admin:
        kb = [
            [InlineKeyboardButton("✅ Одобрить", callback_data=f"status:app:{id_}:approved"),
             InlineKeyboardButton("💰 Оплата", callback_data=f"status:app:{id_}:waiting_payment")],
            [InlineKeyboardButton("🎫 Билет выдан", callback_data=f"status:app:{id_}:ticket_issued"),
             InlineKeyboardButton("🚉 В пути", callback_data=f"status:app:{id_}:in_progress")],
            [InlineKeyboardButton("✅ Завершено", callback_data=f"status:app:{id_}:completed"),
             InlineKeyboardButton("❌ Отклонить", callback_data=f"status:app:{id_}:rejected")],
            [InlineKeyboardButton("✏️ Комментарий", callback_data=f"comment:app:{id_}"),
             InlineKeyboardButton("📋 Подробнее", callback_data=f"details:app:{id_}")],
            [InlineKeyboardButton("🗑️ Архивировать", callback_data=f"archive:app:{id_}")]
        ]
    else:
        # User view - only cancel button
        kb = [
            [InlineKeyboardButton("❌ Отменить заявку", callback_data=f"cancel_app:{id_}")],
            [InlineKeyboardButton("◀️ Назад к списку", callback_data=f"page:application:1")]
        ]

    return card, InlineKeyboardMarkup(kb)


def format_hotel_card(record: Dict, for_admin: bool = False) -> Tuple[str, InlineKeyboardMarkup]:
    """Format hotel card with enhanced information"""
    id_ = record.get("ID", "?")
    fio = record.get("FIO", record.get("FirstName", ""))
    city = record.get("HotelCity", "—")
    checkin = record.get("CheckIn", "—")
    checkout = record.get("CheckOut", "—")
    room = record.get("RoomType", "—")
    status = record.get("Status", STATUSES["pending"])
    username = record.get("Username", "")

    comments_count = len(get_comments("hotel", id_))
    status_key = next((k for k, v in STATUSES.items() if v == status), "pending")
    status_color = STATUS_COLORS.get(status_key, "⚪")

    card = (
        f"🏨 <b>Бронирование #{id_}</b> {status_color}\n"
        f"👤 <b>{fio}</b> {f'(@{username})' if username else ''}\n"
        f"🌍 Город: <i>{city}</i>\n"
        f"📅 Заезд: <code>{checkin}</code> | Выезд: <code>{checkout}</code>\n"
        f"🛏 Номер: {room}\n"
        f"📌 Статус: <b>{status}</b>\n"
        f"💬 Комментарии: {comments_count}"
    )

    if for_admin:
        kb = [
            [InlineKeyboardButton("✅ Подтвердить", callback_data=f"status:hotel:{id_}:approved"),
             InlineKeyboardButton("💰 Оплата", callback_data=f"status:hotel:{id_}:waiting_payment")],
            [InlineKeyboardButton("❌ Отклонить", callback_data=f"status:hotel:{id_}:rejected")],
            [InlineKeyboardButton("✏️ Комментарий", callback_data=f"comment:hotel:{id_}"),
             InlineKeyboardButton("📋 Подробнее", callback_data=f"details:hotel:{id_}")],
            [InlineKeyboardButton("🗑️ Архивировать", callback_data=f"archive:hotel:{id_}")]
        ]
    else:
        # User view - only cancel button
        kb = [
            [InlineKeyboardButton("❌ Отменить бронь", callback_data=f"cancel_hotel:{id_}")],
            [InlineKeyboardButton("◀️ Назад к списку", callback_data=f"page:hotel:1")]
        ]

    return card, InlineKeyboardMarkup(kb)


def build_page(items: List[Dict], page: int, item_type: str = "application") -> Tuple[str, InlineKeyboardMarkup]:
    """Build paginated page with items"""
    total = len(items)
    pages = max(1, math.ceil(total / ITEMS_PER_PAGE))
    page = max(1, min(page, pages))
    start = (page - 1) * ITEMS_PER_PAGE
    chunk = items[start:start + ITEMS_PER_PAGE]

    lines = []
    kb = []

    for item in chunk:
        if "Route" in item:  # Application
            status_emoji = STATUS_COLORS.get(
                next((k for k, v in STATUSES.items() if v == item.get("Status", "")), "pending"), "⚪")
            route_info = f"{item.get('Route', '—')}"
            if item.get("IsRoundTrip", False):
                route_info += " 🔄"
            lines.append(f"{status_emoji} #{item['ID']} — {route_info} | {item.get('Date', '—')}")
            kb.append([InlineKeyboardButton(f"🔍 #{item['ID']}", callback_data=f"view_app:{item['ID']}")])
        else:  # Hotel
            status_emoji = STATUS_COLORS.get(
                next((k for k, v in STATUSES.items() if v == item.get("Status", "")), "pending"), "⚪")
            lines.append(f"{status_emoji} H#{item['ID']} — {item.get('HotelCity', '—')} | {item.get('CheckIn', '—')}")
            kb.append([InlineKeyboardButton(f"🔍 H#{item['ID']}", callback_data=f"view_hotel:{item['ID']}")])

    # Navigation
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"page:{item_type}:{page - 1}"))
    if page < pages:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"page:{item_type}:{page + 1}"))

    if nav:
        kb.append(nav)

    kb.append([InlineKeyboardButton("🔄 Обновить", callback_data=f"page:{item_type}:{page}")])

    text = "\n".join(lines) if lines else "Пусто."
    text = f"{text}\n\nСтраница {page}/{pages}. Всего: {total}"

    return text, InlineKeyboardMarkup(kb)


# -------------------------
# ORIGINAL FLOW HANDLERS
# -------------------------
def cmd_start(update: Update, context: CallbackContext):
    user = update.effective_user
    # CHANGED: Swapped positions of "Мои заявки" and "Новая заявка"
    kb = [
        ["✈ Новая заявка", "📝 Мои заявки"],  # Changed order
        ["🏨 Забронировать отель", "ℹ Помощь"]
    ]
    reply = ReplyKeyboardMarkup(kb, resize_keyboard=True)
    update.message.reply_text(
        f"Привет, {user.first_name}! 👋\nЯ помогу оформить заявку на Ж/Д билет или забронировать гостиницу.",
        reply_markup=reply
    )


def cb_start_app(update: Update, context: CallbackContext):
    """
    Handles both callback_query (if pressed inline) or message (from keyboard)
    If user profile with FIO exists, prefill and go straight to PASSPORT.
    Otherwise ask for NAME.
    """
    q = update.callback_query if update.callback_query else None
    user = q.from_user if q else update.message.from_user
    users = safe_read(USERS_FILE)
    row = users[users["UserID"] == user.id] if not users.empty else pd.DataFrame()
    if q:
        q.answer()
    # If profile exists, prefill name and go to passport
    if not row.empty and row.iloc[0].get("FIO"):
        suggested = row.iloc[0].get("FIO")
        context.user_data["name"] = suggested
        # Inform user and ask passport (allow override by sending new name before passport)
        if q:
            q.message.reply_text(
                f"Я подставил ФИО из профиля: {suggested}\nЕсли хотите изменить — введите новое ФИО. Иначе отправьте фото/скан паспорта:")
        else:
            update.message.reply_text(
                f"Я подставил ФИО из профиля: {suggested}\nЕсли хотите изменить — введите новое ФИО. Иначе отправьте фото/скан паспорта:")
        return PASSPORT
    # else ask for name
    if q:
        q.message.reply_text("📝 Введите ваше ФИО:")
    else:
        update.message.reply_text("📝 Введите ваше ФИО:")
    return NAME


def cb_start_hotel(update: Update, context: CallbackContext):
    q = update.callback_query if update.callback_query else None
    user = q.from_user if q else update.message.from_user
    users = safe_read(USERS_FILE)
    row = users[users["UserID"] == user.id] if not users.empty else pd.DataFrame()
    if q:
        q.answer()
    if not row.empty and row.iloc[0].get("FIO"):
        suggested = row.iloc[0].get("FIO")
        context.user_data["name"] = suggested
        if q:
            q.message.reply_text(
                f"Я подставил ФИО из профиля: {suggested}\nЕсли хотите изменить — введите новое ФИО. Иначе введите город гостиницы:")
        else:
            update.message.reply_text(
                f"Я подставил ФИО из профиля: {suggested}\nЕсли хотите изменить — введите новое ФИО. Иначе введите город гостиницы:")
        # go to HOTEL_CITY so user can directly input hotel city or change FIO by typing
        return HOTEL_CITY
    if q:
        q.message.reply_text("🏨 Введите ваше ФИО:")
    else:
        update.message.reply_text("🏨 Введите ваше ФИО:")
    return NAME


def flow_name(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    if not text:
        update.message.reply_text("ФИО не может быть пустым. Введите ФИО:")
        return NAME
    context.user_data["name"] = text
    save_user_profile(update.effective_user, context.user_data)
    update.message.reply_text("📷 Прикрепите фото или скан паспорта (или отправьте документ):")
    return PASSPORT


def flow_passport(update: Update, context: CallbackContext):
    msg = update.message
    if msg.photo:
        context.user_data["passport"] = msg.photo[-1].file_id
        save_user_profile(update.effective_user, context.user_data)

        # NEW: Show route selection buttons
        kb = []
        # Add popular routes as buttons
        for i in range(0, len(POPULAR_ROUTES), 2):
            row = []
            if i < len(POPULAR_ROUTES):
                row.append(InlineKeyboardButton(POPULAR_ROUTES[i], callback_data=f"route_select:{POPULAR_ROUTES[i]}"))
            if i + 1 < len(POPULAR_ROUTES):
                row.append(InlineKeyboardButton(POPULAR_ROUTES[i + 1], callback_data=f"route_select:{POPULAR_ROUTES[i + 1]}"))
            if row:
                kb.append(row)

        kb.append([InlineKeyboardButton("✏️ Ввести свой маршрут", callback_data="route_custom")])

        update.message.reply_text(
            "🛤 Выберите маршрут из популярных или введите свой:",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return ROUTE
    elif msg.document:
        context.user_data["passport"] = msg.document.file_id
        save_user_profile(update.effective_user, context.user_data)

        # NEW: Show route selection buttons
        kb = []
        for i in range(0, len(POPULAR_ROUTES), 2):
            row = []
            if i < len(POPULAR_ROUTES):
                row.append(InlineKeyboardButton(POPULAR_ROUTES[i], callback_data=f"route_select:{POPULAR_ROUTES[i]}"))
            if i + 1 < len(POPULAR_ROUTES):
                row.append(InlineKeyboardButton(POPULAR_ROUTES[i + 1], callback_data=f"route_select:{POPULAR_ROUTES[i + 1]}"))
            if row:
                kb.append(row)

        kb.append([InlineKeyboardButton("✏️ Ввести свой маршрут", callback_data="route_custom")])

        update.message.reply_text(
            "🛤 Выберите маршрут из популярных или введите свой:",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return ROUTE
    else:
        # if user typed a new name (override) while in PASSPORT state
        if msg.text and len(msg.text.split()) >= 2:
            # treat as new name
            context.user_data["name"] = msg.text.strip()
            save_user_profile(update.effective_user, context.user_data)
            update.message.reply_text("ФИО обновлено. Теперь прикрепите паспорт (фото/файл).")
            return PASSPORT
        update.message.reply_text("Пожалуйста, отправьте фото или документ с паспортом.")
        return PASSPORT


def handle_route_selection(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()

    if q.data == "route_custom":
        q.message.reply_text("✏️ Введите свой маршрут (например: Самарканд - Ташкент):")
        return ROUTE
    else:
        # Extract route from callback data
        selected_route = q.data.split(":")[1]
        context.user_data["route"] = selected_route
        q.message.reply_text(f"✅ Выбран маршрут: {selected_route}")

        # NEW: Ask about return ticket immediately after route selection
        kb = [
            [InlineKeyboardButton("✅ Да, нужен обратный билет", callback_data="return_yes")],
            [InlineKeyboardButton("❌ Нет, только в один конец", callback_data="return_no")]
        ]
        q.message.reply_text(
            "🔄 Нужен ли обратный билет?",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return RETURN_DATE


def flow_route(update: Update, context: CallbackContext):
    # This handles manual route input
    context.user_data["route"] = update.message.text.strip()

    # NEW: Ask about return ticket
    kb = [
        [InlineKeyboardButton("✅ Да, нужен обратный билет", callback_data="return_yes")],
        [InlineKeyboardButton("❌ Нет, только в один конец", callback_data="return_no")]
    ]
    update.message.reply_text(
        "🔄 Нужен ли обратный билет?",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    return RETURN_DATE


def handle_return_ticket(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()

    if q.data == "return_no":
        context.user_data["is_round_trip"] = False
        q.message.reply_text("📅 Укажите дату поездки (например: 25.11.2025 утром):")
        return DATE_STR
    else:
        context.user_data["is_round_trip"] = True
        q.message.reply_text("📅 Укажите дату поездки (например: 25.11.2025 утром):")
        return DATE_STR


def flow_date(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    try:
        d, tod = parse_single_date(text)
    except Exception:
        update.message.reply_text("❌ Неверный формат. Пример: 25.11.2025 утром. Попробуйте снова:")
        return DATE_STR
    if not is_future_or_today(d):
        update.message.reply_text("❌ Дата должна быть сегодня или в будущем. Введите корректную дату:")
        return DATE_STR
    context.user_data["date"] = d.strftime("%d.%m.%Y")
    context.user_data["date_raw"] = text
    context.user_data["time_of_day"] = tod or ""

    # If round trip, ask for return date
    if context.user_data.get("is_round_trip", False):
        update.message.reply_text("📅 Укажите дату обратного билета (например: 30.11.2025 вечером):")
        return RETURN_DATE
    else:
        update.message.reply_text("📝 Укажите причину поездки (например: командировка):")
        return REASON


def flow_return_date(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    try:
        d, tod = parse_single_date(text)
    except Exception:
        update.message.reply_text("❌ Неверный формат. Пример: 30.11.2025 вечером. Попробуйте снова:")
        return RETURN_DATE

    # Check if return date is after departure date
    departure_date_str = context.user_data.get("date", "")
    if departure_date_str:
        try:
            departure_date = datetime.strptime(departure_date_str, "%d.%m.%Y").date()
            if d <= departure_date:
                update.message.reply_text("❌ Дата возврата должна быть позже даты отправления. Введите снова:")
                return RETURN_DATE
        except:
            pass

    context.user_data["return_date"] = d.strftime("%d.%m.%Y")
    context.user_data["return_time_of_day"] = tod or ""

    # Auto-generate return route (reverse of original route)
    original_route = context.user_data.get("route", "")
    if " - " in original_route:
        parts = original_route.split(" - ")
        if len(parts) == 2:
            context.user_data["return_route"] = f"{parts[1]} - {parts[0]}"
            update.message.reply_text(f"🔄 Обратный маршрут: {context.user_data['return_route']}")

    update.message.reply_text("📝 Укажите причину поездки (например: командировка):")
    return REASON


def flow_reason(update: Update, context: CallbackContext):
    context.user_data["reason"] = update.message.text.strip()

    # Build confirmation card
    card = (
        f"📋 Проверьте заявку:\n\n"
        f"👤 <b>{context.user_data.get('name')}</b>\n"
        f"🛤 Маршрут: <i>{context.user_data.get('route')}</i>\n"
        f"📅 Дата: <code>{context.user_data.get('date')}</code> {context.user_data.get('time_of_day', '')}\n"
    )

    if context.user_data.get("is_round_trip"):
        card += (
            f"🔄 Обратный билет: <i>{context.user_data.get('return_route', '')}</i>\n"
            f"📅 Дата возврата: <code>{context.user_data.get('return_date')}</code> {context.user_data.get('return_time_of_day', '')}\n"
        )

    card += f"📝 Причина: {context.user_data.get('reason')}\n\n"

    kb = [
        [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_app")],
        [InlineKeyboardButton("❌ Отменить", callback_data="cancel_app")]
    ]
    update.message.reply_html(card, reply_markup=InlineKeyboardMarkup(kb))
    return CONFIRM


def cb_confirm_app(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    user = q.from_user
    nid = save_application(user, context.user_data, status=STATUSES["pending"])

    # Build notification message
    notification_msg = (
        f"📩 Новая заявка #{nid} от {user.full_name} ({user.id})\n"
        f"{context.user_data.get('route', '-')} | {context.user_data.get('date', '-')} {context.user_data.get('time_of_day', '')}"
    )

    if context.user_data.get("is_round_trip"):
        notification_msg += f"\n🔄 Обратный: {context.user_data.get('return_route', '-')} | {context.user_data.get('return_date', '-')}"

    # notify admin
    try:
        context.bot.send_message(ADMIN_ID, notification_msg)
        if context.user_data.get("passport"):
            context.bot.send_photo(ADMIN_ID, context.user_data["passport"], caption=f"Паспорт — заявка #{nid}")
    except Exception:
        logger.exception("notify admin failed")

    kb = [
        [InlineKeyboardButton("🏨 Забронировать гостиницу", callback_data="start_hotel")],
        [InlineKeyboardButton("📋 Мои заявки", callback_data="my_requests")],
        [InlineKeyboardButton("📝 Заполнить новую заявку", callback_data="start_app_again")]
    ]
    q.edit_message_text("✅ Заявка сохранена и отправлена администратору.", reply_markup=InlineKeyboardMarkup(kb))
    preserved = {k: context.user_data.get(k) for k in ("name", "passport")}
    context.user_data.clear()
    context.user_data.update(preserved)
    return ConversationHandler.END


def cb_cancel_app(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    q.edit_message_text("❌ Заявка отменена. Вы можете начать заново командой /start")
    return ConversationHandler.END


# -------------------------
# Hotel flow handlers
# -------------------------
def flow_hotel_city(update: Update, context: CallbackContext):
    text = update.message.text.strip()

    # Если пользователь ввел что-то похожее на ФИО (содержит пробелы)
    if len(text.split()) >= 2 and "@" not in text:
        # Сохраняем как ФИО и просим город
        context.user_data["name"] = text
        save_user_profile(update.effective_user, context.user_data)
        update.message.reply_text("ФИО сохранено. Теперь укажите город гостиницы:")
        return HOTEL_CITY

    # Если это город, сохраняем и переходим к датам
    context.user_data["hotel_city"] = text
    update.message.reply_text("📅 Введите заезд и выезд как: DD.MM.YYYY - DD.MM.YYYY")
    return HOTEL_DATERANGE


def flow_hotel_daterange(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    try:
        checkin, checkout = parse_date_range(text)
    except Exception:
        update.message.reply_text("❌ Неверный формат. Пример: 11.11.2025 - 20.11.2025")
        return HOTEL_DATERANGE
    if not (is_future_or_today(checkin) and is_future_or_today(checkout)):
        update.message.reply_text("❌ Даты должны быть сегодня или в будущем. Введите снова:")
        return HOTEL_DATERANGE
    if checkout <= checkin:
        update.message.reply_text("❌ Дата выезда должна быть позже заезда. Введите снова:")
        return HOTEL_DATERANGE
    context.user_data["hotel_checkin_raw"] = checkin.strftime("%d.%m.%Y")
    context.user_data["hotel_checkout_raw"] = checkout.strftime("%d.%m.%Y")
    context.user_data[
        "hotel_date"] = f"{context.user_data['hotel_checkin_raw']} - {context.user_data['hotel_checkout_raw']}"
    kb = [
        [InlineKeyboardButton("🛌 Одноместный", callback_data="room_single")],
        [InlineKeyboardButton("🛌🛌 Двухместный", callback_data="room_double")],
        [InlineKeyboardButton("👨‍👩‍👧 Семейный", callback_data="room_family")],
        [InlineKeyboardButton("💼 Бизнес-люкс", callback_data="room_luxury")]
    ]
    update.message.reply_text("Выберите тип номера:", reply_markup=InlineKeyboardMarkup(kb))
    return HOTEL_ROOM_TYPE


def cb_hotel_room_type(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    mapping = {
        "room_single": "Одноместный",
        "room_double": "Двухместный",
        "room_family": "Семейный",
        "room_luxury": "Бизнес-люкс"
    }
    sel = mapping.get(q.data, "Не указано")
    context.user_data["hotel_room_type"] = sel
    hid = save_hotel(q.from_user, context.user_data, status=STATUSES["pending"])
    try:
        context.bot.send_message(ADMIN_ID,
                                 f"🏨 Новая бронь #{hid} от {q.from_user.full_name}: {context.user_data.get('hotel_city')} | {context.user_data.get('hotel_date')} | {sel}")
    except Exception:
        logger.exception("notify admin hotel failed")
    kb = [[InlineKeyboardButton("📝 Новая заявка", callback_data="start_app")]]
    q.edit_message_text("✅ Бронирование сохранено и отправлено админу.", reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END


# -------------------------
# My requests, pagination, detail, cancel
# -------------------------
def get_user_applications(user_id):
    df = safe_read(APPLICATIONS_FILE)
    if df.empty:
        return []
    return df[df["UserID"] == user_id].sort_values("Timestamp", ascending=False).to_dict("records")


def get_user_hotels(user_id):
    df = safe_read(HOTELS_FILE)
    if df.empty:
        return []
    return df[df["UserID"] == user_id].sort_values("Timestamp", ascending=False).to_dict("records")


def cb_my_requests(update: Update, context: CallbackContext):
    # supports both callback_query and message
    if update.callback_query:
        q = update.callback_query
        q.answer()
        user_id = q.from_user.id
        target = q.message
    else:
        user_id = update.message.from_user.id
        target = update.message
    apps = get_user_applications(user_id)
    hotels = get_user_hotels(user_id)
    combined = [("app", a) for a in apps] + [("hotel", h) for h in hotels]
    combined.sort(key=lambda t: t[1].get("Timestamp", ""), reverse=True)
    context.user_data["my_list"] = combined
    context.user_data["my_page"] = 1

    # Определяем тип для отображения первой страницы
    if apps and hotels:
        # Если есть оба типа, показываем сначала обычные заявки
        item_type = "application"
    elif hotels:
        item_type = "hotel"
    else:
        item_type = "application"

    records = [r for (_, r) in combined]
    text, kb = build_page(records, 1, item_type=item_type)
    target.reply_text("📋 Мои заявки:\n\n" + text, reply_markup=kb)


def cb_page_view(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    data = q.data

    if data.startswith("page:"):
        _, typ, p = data.split(":")
        p = int(p)

        # Если это обновление (кнопка "Обновить"), перезагружаем данные
        if p == context.user_data.get("my_page", 1):
            user_id = q.from_user.id
            apps = get_user_applications(user_id)
            hotels = get_user_hotels(user_id)
            combined = [("app", a) for a in apps] + [("hotel", h) for h in hotels]
            combined.sort(key=lambda t: t[1].get("Timestamp", ""), reverse=True)
            context.user_data["my_list"] = combined

        combo = context.user_data.get("my_list", [])
        records = [r for (_, r) in combo]
        text, kb = build_page(records, p, item_type=typ)
        context.user_data["my_page"] = p
        q.message.edit_text("📋 Мои заявки:\n\n" + text, reply_markup=kb)
        return

    if data.startswith("view_app:"):
        _, sid = data.split(":")
        sid = int(sid)
        df = safe_read(APPLICATIONS_FILE)
        row = df[df["ID"] == sid]
        if row.empty:
            q.message.reply_text("Заявка не найдена.")
            return
        r = row.iloc[0].to_dict()
        # FIXED: Use the function correctly and show proper info to user
        card, kb = format_application_card(r, for_admin=False)
        q.message.reply_html(card, reply_markup=kb)
        return

    if data.startswith("view_hotel:"):
        _, sid = data.split(":")
        sid = int(sid)
        df = safe_read(HOTELS_FILE)
        row = df[df["ID"] == sid]
        if row.empty:
            q.message.reply_text("Бронирование не найдено.")
            return
        r = row.iloc[0].to_dict()
        # FIXED: Use the function correctly and show proper info to user
        card, kb = format_hotel_card(r, for_admin=False)
        q.message.reply_html(card, reply_markup=kb)
        return


def cb_cancel_app_by_id(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    try:
        _, sid = q.data.split(":")
        sid = int(sid)
    except:
        q.message.reply_text("Неверный ID")
        return
    df = safe_read(APPLICATIONS_FILE)
    idx = df.index[df["ID"] == sid].tolist()
    if not idx:
        q.message.reply_text("Заявка не найдена.")
        return
    i = idx[0]
    df.at[i, "Status"] = "❌ Отклонена пользователем"
    safe_write(df, APPLICATIONS_FILE)
    user_id = int(df.at[i, "UserID"])
    try:
        context.bot.send_message(user_id, f"⚠️ Ваша заявка #{sid} помечена как отменённая.")
    except Exception:
        logger.exception("notify user cancel failed")
    q.message.reply_text(f"✅ Заявка #{sid} помечена как отменена.")


def cb_cancel_hotel_by_id(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    try:
        _, sid = q.data.split(":")
        sid = int(sid)
    except:
        q.message.reply_text("Неверный ID")
        return
    df = safe_read(HOTELS_FILE)
    idx = df.index[df["ID"] == sid].tolist()
    if not idx:
        q.message.reply_text("Бронирование не найдено.")
        return
    i = idx[0]
    df.at[i, "Status"] = "❌ Отменено пользователем"
    safe_write(df, HOTELS_FILE)
    user_id = int(df.at[i, "UserID"])
    try:
        context.bot.send_message(user_id, f"⚠️ Ваше бронирование отеля #{sid} помечено как отменённое.")
    except Exception:
        logger.exception("notify user cancel hotel failed")
    q.message.reply_text(f"✅ Бронирование #{sid} помечено как отменено.")


# -------------------------
# NEW ADMIN COMMANDS
# -------------------------
def cmd_admin_all(update: Update, context: CallbackContext):
    """Показать все заявки админу с кнопками управления"""
    if update.effective_user.id != ADMIN_ID:
        update.message.reply_text("⛔ Только админ.")
        return

    # Заявки на билеты
    apps_df = safe_read(APPLICATIONS_FILE)
    if not apps_df.empty:
        update.message.reply_text("📊 Все заявки на билеты:")
        for _, app in apps_df.iterrows():
            card, kb = format_application_card(app.to_dict(), for_admin=True)
            update.message.reply_html(card, reply_markup=kb)
    else:
        update.message.reply_text("Нет заявок на билеты.")

    # Бронирования отелей
    hotels_df = safe_read(HOTELS_FILE)
    if not hotels_df.empty:
        update.message.reply_text("🏨 Все бронирования отелей:")
        for _, hotel in hotels_df.iterrows():
            card, kb = format_hotel_card(hotel.to_dict(), for_admin=True)
            update.message.reply_html(card, reply_markup=kb)
    else:
        update.message.reply_text("Нет бронирований отелей.")


def cmd_admin_pending(update: Update, context: CallbackContext):
    """Показать только заявки на рассмотрении"""
    if update.effective_user.id != ADMIN_ID:
        update.message.reply_text("⛔ Только админ.")
        return

    # Заявки на билеты
    apps_df = safe_read(APPLICATIONS_FILE)
    if not apps_df.empty:
        pending_apps = apps_df[apps_df["Status"] == STATUSES["pending"]]
        if not pending_apps.empty:
            update.message.reply_text("🕒 Заявки на рассмотрении:")
            for _, app in pending_apps.iterrows():
                card, kb = format_application_card(app.to_dict(), for_admin=True)
                update.message.reply_html(card, reply_markup=kb)
        else:
            update.message.reply_text("Нет заявок на рассмотрении.")
    else:
        update.message.reply_text("Нет заявок на билеты.")

    # Бронирования отелей
    hotels_df = safe_read(HOTELS_FILE)
    if not hotels_df.empty:
        pending_hotels = hotels_df[hotels_df["Status"] == STATUSES["pending"]]
        if not pending_hotels.empty:
            update.message.reply_text("🕒 Бронирования на рассмотрении:")
            for _, hotel in pending_hotels.iterrows():
                card, kb = format_hotel_card(hotel.to_dict(), for_admin=True)
                update.message.reply_html(card, reply_markup=kb)
        else:
            update.message.reply_text("Нет бронирований на рассмотрении.")


# -------------------------
# Search (user & admin)
# -------------------------
def cmd_search_user_date(update: Update, context: CallbackContext):
    if not context.args:
        update.message.reply_text("Использование: /search_date DD.MM.YYYY")
        return
    try:
        d = datetime.strptime(context.args[0], "%d.%m.%Y").date()
    except Exception:
        update.message.reply_text("Неверный формат. Пример: 25.11.2025")
        return
    df = safe_read(APPLICATIONS_FILE)
    if df.empty:
        update.message.reply_text("Нет заявок.")
        return
    matched = df[df["Date"] == d.strftime("%d.%m.%Y")]
    if matched.empty:
        update.message.reply_text("Нет заявок на указанную дату.")
        return
    for _, r in matched.iterrows():
        card, kb = format_application_card(r.to_dict(), for_admin=(update.effective_user.id == ADMIN_ID))
        update.message.reply_html(card, reply_markup=kb)


def cmd_search_user_city(update: Update, context: CallbackContext):
    if not context.args:
        update.message.reply_text("Использование: /search_city <город>")
        return
    city = " ".join(context.args).lower()
    df = safe_read(APPLICATIONS_FILE)
    if df.empty:
        update.message.reply_text("Нет заявок.")
        return
    matched = df[df["Route"].str.lower().str.contains(city, na=False)]
    if matched.empty:
        update.message.reply_text("Нет заявок для этого города.")
        return
    for _, r in matched.iterrows():
        card, kb = format_application_card(r.to_dict(), for_admin=(update.effective_user.id == ADMIN_ID))
        update.message.reply_html(card, reply_markup=kb)


def cmd_admin_search(update: Update, context: CallbackContext):
    if update.effective_user.id != ADMIN_ID:
        update.message.reply_text("⛔ Только админ.")
        return
    if not context.args:
        update.message.reply_text("Использование: /admin_search <user_id|FIO>")
        return
    query = " ".join(context.args)
    df = safe_read(APPLICATIONS_FILE)
    if df.empty:
        update.message.reply_text("Нет заявок.")
        return
    if query.isdigit():
        uid = int(query)
        matched = df[df["UserID"] == uid]
    else:
        matched = df[df["FIO"].str.contains(query, na=False, case=False)]
    if matched.empty:
        update.message.reply_text("Не найдено.")
        return
    for _, r in matched.iterrows():
        card, kb = format_application_card(r.to_dict(), for_admin=True)
        update.message.reply_html(card, reply_markup=kb)


# -------------------------
# PDF reports (ReportLab)
# -------------------------
def generate_pdf_report_applications(records, title="Отчёт"):
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    y = height - 50
    c.setFont("Helvetica-Bold", 14)
    c.drawString(40, y, title)
    y -= 30
    c.setFont("Helvetica", 10)
    for rec in records:
        text = (
            f"ID: {rec.get('ID')} | {rec.get('Timestamp')}\n"
            f"FIO: {rec.get('FIO')}\n"
            f"Маршрут: {rec.get('Route')}\n"
            f"Дата: {rec.get('Date')} {rec.get('TimeOfDay')}\n"
        )
        if rec.get('IsRoundTrip'):
            text += f"Обратный маршрут: {rec.get('ReturnRoute')}\n"
            text += f"Дата возврата: {rec.get('ReturnDate')}\n"
        text += (
            f"Причина: {rec.get('Reason')}\n"
            f"Статус: {rec.get('Status')}\n"
        )
        for line in text.splitlines():
            c.drawString(40, y, line)
            y -= 14
            if y < 80:
                c.showPage()
                y = height - 50
        y -= 6
    c.save()
    buffer.seek(0)
    return buffer


def cmd_report_user(update: Update, context: CallbackContext):
    if update.effective_user.id != ADMIN_ID:
        update.message.reply_text("⛔ Только админ.")
        return
    if not context.args:
        update.message.reply_text("Использование: /report_user <user_id>")
        return
    try:
        uid = int(context.args[0])
    except:
        update.message.reply_text("user_id должен быть числом.")
        return
    apps = safe_read(APPLICATIONS_FILE)
    recs = apps[apps["UserID"] == uid].to_dict("records")
    if not recs:
        update.message.reply_text("Нет заявок у пользователя.")
        return
    pdf = generate_pdf_report_applications(recs, title=f"Отчёт — заявки пользователя {uid}")
    update.message.reply_document(document=BytesIO(pdf.read()), filename=f"report_user_{uid}.pdf")


def cmd_report_period(update: Update, context: CallbackContext):
    if update.effective_user.id != ADMIN_ID:
        update.message.reply_text("⛔ Только админ.")
        return
    if not context.args:
        update.message.reply_text("Использование: /report_period YYYY-MM")
        return
    period = context.args[0]
    try:
        dt = datetime.strptime(period, "%Y-%m")
    except:
        update.message.reply_text("Неверный формат. Пример: 2025-09")
        return
    apps = safe_read(APPLICATIONS_FILE)
    if apps.empty:
        update.message.reply_text("Нет заявок.")
        return
    apps["TS"] = pd.to_datetime(apps["Timestamp"], errors="coerce")
    recs = apps[(apps["TS"].dt.year == dt.year) & (apps["TS"].dt.month == dt.month)].to_dict("records")
    if not recs:
        update.message.reply_text("Нет заявок в указанный период.")
        return
    pdf = generate_pdf_report_applications(recs, title=f"Отчёт заявок {period}")
    update.message.reply_document(document=BytesIO(pdf.read()), filename=f"report_{period}.pdf")


# -------------------------
# Admin dashboard & status set
# -------------------------
def cmd_dashboard(update: Update, context: CallbackContext):
    if update.effective_user.id != ADMIN_ID:
        update.message.reply_text("⛔ Только админ.")
        return
    days = 14
    apps = safe_read(APPLICATIONS_FILE)
    hotels = safe_read(HOTELS_FILE)
    now = datetime.now().date()
    end = now + timedelta(days=days)
    lines = [f"📊 Доска поездок на {days} дней ({now} → {end})\n"]
    if not apps.empty:
        for _, r in apps.iterrows():
            try:
                d = datetime.strptime(str(r.get("Date", "")), "%d.%m.%Y").date()
            except:
                continue
            if now <= d <= end:
                route_info = f"{r.get('Route', '—')}"
                if r.get('IsRoundTrip'):
                    route_info += " 🔄"
                lines.append(f"✈ #{r.get('ID')} {r.get('FIO')} → {route_info} ({r.get('Date')}) {r.get('Status')}")
    if not hotels.empty:
        for _, r in hotels.iterrows():
            try:
                d = datetime.strptime(str(r.get("CheckIn", "")), "%d.%m.%Y").date()
            except:
                continue
            if now <= d <= end:
                lines.append(
                    f"🏨 H#{r.get('ID')} {r.get('FIO')} → {r.get('HotelCity')} (заезд {r.get('CheckIn')}) {r.get('Status')}")
    if len(lines) == 1:
        update.message.reply_text("План поездок пуст на ближайшие 14 дней.")
    else:
        for i in range(0, len(lines), 20):
            update.message.reply_text("\n".join(lines[i:i + 20]))


def cmd_set_status(update: Update, context: CallbackContext):
    if update.effective_user.id != ADMIN_ID:
        update.message.reply_text("⛔ Только админ.")
        return
    if len(context.args) < 3:
        update.message.reply_text("Использование: /set_status app|hotel <id> <status>")
        return
    typ = context.args[0]
    try:
        sid = int(context.args[1])
    except:
        update.message.reply_text("ID должен быть числом.")
        return
    status = " ".join(context.args[2:])
    if typ == "app":
        df = safe_read(APPLICATIONS_FILE)
        idx = df.index[df["ID"] == sid].tolist()
        if not idx:
            update.message.reply_text("Заявка не найдена.")
        else:
            i = idx[0]
            df.at[i, "Status"] = status
            safe_write(df, APPLICATIONS_FILE)
            user_id = int(df.at[i, "UserID"])
            try:
                context.bot.send_message(user_id, f"🔔 Статус вашей заявки #{sid} изменён: {status}")
            except Exception:
                logger.exception("notify user status change failed")
            update.message.reply_text(f"Статус заявки #{sid} изменён на: {status}")
    elif typ == "hotel":
        df = safe_read(HOTELS_FILE)
        idx = df.index[df["ID"] == sid].tolist()
        if not idx:
            update.message.reply_text("Бронирование не найдено.")
        else:
            i = idx[0]
            df.at[i, "Status"] = status
            safe_write(df, HOTELS_FILE)
            user_id = int(df.at[i, "UserID"])
            try:
                context.bot.send_message(user_id, f"🔔 Статус брони отеля #{sid} изменён: {status}")
            except Exception:
                logger.exception("notify user hotel status change failed")
            update.message.reply_text(f"Статус брони #{sid} изменён на: {status}")
    else:
        update.message.reply_text("Тип должен быть app или hotel.")


# -------------------------
# Admin forwarding & get db
# -------------------------
def cmd_send_ticket(update: Update, context: CallbackContext):
    if update.effective_user.id != ADMIN_ID:
        update.message.reply_text("⛔ Только админ.")
        return
    if not context.args:
        update.message.reply_text("Использование: /send_ticket <user_id>")
        return
    try:
        target = int(context.args[0])
    except:
        update.message.reply_text("user_id должен быть числом.")
        return
    admin_forwarding[update.effective_user.id] = target
    update.message.reply_text(
        f"Режим пересылки активирован для пользователя {target}. Отправьте фото/документ/текст. /done чтобы завершить.")


def handler_forward_any(update: Update, context: CallbackContext):
    admin_id = update.effective_user.id
    if admin_id not in admin_forwarding:
        return
    target = admin_forwarding[admin_id]
    try:
        if update.message.document:
            context.bot.send_document(chat_id=target, document=update.message.document.file_id,
                                      caption=update.message.caption)
        elif update.message.photo:
            context.bot.send_photo(chat_id=target, photo=update.message.photo[-1].file_id,
                                   caption=update.message.caption)
        elif update.message.text:
            context.bot.send_message(chat_id=target, text=update.message.text)
        update.message.reply_text(f"✅ Переслано пользователю {target}")
    except Exception:
        logger.exception("handler_forward_any failed")
        update.message.reply_text("❌ Ошибка при пересылке.")


def cmd_done(update: Update, context: CallbackContext):
    aid = update.effective_user.id
    if aid in admin_forwarding:
        del admin_forwarding[aid]
        update.message.reply_text("✅ Режим пересылки завершён.")
    else:
        update.message.reply_text("Режим пересылки не активен.")


def cmd_get_db(update: Update, context: CallbackContext):
    if update.effective_user.id != ADMIN_ID:
        update.message.reply_text("⛔ Только админ.")
        return
    try:
        if os.path.exists(APPLICATIONS_FILE):
            update.message.reply_document(document=open(APPLICATIONS_FILE, "rb"), caption="applications.xlsx")
        if os.path.exists(HOTELS_FILE):
            update.message.reply_document(document=open(HOTELS_FILE, "rb"), caption="hotels.xlsx")
        if os.path.exists(USERS_FILE):
            update.message.reply_document(document=open(USERS_FILE, "rb"), caption="users.xlsx")
    except Exception:
        logger.exception("cmd_get_db failed")
        update.message.reply_text("❌ Ошибка отправки файлов.")


# -------------------------
# Help text
# -------------------------
HELP_TEXT = (
    "ℹ️ <b>Как заполнить заявку</b>:\n\n"
    "1) Нажмите <b>✈ Новая заявка</b> или /start\n"
    "2) Введите ФИО или используйте автозаполнение\n"
    "3) Прикрепите фото/скан паспорта\n"
    "4) Выберите маршрут из списка или введите свой\n"
    "5) Укажите дату поездки (ДД.MM.ГГГГ, можно 'утром'/'вечером')\n"
    "6) Укажите нужен ли обратный билет\n"
    "7) Укажите причину поездки\n\n"
    "После подтверждения заявка отправляется админу и сохраняется в базе.\n"
)

HELP_TEXT_ADMIN = (
    "🛠️ <b>Команды администратора</b>:\n\n"
    "/admin_all - Все заявки с кнопками управления\n"
    "/admin_pending - Только заявки на рассмотрении\n"
    "/admin_search - Поиск заявок\n"
    "/dashboard - План поездок на 14 дней\n"
    "/set_status - Изменить статус вручную\n"
    "/send_ticket - Пересылка документов пользователю\n"
    "/get_db - Получить файлы базы данных\n"
    "/report_user - Отчет по пользователю\n"
    "/report_period - Отчет за период\n"
)


def send_help(update: Update, context: CallbackContext):
    if update.message:
        if update.effective_user.id == ADMIN_ID:
            update.message.reply_html(HELP_TEXT_ADMIN)
        update.message.reply_html(HELP_TEXT)


# -------------------------
# NEW ENHANCED FEATURES
# -------------------------
def cb_change_status(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    try:
        _, item_type, item_id, status_key = q.data.split(":")
        item_id = int(item_id)
        new_status = STATUSES[status_key]

        if item_type == "app":
            df = safe_read(APPLICATIONS_FILE)
            row = df[df["ID"] == item_id]
            if not row.empty:
                df.loc[df["ID"] == item_id, "Status"] = new_status
                safe_write(df, APPLICATIONS_FILE)

                notify_user_status_change(context, row.iloc[0]["UserID"], "app", item_id, new_status)
                # Need to reload row content to show updated status
                row2 = df[df["ID"] == item_id].iloc[0].to_dict()
                card, kb = format_application_card(row2, for_admin=True)
                q.message.edit_text(card, parse_mode="HTML", reply_markup=kb)

        elif item_type == "hotel":
            df = safe_read(HOTELS_FILE)
            row = df[df["ID"] == item_id]
            if not row.empty:
                df.loc[df["ID"] == item_id, "Status"] = new_status
                safe_write(df, HOTELS_FILE)

                notify_user_status_change(context, row.iloc[0]["UserID"], "hotel", item_id, new_status)
                row2 = df[df["ID"] == item_id].iloc[0].to_dict()
                card, kb = format_hotel_card(row2, for_admin=True)
                q.message.edit_text(card, parse_mode="HTML", reply_markup=kb)

    except Exception as e:
        logger.error("cb_change_status failed: %s", e)
        q.message.reply_text("❌ Ошибка при изменении статуса")


def cb_add_comment(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    try:
        _, item_type, item_id = q.data.split(":")
        context.user_data['comment_item'] = (item_type, int(item_id))
        kb = [
            [InlineKeyboardButton("📝 Публичный", callback_data=f"comment_type:{item_type}:{item_id}:public")],
            [InlineKeyboardButton("🔒 Внутренний", callback_data=f"comment_type:{item_type}:{item_id}:internal")],
            [InlineKeyboardButton("❌ Отмена", callback_data=f"comment_cancel:{item_type}:{item_id}")]
        ]
        q.message.reply_text("Выберите тип комментария:", reply_markup=InlineKeyboardMarkup(kb))
    except Exception as e:
        logger.error("cb_add_comment failed: %s", e)


def cb_comment_type(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    try:
        _, item_type, item_id, comment_type = q.data.split(":")
        context.user_data['comment_type'] = comment_type
        context.user_data['comment_item'] = (item_type, int(item_id))
        q.message.reply_text("Введите комментарий:", reply_markup=ReplyKeyboardRemove())
        return COMMENT
    except Exception as e:
        logger.error("cb_comment_type failed: %s", e)
        return ConversationHandler.END


def handle_comment(update: Update, context: CallbackContext):
    comment_text = update.message.text
    item_type, item_id = context.user_data.get('comment_item', (None, None))
    comment_type = context.user_data.get('comment_type', 'public')

    if item_type and item_id:
        is_internal = (comment_type == 'internal')
        comment_id = add_comment(item_type, item_id, update.effective_user.id, comment_text, is_internal)

        # Notify user if public comment
        if not is_internal:
            if item_type == "app":
                df = safe_read(APPLICATIONS_FILE)
            else:
                df = safe_read(HOTELS_FILE)

            row = df[df["ID"] == item_id]
            if not row.empty:
                notify_user_status_change(
                    context, row.iloc[0]["UserID"], item_type, item_id,
                    "Новый комментарий", comment_text
                )

        update.message.reply_text(f"✅ Комментарий #{comment_id} добавлен")

    return ConversationHandler.END


def job_check_reminders(context: CallbackContext):
    """Job wrapper for job queue"""
    # job callback signature receives context only
    check_reminders(context)


def check_reminders(context: CallbackContext):
    """Check and send reminders for upcoming trips"""
    logger.info("Checking reminders...")

    # Check applications
    apps_df = safe_read(APPLICATIONS_FILE)
    if not apps_df.empty:
        for _, app in apps_df.iterrows():
            try:
                app_date = datetime.strptime(app["Date"], "%d.%m.%Y")
                if timedelta(days=3) <= (app_date - datetime.now()) <= timedelta(days=4):
                    send_reminder(context, app["UserID"], "app", app["ID"], app["Date"])
            except:
                continue

    # Check hotels
    hotels_df = safe_read(HOTELS_FILE)
    if not hotels_df.empty:
        for _, hotel in hotels_df.iterrows():
            try:
                checkin_date = datetime.strptime(hotel["CheckIn"], "%d.%m.%Y")
                if timedelta(days=3) <= (checkin_date - datetime.now()) <= timedelta(days=4):
                    send_reminder(context, hotel["UserID"], "hotel", hotel["ID"], hotel["CheckIn"])
            except:
                continue


def cmd_dashboard_admin(update: Update, context: CallbackContext):
    """Admin dashboard command"""
    # This duplicates cmd_dashboard but kept for compatibility if needed
    cmd_dashboard(update, context)


# -------------------------
# MAIN APPLICATION SETUP
# -------------------------
def main():
    """Main application setup"""
    # Initialize files
    init_files()

    # Updater & Dispatcher (PTB v13)
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    # Job queue: run daily check at 09:00
    jq = updater.job_queue
    try:
        jq.run_daily(job_check_reminders, time=dt_time(hour=9, minute=0))
    except Exception:
        # in some environments timezone/time issues may appear; fallback to interval
        jq.run_repeating(job_check_reminders, interval=24 * 3600, first=10)

    # Conversation handler for main flows
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", cmd_start),
            CallbackQueryHandler(cb_start_app, pattern="^start_app$"),
            CallbackQueryHandler(cb_start_hotel, pattern="^start_hotel$"),
            CallbackQueryHandler(cb_my_requests, pattern="^my_requests$"),
            MessageHandler(Filters.regex(r"^✈ Новая заявка$"), cb_start_app),
            MessageHandler(Filters.regex(r"^🏨 Забронировать отель$"), cb_start_hotel),
        ],
        states={
            NAME: [MessageHandler(Filters.text & (~Filters.command), flow_name)],
            PASSPORT: [MessageHandler(Filters.photo | Filters.document | (Filters.text & (~Filters.command)),
                                      flow_passport)],
            ROUTE: [
                CallbackQueryHandler(handle_route_selection, pattern="^route_"),
                MessageHandler(Filters.text & (~Filters.command), flow_route)
            ],
            DATE_STR: [MessageHandler(Filters.text & (~Filters.command), flow_date)],
            RETURN_DATE: [
                CallbackQueryHandler(handle_return_ticket, pattern="^return_"),
                MessageHandler(Filters.text & (~Filters.command), flow_return_date)
            ],
            REASON: [MessageHandler(Filters.text & (~Filters.command), flow_reason)],
            CONFIRM: [CallbackQueryHandler(cb_confirm_app, pattern="^confirm_app$"),
                      CallbackQueryHandler(cb_cancel_app, pattern="^cancel_app$")],
            HOTEL_CITY: [MessageHandler(Filters.text & (~Filters.command), flow_hotel_city)],
            HOTEL_DATERANGE: [MessageHandler(Filters.text & (~Filters.command), flow_hotel_daterange)],
            HOTEL_ROOM_TYPE: [CallbackQueryHandler(cb_hotel_room_type, pattern="^room_.*")],
            COMMENT: [MessageHandler(Filters.text & (~Filters.command), handle_comment)],
        },
        fallbacks=[CommandHandler("start", cmd_start)],
        allow_reentry=True,
    )

    dp.add_handler(conv_handler)

    # Inline/callbacks
    dp.add_handler(CallbackQueryHandler(cb_page_view, pattern=r"^(page:|view_app:|view_hotel:)"))
    dp.add_handler(CallbackQueryHandler(cb_cancel_app_by_id, pattern=r"^cancel_app:"))
    dp.add_handler(CallbackQueryHandler(cb_cancel_hotel_by_id, pattern=r"^cancel_hotel:"))
    dp.add_handler(CallbackQueryHandler(cb_hotel_room_type, pattern=r"^room_.*$"))
    dp.add_handler(CallbackQueryHandler(cb_start_app, pattern="^start_app$"))
    dp.add_handler(CallbackQueryHandler(cb_start_hotel, pattern="^start_hotel$"))
    dp.add_handler(CallbackQueryHandler(cb_my_requests, pattern="^my_requests$"))
    dp.add_handler(CallbackQueryHandler(cb_confirm_app, pattern="^confirm_app$"))
    dp.add_handler(CallbackQueryHandler(cb_cancel_app, pattern="^cancel_app$"))

    # Reply keyboard text handlers
    dp.add_handler(MessageHandler(Filters.regex(r"^📝 Мои заявки$"), cb_my_requests))
    dp.add_handler(MessageHandler(Filters.regex(r"^ℹ Помощь$"), send_help))

    # Search & admin commands
    dp.add_handler(CommandHandler("search_date", cmd_search_user_date))
    dp.add_handler(CommandHandler("search_city", cmd_search_user_city))
    dp.add_handler(CommandHandler("admin_search", cmd_admin_search))
    dp.add_handler(CommandHandler("report_user", cmd_report_user))
    dp.add_handler(CommandHandler("report_period", cmd_report_period))
    dp.add_handler(CommandHandler("dashboard", cmd_dashboard))
    dp.add_handler(CommandHandler("set_status", cmd_set_status))
    dp.add_handler(CommandHandler("get_db", cmd_get_db))

    # NEW ADMIN COMMANDS
    dp.add_handler(CommandHandler("admin_all", cmd_admin_all))
    dp.add_handler(CommandHandler("admin_pending", cmd_admin_pending))

    # Admin forwarding handlers
    dp.add_handler(CommandHandler("send_ticket", cmd_send_ticket))
    dp.add_handler(CommandHandler("done", cmd_done))
    # this catches messages for forwarding — it returns quickly if admin not in forwarding mode
    dp.add_handler(MessageHandler(Filters.all & (~Filters.command), handler_forward_any))

    # New enhanced features handlers
    dp.add_handler(CallbackQueryHandler(cb_change_status, pattern=r"^status:"))
    dp.add_handler(CallbackQueryHandler(cb_add_comment, pattern=r"^comment:"))
    dp.add_handler(CallbackQueryHandler(cb_comment_type, pattern=r"^comment_type:"))

    # Help & misc
    dp.add_handler(CommandHandler("reminders", lambda u, c: (check_reminders(c), c.bot.send_message(chat_id=u.effective_user.id, text="Проверка запущена"))))

    def cb_archive_item(update: Update, context: CallbackContext):
        q = update.callback_query
        q.answer()

        if update.effective_user.id != ADMIN_ID:
            q.message.reply_text("⛔ Только админ может архивировать записи.")
            return

        try:
            _, item_type, item_id = q.data.split(":")
            item_id = int(item_id)

            success = archive_item(item_type, item_id)
            if success:
                q.message.reply_text(f"✅ {item_type.capitalize()} #{item_id} успешно архивировано.")

                # Обновляем карточку или удаляем сообщение, если нужно
                if item_type == "app":
                    df = safe_read(APPLICATIONS_FILE)
                    row = df[df["ID"] == item_id]
                    if row.empty:  # Проверяем, что запись действительно удалена
                        q.message.edit_text(f"Заявка #{item_id} архивирована.")
                elif item_type == "hotel":
                    df = safe_read(HOTELS_FILE)
                    row = df[df["ID"] == item_id]
                    if row.empty:
                        q.message.edit_text(f"Бронирование #{item_id} архивировано.")
            else:
                q.message.reply_text(f"❌ Не удалось архивировать {item_type} #{item_id}. Запись не найдена.")
        except Exception as e:
            logger.error("cb_archive_item failed: %s", e)
            q.message.reply_text("❌ Ошибка при архивации.")

    # Добавить в список обработчиков
    dp.add_handler(CallbackQueryHandler(cb_archive_item, pattern=r"^archive:"))

    def cmd_clear_db(update: Update, context: CallbackContext):
        if update.effective_user.id != ADMIN_ID:
            update.message.reply_text("⛔ Только админ может очищать базу данных.")
            return

        # Отправляем запрос на подтверждение
        kb = [
            [InlineKeyboardButton("✅ Подтвердить очистку", callback_data="clear_db_confirm")],
            [InlineKeyboardButton("❌ Отменить", callback_data="clear_db_cancel")]
        ]
        update.message.reply_text(
            "⚠️ Внимание! Это действие удалит все данные из баз (заявки, бронирования, пользователи, архив, комментарии, шаблоны). Продолжить?",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    def cb_clear_db(update: Update, context: CallbackContext):
        q = update.callback_query
        q.answer()

        if update.effective_user.id != ADMIN_ID:
            q.message.reply_text("⛔ Только админ.")
            return

        if q.data == "clear_db_confirm":
            try:
                # Список файлов для очистки
                files_to_clear = [
                    APPLICATIONS_FILE,
                    HOTELS_FILE,
                    USERS_FILE,
                    ARCHIVE_FILE,
                    COMMENTS_FILE,
                    TEMPLATES_FILE
                ]

                # Очищаем каждый файл, сохраняя структуру
                for file_path in files_to_clear:
                    if os.path.exists(file_path):
                        df = safe_read(file_path)
                        if not df.empty:
                            # Создаем пустой DataFrame с теми же столбцами
                            empty_df = pd.DataFrame(columns=df.columns)
                            safe_write(empty_df, file_path)
                            logger.info("Cleared file %s", file_path)

                # Очищаем кэш
                cache.clear()
                logger.info("Cache cleared")
                q.message.reply_text("✅ База данных успешно очищена.")
            except Exception as e:
                logger.error("cmd_clear_db failed: %s", e)
                q.message.reply_text("❌ Ошибка при очистке базы данных.")
        else:
            q.message.reply_text("❌ Очистка базы данных отменена.")

    # Добавить в список обработчиков
    dp.add_handler(CommandHandler("clear_db", cmd_clear_db))
    dp.add_handler(CallbackQueryHandler(cb_clear_db, pattern=r"^clear_db_"))

    logger.info("Bot started with enhanced features (PTB v13)")
    print("🤖 Bot started with enhanced features (PTB v13)!")

    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
import os
# Set environment variables to disable proxy before importing supabase
# This prevents httpx.Client from receiving 'proxy' argument which is not supported in newer httpx versions
os.environ.setdefault("NO_PROXY", "*")
os.environ.setdefault("HTTPX_NO_PROXY", "1")

# Configure logging
import logging
DEBUG_MODE = os.environ.get('DEBUG', 'false').lower() == 'true'
log_level = logging.DEBUG if DEBUG_MODE else logging.INFO
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=log_level
)
logger = logging.getLogger(__name__)

# Try to monkeypatch httpx.Client to intercept proxy argument (silent in production)
try:
    import httpx
    
    # Store original Client.__init__
    original_httpx_client_init = httpx.Client.__init__
    
    def patched_httpx_client_init(self, *args, **kwargs):
        """Patched httpx.Client.__init__ to remove proxy argument"""
        if 'proxy' in kwargs:
            if DEBUG_MODE:
                logger.warning(f"DEBUG: httpx.Client.__init__ called with proxy={kwargs['proxy']}, removing it")
            kwargs.pop('proxy')
        return original_httpx_client_init(self, *args, **kwargs)
    
    # Apply monkeypatch
    httpx.Client.__init__ = patched_httpx_client_init
    if DEBUG_MODE:
        logger.info("DEBUG: httpx.Client monkeypatch applied successfully")
except Exception as e:
    if DEBUG_MODE:
        logger.warning(f"DEBUG: Failed to apply httpx.Client monkeypatch: {e}")

from datetime import datetime
import time
import signal
import sys
from functools import wraps
from flask import Flask, request, jsonify
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes, ConversationHandler

from supabase import create_client, Client

# Initialize Flask app
app = Flask(__name__)

# Environment variables - all required except PORT (set by Koyeb)
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
WEBHOOK_URL = os.environ.get('WEBHOOK_URL')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
TABLE_NAME = os.environ.get('TABLE_NAME', 'facebook_leads')  # Default table name
PORT = int(os.environ.get('PORT', 8000))  # Default port, usually set by Koyeb

# Supabase client
supabase: Client = None

# Cache for uniqueness checks (TTL: 5 minutes)
uniqueness_cache = {}
CACHE_TTL = 300  # 5 minutes in seconds

# Graceful shutdown flag
shutdown_requested = False

def retry_supabase_query(max_retries=3, delay=1, backoff=2):
    """Decorator for retrying Supabase queries with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    error_msg = str(e).lower()
                    
                    # Only retry on network/temporary errors
                    if any(keyword in error_msg for keyword in ['timeout', 'connection', 'network', 'temporary', '503', '502', '504']):
                        if attempt < max_retries - 1:
                            logger.warning(f"Supabase query failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {current_delay}s...")
                            time.sleep(current_delay)
                            current_delay *= backoff
                        else:
                            logger.error(f"Supabase query failed after {max_retries} attempts: {e}")
                    else:
                        # Non-retryable error, raise immediately
                        raise
            
            # If all retries failed, raise the last exception
            raise last_exception
        return wrapper
    return decorator

def get_supabase_client():
    """Initialize and return Supabase client"""
    global supabase
    if supabase is None:
        try:
            if DEBUG_MODE:
                logger.info("DEBUG: Starting Supabase client initialization...")
            
            if not SUPABASE_KEY:
                logger.error("SUPABASE_KEY not found in environment variables")
                return None
            
            if not SUPABASE_URL:
                logger.error("SUPABASE_URL not found in environment variables")
                return None
            
            # Create Supabase client - environment variables are set at module level to prevent proxy issues
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            
            logger.info("Supabase client initialized successfully")
        except TypeError as e:
            error_msg = str(e)
            if DEBUG_MODE:
                logger.error(f"DEBUG: TypeError in get_supabase_client: {error_msg}")
                logger.error(f"DEBUG: Error type: {type(e)}")
                logger.error(f"DEBUG: Error args: {e.args}")
                if "proxy" in error_msg.lower():
                    logger.error("DEBUG: Proxy-related error detected!")
                    try:
                        import inspect
                        import httpx
                        sig = inspect.signature(httpx.Client.__init__)
                        logger.error(f"DEBUG: httpx.Client.__init__ signature: {sig}")
                    except Exception:
                        pass
            logger.error(f"Error initializing Supabase client: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Error initializing Supabase client: {e}", exc_info=True)
            return None
    return supabase

def normalize_phone(phone: str) -> str:
    """Normalize phone number: remove all non-digit characters"""
    if not phone:
        return ""
    # Remove all non-digit characters
    return ''.join(filter(str.isdigit, phone))

def escape_html(text: str) -> str:
    """Escape HTML special characters"""
    if not text:
        return text
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

import re
from urllib.parse import urlparse, parse_qs

def validate_phone(phone: str) -> tuple[bool, str, str]:
    """Validate phone number: minimum 10 digits, maximum 15 digits (with country code)"""
    normalized = normalize_phone(phone)
    if not normalized:
        return False, "Номер телефона не может быть пустым", ""
    if len(normalized) < 10:
        return False, "Номер телефона должен содержать минимум 10 цифр (с кодом страны)", ""
    if len(normalized) > 15:
        return False, "Номер телефона не может содержать более 15 цифр", ""
    return True, "", normalized

def validate_email(email: str) -> tuple[bool, str]:
    """Validate email format"""
    if not email:
        return False, "Email не может быть пустым"
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False, "Неверный формат email. Пример: user@example.com"
    return True, ""

def validate_facebook_id(fb_id: str) -> tuple[bool, str]:
    """Validate Facebook ID: only digits"""
    if not fb_id:
        return False, "Facebook ID не может быть пустым"
    if not fb_id.isdigit():
        return False, "Facebook ID должен содержать только цифры"
    return True, ""

def validate_facebook_link(link: str) -> tuple[bool, str, str]:
    """
    Validate Facebook link and extract username or profile.php?id= format.
    Supports various Facebook URL formats:
    - https://www.facebook.com/profile.php?id=123456 → profile.php?id=123456
    - https://www.facebook.com/markl1n → markl1n
    - https://www.facebook.com/profile/username → username
    - https://m.facebook.com/username → username
    - facebook.com/username → username
    """
    if not link:
        return False, "Facebook ссылка не может быть пустой", ""
    
    link_clean = link.strip()
    
    # Remove @ if present at the beginning
    if link_clean.startswith('@'):
        link_clean = link_clean[1:]
    
    # Check if it's a valid Facebook URL
    facebook_patterns = [
        r'https?://(www\.)?(m\.)?facebook\.com/',
        r'^facebook\.com/',
        r'^m\.facebook\.com/'
    ]
    
    is_facebook_url = False
    for pattern in facebook_patterns:
        if re.search(pattern, link_clean, re.IGNORECASE):
            is_facebook_url = True
            break
    
    if not is_facebook_url and not link_clean.startswith('facebook.com/') and not link_clean.startswith('m.facebook.com/'):
        return False, "Неверный формат Facebook ссылки. Пример: https://www.facebook.com/username", ""
    
    # Remove http:// or https:// if present
    if link_clean.startswith('http://'):
        link_clean = link_clean[7:]
    elif link_clean.startswith('https://'):
        link_clean = link_clean[8:]
    
    # Remove www. if present
    if link_clean.startswith('www.'):
        link_clean = link_clean[4:]
    
    # Remove facebook.com/ or m.facebook.com/ if present
    if link_clean.lower().startswith('facebook.com/'):
        link_clean = link_clean[13:]
    elif link_clean.lower().startswith('m.facebook.com/'):
        link_clean = link_clean[15:]
    
    # Handle profile.php?id= format
    if 'profile.php' in link_clean:
        # Extract ID from query string
        if 'id=' in link_clean:
            id_part = link_clean.split('id=')[-1].split('&')[0]
            extracted = f"profile.php?id={id_part}"
            return True, "", extracted
    
    # For username format: extract just the username (last part after /)
    # Remove query parameters if present
    if '?' in link_clean:
        link_clean = link_clean.split('?')[0]
    
    # Remove trailing slash
    link_clean = link_clean.rstrip('/')
    
    # Extract username (last part after /)
    parts = link_clean.split('/')
    if len(parts) > 0:
        # Get the last non-empty part (username)
        extracted = parts[-1] if parts[-1] else (parts[-2] if len(parts) > 1 else "")
        if extracted:
            return True, "", extracted
    
    return False, "Неверный формат Facebook ссылки", ""

def validate_telegram_name(tg_name: str) -> tuple[bool, str, str]:
    """Validate Telegram name: remove @ if present, check not empty"""
    if not tg_name:
        return False, "Telegram Name не может быть пустым", ""
    normalized = tg_name.strip()
    if normalized.startswith('@'):
        normalized = normalized[1:]
    if not normalized:
        return False, "Telegram Name не может быть пустым", ""
    return True, "", normalized

def validate_telegram_id(tg_id: str) -> tuple[bool, str]:
    """Validate Telegram ID: only digits, minimum 1 digit"""
    if not tg_id:
        return False, "Telegram ID не может быть пустым"
    normalized = tg_id.strip()
    if not normalized.isdigit():
        return False, "Telegram ID должен содержать только цифры"
    if len(normalized) < 1:
        return False, "Telegram ID должен содержать минимум 1 цифру"
    return True, ""

def get_field_format_requirements(field_name: str) -> str:
    """Get format requirements description for a field"""
    requirements = {
        'fullname': (
            "⚠️ Требования к формату:\n"
            "• Поле не может быть пустым"
        ),
        'manager_name': (
            "⚠️ Требования к формату:\n"
            "• Поле не может быть пустым"
        ),
        'phone': (
            "⚠️ Требования к формату:\n"
            "• Обязателен код страны\n"
            "• Только цифры (без пробелов)\n"
            "• Без знака '+' в начале\n"
            "• От 10 до 15 цифр\n\n"
            "Примеры: 79001234567, 380501234567"
        ),
        'facebook_link': (
            "⚠️ Требования к формату:\n"
            "• Должен быть валидной Facebook ссылкой\n"
            "• Может быть полная ссылка или username\n\n"
            "Примеры: https://www.facebook.com/username, facebook.com/username"
        ),
        'telegram_name': (
            "⚠️ Требования к формату:\n"
            "• Только текст (без @)\n"
            "• Без пробелов\n\n"
            "Примеры: username, myname"
        ),
        'telegram_id': (
            "⚠️ Требования к формату:\n"
            "• Только цифры (без букв и символов)\n"
            "• Без пробелов\n"
            "• Минимум 1 цифра\n\n"
            "Примеры: 12345, 789, 999888777"
        ),
        'email': (
            "⚠️ Требования к формату:\n"
            "• Стандартный формат email\n\n"
            "Примеры: user@example.com"
        ),
        'country': (
            "⚠️ Требования к формату:\n"
            "• Поле не может быть пустым"
        )
    }
    return requirements.get(field_name, "")

def get_field_label(field_name: str) -> str:
    """Get Russian label for field"""
    labels = {
        'fullname': 'Имя Фамилия',
        'manager_name': 'Агент',
        'phone': 'Номер телефона',
        'facebook_link': 'Facebook Link',
        'telegram_name': 'Telegram Name',
        'telegram_id': 'Telegram ID',
        'email': 'Email',
        'country': 'Country'
    }
    return labels.get(field_name, field_name)

def get_next_add_field(current_field: str) -> tuple[str, int]:
    """Get next field in the add flow"""
    field_sequence = [
        ('fullname', ADD_FULLNAME),
        ('manager_name', ADD_MANAGER_NAME),
        ('phone', ADD_PHONE),
        ('facebook_link', ADD_FB_LINK),
        ('telegram_name', ADD_TELEGRAM_NAME),
        ('telegram_id', ADD_TELEGRAM_ID),
        ('email', ADD_EMAIL),
        ('country', ADD_COUNTRY),
    ]
    
    if not current_field:
        return field_sequence[0]
    
    for i, (field, state) in enumerate(field_sequence):
        if field == current_field:
            if i + 1 < len(field_sequence):
                return field_sequence[i + 1]
            else:
                return ('review', ADD_REVIEW)
    
    return field_sequence[0]

def get_navigation_keyboard(is_optional: bool = False, show_back: bool = True) -> InlineKeyboardMarkup:
    """Get navigation keyboard for field input"""
    keyboard = []
    
    if is_optional:
        keyboard.append([InlineKeyboardButton("⏭️ Пропустить", callback_data="add_skip")])
    
    if show_back:
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="add_back")])
    
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
    
    return InlineKeyboardMarkup(keyboard)

# Facebook username validation removed - using only Facebook Link now

# Conversation states
(
    # Check states
    CHECK_BY_TELEGRAM,
    CHECK_BY_FB_LINK,
    CHECK_BY_FB_USERNAME,
    CHECK_BY_FB_ID,
    CHECK_BY_PHONE,
    CHECK_BY_FULLNAME,
    # Add states (sequential flow)
    ADD_FULLNAME,
    ADD_MANAGER_NAME,
    ADD_PHONE,
    ADD_FB_LINK,
    ADD_TELEGRAM_NAME,
    ADD_TELEGRAM_ID,
    ADD_EMAIL,
    ADD_COUNTRY,
    ADD_REVIEW,  # Review before saving
    # Edit states
    EDIT_MENU,
    EDIT_FULLNAME,
    EDIT_PHONE,
    EDIT_EMAIL,
    EDIT_COUNTRY,
    EDIT_FB_LINK,
    EDIT_TELEGRAM_NAME,
    EDIT_TELEGRAM_ID,
    EDIT_MANAGER_NAME
) = range(24)

# Store user data during conversation
user_data_store = {}
# Track last access time for memory optimization
user_data_store_access_time = {}
USER_DATA_STORE_TTL = 3600  # 1 hour in seconds
USER_DATA_STORE_MAX_SIZE = 1000  # Maximum number of entries

# Main menu keyboard
def get_main_menu_keyboard():
    """Create main menu keyboard"""
    keyboard = [
        [InlineKeyboardButton("✅ Проверить", callback_data="check_menu")],
        [InlineKeyboardButton("➕ Добавить", callback_data="add_new")]
    ]
    return InlineKeyboardMarkup(keyboard)

# Check menu keyboard
def get_check_menu_keyboard():
    """Create check menu keyboard with all search options"""
    keyboard = [
        [InlineKeyboardButton("📱 Telegram", callback_data="check_telegram")],
        [InlineKeyboardButton("🔗 Facebook Link", callback_data="check_fb_link")],
        [InlineKeyboardButton("👤 Facebook Username", callback_data="check_fb_username")],
        [InlineKeyboardButton("🆔 Facebook ID", callback_data="check_fb_id")],
        [InlineKeyboardButton("🔢 Phone", callback_data="check_phone")],
        [InlineKeyboardButton("👤 Full Name", callback_data="check_fullname")],
        [InlineKeyboardButton("◀️ Назад", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

# Add menu keyboard
def get_add_menu_keyboard():
    """Create add menu keyboard"""
    keyboard = [
        [InlineKeyboardButton("➕ Добавить новый лид", callback_data="add_new")],
        [InlineKeyboardButton("◀️ Назад", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_add_field_keyboard(user_id: int):
    """Create keyboard for adding fields - shows which fields are already filled"""
    user_data = user_data_store.get(user_id, {})
    keyboard = []
    
    # Обязательные поля - серый круг по умолчанию, зеленый круг когда заполнено
    fullname_status = "🟢" if user_data.get('fullname') else "⚪"
    manager_status = "🟢" if user_data.get('manager_name') else "⚪"
    
    keyboard.append([InlineKeyboardButton(f"{fullname_status} Имя Фамилия *", callback_data="add_field_fullname")])
    keyboard.append([InlineKeyboardButton(f"{manager_status} Агент *", callback_data="add_field_manager")])
    
    # Обязательные идентификаторы (минимум один) - серый круг по умолчанию, зеленый круг когда заполнено
    phone_status = "🟢" if user_data.get('phone') else "⚪"
    fb_link_status = "🟢" if user_data.get('facebook_link') else "⚪"
    telegram_status = "🟢" if user_data.get('telegram_user') else "⚪"
    fb_username_status = "🟢" if user_data.get('facebook_username') else "⚪"
    fb_id_status = "🟢" if user_data.get('facebook_id') else "⚪"
    
    keyboard.append([InlineKeyboardButton(f"{phone_status} Phone", callback_data="add_field_phone")])
    keyboard.append([InlineKeyboardButton(f"{fb_link_status} Facebook Link", callback_data="add_field_fb_link")])
    keyboard.append([InlineKeyboardButton(f"{telegram_status} Telegram", callback_data="add_field_telegram")])
    keyboard.append([InlineKeyboardButton(f"{fb_username_status} Facebook Username", callback_data="add_field_fb_username")])
    keyboard.append([InlineKeyboardButton(f"{fb_id_status} Facebook ID", callback_data="add_field_fb_id")])
    
    # Опциональные поля - серый круг по умолчанию, зеленый круг когда заполнено
    email_status = "🟢" if user_data.get('email') else "⚪"
    country_status = "🟢" if user_data.get('country') else "⚪"
    
    keyboard.append([InlineKeyboardButton(f"{email_status} Email", callback_data="add_field_email")])
    keyboard.append([InlineKeyboardButton(f"{country_status} Country", callback_data="add_field_country")])
    
    # Кнопки действий
    keyboard.append([InlineKeyboardButton("💾 Сохранить", callback_data="add_save")])
    keyboard.append([InlineKeyboardButton("❌ Отменить", callback_data="add_cancel")])
    
    return InlineKeyboardMarkup(keyboard)

# Command handlers
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command - show main menu"""
    try:
        welcome_message = (
            "👋 Добро пожаловать в ClientsBot!\n\n"
            "Выберите действие:"
        )
        await update.message.reply_text(
            welcome_message,
            reply_markup=get_main_menu_keyboard()
        )
        logger.info(f"Start command processed for user {update.effective_user.id}")
    except Exception as e:
        logger.error(f"Error in start_command: {e}", exc_info=True)
        try:
            await update.message.reply_text(
                "❌ Произошла ошибка при обработке команды. Попробуйте позже."
            )
        except:
            pass

async def quit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /q command - return to main menu from any state"""
    try:
        # Clear any conversation state
        if context.user_data:
            context.user_data.clear()
        
        # Clear user data store if exists
        user_id = update.effective_user.id
        if user_id in user_data_store:
            del user_data_store[user_id]
        
        # Show main menu
        welcome_message = (
            "👋 Главное меню\n\n"
            "Выберите действие:"
        )
        await update.message.reply_text(
            welcome_message,
            reply_markup=get_main_menu_keyboard()
        )
        logger.info(f"Quit command processed for user {user_id}")
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in quit_command: {e}", exc_info=True)
        try:
            await update.message.reply_text(
                "❌ Произошла ошибка. Попробуйте позже.",
                reply_markup=get_main_menu_keyboard()
            )
        except:
            pass
        return ConversationHandler.END

# Callback query handlers
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button callbacks for menu navigation"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    if DEBUG_MODE:
        logger.info(f"DEBUG: button_callback received data: {data}")
    
    # Handle edit lead callback
    if data.startswith("edit_lead_"):
        try:
            lead_id = int(data.split("_")[-1])
            await edit_lead_callback(update, context, lead_id)
            return
        except (ValueError, IndexError) as e:
            logger.error(f"Error parsing edit_lead callback: {e}")
            await query.edit_message_text(
                "❌ Ошибка: Неверный формат запроса.",
                reply_markup=get_main_menu_keyboard()
            )
            return
    
    if data == "main_menu":
        await query.edit_message_text(
            "👋 Главное меню\n\nВыберите действие:",
            reply_markup=get_main_menu_keyboard()
        )
    
    elif data == "check_menu":
        if DEBUG_MODE:
            logger.info("DEBUG: Processing check_menu callback")
        await query.edit_message_text(
            "✅ Проверить клиента\n\nВыберите способ проверки:",
            reply_markup=get_check_menu_keyboard()
        )
    
    elif data == "add_menu":
        await query.edit_message_text(
            "➕ Добавить клиента\n\nВыберите способ добавления:",
            reply_markup=get_add_menu_keyboard()
        )

# Check callbacks
async def check_telegram_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for check by telegram conversation"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("📱 Введите Telegram username для проверки:")
    return CHECK_BY_TELEGRAM

async def check_fb_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for check by facebook link conversation"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("🔗 Введите Facebook Link для проверки:")
    return CHECK_BY_FB_LINK

async def check_fb_username_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for check by facebook username conversation"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("👤 Введите Facebook Username для проверки:")
    return CHECK_BY_FB_USERNAME

async def check_fb_id_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for check by facebook id conversation"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("🆔 Введите Facebook ID для проверки:")
    return CHECK_BY_FB_ID

async def check_phone_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for check by phone conversation"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("🔢 Введите номер телефона для проверки:")
    return CHECK_BY_PHONE

async def check_fullname_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for check by fullname conversation"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("👤 Введите полное имя (или фамилию):")
    return CHECK_BY_FULLNAME

# Add callback - new sequential flow
async def add_new_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start adding new lead - sequential flow"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user_data_store[user_id] = {}
    user_data_store_access_time[user_id] = time.time()
    context.user_data['current_field'] = 'fullname'
    context.user_data['add_step'] = 0
    
    # Start with first field: Full Name
    field_label = get_field_label('fullname')
    requirements = get_field_format_requirements('fullname')
    
    message = f"📝 Введите {field_label}:\n\n{requirements}"
    
    await query.edit_message_text(
        message,
        reply_markup=get_navigation_keyboard(is_optional=False, show_back=False)
    )
    return ADD_FULLNAME

# Universal check function
async def check_by_field(update: Update, context: ContextTypes.DEFAULT_TYPE, field_name: str, field_label: str, current_state: int):
    """Universal function to check by any field"""
    search_value = update.message.text.strip()
    
    if not search_value:
        await update.message.reply_text(f"❌ {field_label} не может быть пустым. Попробуйте снова:")
        return current_state
    
    # Validate minimum length for search
    if field_name == "phone":
        normalized = normalize_phone(search_value)
        if len(normalized) < 7:
            await update.message.reply_text("❌ Для поиска по телефону необходимо минимум 7 цифр. Попробуйте снова:")
            return current_state
        search_value = normalized
        if DEBUG_MODE:
            logger.info(f"DEBUG: Checking phone, normalized: {search_value}")
    
    # Normalize Facebook link if checking by facebook_link
    elif field_name == "facebook_link":
        # Use validate_facebook_link to normalize the link (same logic as when adding)
        is_valid, error_msg, normalized = validate_facebook_link(search_value)
        if not is_valid:
            await update.message.reply_text(f"❌ {error_msg}\n\nПопробуйте снова:")
            return current_state
        search_value = normalized
        if DEBUG_MODE:
            logger.info(f"DEBUG: Checking facebook_link, normalized: {search_value}")
    
    # Get Supabase client (for all fields, not just phone)
    client = get_supabase_client()
    if not client:
        await update.message.reply_text(
            "❌ Ошибка: Не удалось подключиться к базе данных.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END
    
    try:
        # For phone: search by last 7-9 digits
        if field_name == "phone":
            # Extract last 7-9 digits
            if len(search_value) >= 9:
                last_digits = search_value[-9:]
            elif len(search_value) >= 7:
                last_digits = search_value[-7:]
            else:
                last_digits = search_value
            
            if DEBUG_MODE:
                logger.info(f"DEBUG: Searching phone by last digits: {last_digits}")
            # Search by suffix using ilike (case-insensitive pattern matching)
            # Limit results to 50 for performance
            response = client.table(TABLE_NAME).select("*").ilike(field_name, f"%{last_digits}").limit(50).execute()
        else:
            # For other fields: exact match, limit to 50 results
            response = client.table(TABLE_NAME).select("*").eq(field_name, search_value).limit(50).execute()
        
        # Field labels mapping (Russian)
        field_labels = {
            'fullname': 'Имя',
            'phone': 'Телефон',
            'email': 'Email',
            'country': 'Страна',
            'facebook_id': 'Facebook ID',
            'facebook_username': 'Facebook Username',
            'facebook_link': 'Facebook Link',
            'telegram_name': 'Telegram Name',
            'telegram_id': 'Telegram ID',
            'manager_name': 'Добавил',
            'created_at': 'Дата'
        }
        
        if response.data and len(response.data) > 0:
            results = response.data
            if DEBUG_MODE:
                logger.info(f"DEBUG: Found {len(results)} result(s)")
            
            # If multiple results, show all
            if len(results) > 1:
                message_parts = [f"✅ Найдено клиентов: {len(results)}\n"]
                
                for idx, result in enumerate(results, 1):
                    if idx > 1:
                        message_parts.append("")  # Empty line between leads
                    message_parts.append(f"--- Клиент {idx} ---")
                    for field_name_key, field_label in field_labels.items():
                        value = result.get(field_name_key)
                        
                        # Skip if None, empty string, or 'Не указано'
                        if value is None or value == '' or value == 'Не указано':
                            continue
                        
                        # Format date field
                        if field_name_key == 'created_at':
                            try:
                                dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                                value = dt.strftime('%d.%m.%Y %H:%M')
                            except:
                                pass
                        
                        # Format value in code tags for easy copying
                        escaped_value = escape_html(str(value))
                        message_parts.append(f"{field_label}: <code>{escaped_value}</code>")
            else:
                # Single result
                result = results[0]
                message_parts = ["✅ Лид найден.", ""]  # Empty line after header
                
                for field_name_key, field_label in field_labels.items():
                    value = result.get(field_name_key)
                    
                    # Skip if None, empty string, or 'Не указано'
                    if value is None or value == '' or value == 'Не указано':
                        continue
            
                    # Format date field
                    if field_name_key == 'created_at':
                        try:
                            dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                            value = dt.strftime('%d.%m.%Y %H:%M')
                        except:
                            pass
                    
                    # Format value in code tags for easy copying
                    escaped_value = escape_html(str(value))
                    message_parts.append(f"{field_label}: <code>{escaped_value}</code>")
            
            message = "\n".join(message_parts)
        else:
            message = "❌ Клиент не найден."
        
        await update.message.reply_text(
            message,
            reply_markup=get_main_menu_keyboard(),
            parse_mode='HTML'
        )
        
    except Exception as e:
        logger.error(f"Error checking by {field_name}: {e}", exc_info=True)
        await update.message.reply_text(
            "❌ Произошла ошибка при проверке. Попробуйте позже.",
            reply_markup=get_main_menu_keyboard()
        )
    
    return ConversationHandler.END

async def check_by_fullname(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check by fullname using contains search with limit of 10 results"""
    search_value = update.message.text.strip()
    
    if not search_value:
        await update.message.reply_text("❌ Имя не может быть пустым. Попробуйте снова:")
        return CHECK_BY_FULLNAME
    
    # Validate minimum length for fullname search
    if len(search_value) < 3:
        await update.message.reply_text("❌ Для поиска по имени необходимо минимум 3 символа. Попробуйте снова:")
        return CHECK_BY_FULLNAME
    
    # Get Supabase client
    client = get_supabase_client()
    if not client:
        await update.message.reply_text(
            "❌ Ошибка: Не удалось подключиться к базе данных.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END
    
    try:
        # Search using ilike with contains pattern (case-insensitive)
        # Limit to 10 results at DB level for better performance
        # Sort by created_at descending (newest first)
        response = client.table(TABLE_NAME).select("*").ilike("fullname", f"%{search_value}%").order("created_at", desc=True).limit(10).execute()
        
        # Field labels mapping (Russian)
        field_labels = {
            'fullname': 'Имя',
            'phone': 'Телефон',
            'email': 'Email',
            'country': 'Страна',
            'facebook_id': 'Facebook ID',
            'facebook_username': 'Facebook Username',
            'facebook_link': 'Facebook Link',
            'telegram_name': 'Telegram Name',
            'telegram_id': 'Telegram ID',
            'manager_name': 'Добавил',
            'created_at': 'Дата'
        }
        
        if response.data and len(response.data) > 0:
            results = response.data
            if DEBUG_MODE:
                logger.info(f"DEBUG: Found {len(results)} result(s) for fullname search: {search_value}")
            
            # Check if more than 10 results
            if len(results) > 10:
                await update.message.reply_text(
                    "❌ Слишком много совпадений. Попробуйте другой фильтр поиска.",
                    reply_markup=get_main_menu_keyboard()
                )
                return ConversationHandler.END
            
            # If multiple results, show all
            if len(results) > 1:
                message_parts = [f"✅ Найдено клиентов: {len(results)}\n"]
                
                for idx, result in enumerate(results, 1):
                    if idx > 1:
                        message_parts.append("")  # Empty line between leads
                    message_parts.append(f"--- Клиент {idx} ---")
                    for field_name_key, field_label in field_labels.items():
                        value = result.get(field_name_key)
                        
                        # Skip if None, empty string, or 'Не указано'
                        if value is None or value == '' or value == 'Не указано':
                            continue
                        
                        # Format date field
                        if field_name_key == 'created_at':
                            try:
                                dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                                value = dt.strftime('%d.%m.%Y %H:%M')
                            except:
                                pass
                        
                        # Format value in code tags for easy copying
                        escaped_value = escape_html(str(value))
                        message_parts.append(f"{field_label}: <code>{escaped_value}</code>")
            else:
                # Single result
                result = results[0]
                message_parts = ["✅ Лид найден.", ""]  # Empty line after header
                
                for field_name_key, field_label in field_labels.items():
                    value = result.get(field_name_key)
                    
                    # Skip if None, empty string, or 'Не указано'
                    if value is None or value == '' or value == 'Не указано':
                        continue
            
                    # Format date field
                    if field_name_key == 'created_at':
                        try:
                            dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                            value = dt.strftime('%d.%m.%Y %H:%M')
                        except:
                            pass
                    
                    # Format value in code tags for easy copying
                    escaped_value = escape_html(str(value))
                    message_parts.append(f"{field_label}: <code>{escaped_value}</code>")
            
            message = "\n".join(message_parts)
        else:
            message = "❌ Клиент не найден."
        
        await update.message.reply_text(
            message,
            reply_markup=get_main_menu_keyboard(),
            parse_mode='HTML'
        )
        
    except Exception as e:
        logger.error(f"Error checking by fullname: {e}", exc_info=True)
        error_msg = "❌ Произошла ошибка при проверке."
        if DEBUG_MODE:
            error_msg += f"\n\nДетали: {str(e)}"
        else:
            error_msg += " Попробуйте позже или обратитесь к администратору."
        await update.message.reply_text(
            error_msg,
            reply_markup=get_main_menu_keyboard()
        )
    
    return ConversationHandler.END

# Check input handlers
async def check_telegram_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await check_by_field(update, context, "telegram_name", "Telegram Name", CHECK_BY_TELEGRAM)

async def check_fb_link_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await check_by_field(update, context, "facebook_link", "Facebook Link", CHECK_BY_FB_LINK)

async def check_fb_username_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await check_by_field(update, context, "facebook_username", "Facebook Username", CHECK_BY_FB_USERNAME)

async def check_fb_id_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await check_by_field(update, context, "facebook_id", "Facebook ID", CHECK_BY_FB_ID)

async def check_phone_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await check_by_field(update, context, "phone", "Номер телефона", CHECK_BY_PHONE)

async def check_fullname_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await check_by_fullname(update, context)

# Old add_field_callback removed - using sequential flow now

def cleanup_user_data_store():
    """Clean up old entries from user_data_store to optimize memory"""
    global user_data_store, user_data_store_access_time
    
    current_time = time.time()
    users_to_remove = []
    
    # Remove entries older than TTL
    for user_id, access_time in user_data_store_access_time.items():
        if current_time - access_time > USER_DATA_STORE_TTL:
            users_to_remove.append(user_id)
    
    # Remove old entries
    for user_id in users_to_remove:
        if user_id in user_data_store:
            del user_data_store[user_id]
        if user_id in user_data_store_access_time:
            del user_data_store_access_time[user_id]
    
    # If still too large, remove oldest entries
    if len(user_data_store) > USER_DATA_STORE_MAX_SIZE:
        sorted_users = sorted(user_data_store_access_time.items(), key=lambda x: x[1])
        users_to_remove = [user_id for user_id, _ in sorted_users[:len(user_data_store) - USER_DATA_STORE_MAX_SIZE]]
        for user_id in users_to_remove:
            if user_id in user_data_store:
                del user_data_store[user_id]
            if user_id in user_data_store_access_time:
                del user_data_store_access_time[user_id]
    
    if users_to_remove and DEBUG_MODE:
        logger.info(f"Cleaned up {len(users_to_remove)} old user_data_store entries")

async def check_duplicate_realtime(client, field_name: str, field_value: str) -> tuple[bool, str]:
    """Check if a field value already exists in the database (for real-time validation)"""
    if not field_value or field_value.strip() == '':
        return True, ""  # Empty values are considered unique
    
    try:
        response = client.table(TABLE_NAME).select("id, fullname").eq(field_name, field_value).limit(1).execute()
        if response.data and len(response.data) > 0:
            existing_lead = response.data[0]
            fullname = existing_lead.get('fullname', 'Неизвестно')
            return False, fullname
        return True, ""
    except Exception as e:
        logger.error(f"Error checking real-time duplicate for {field_name}: {e}", exc_info=True)
        return True, ""  # On error, allow to continue (will be checked again on save)

async def add_field_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Universal handler for field input - sequential flow"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    field_name = context.user_data.get('current_field')
    current_state = context.user_data.get('current_state', ADD_FULLNAME)
    
    # Update access time
    user_data_store_access_time[user_id] = time.time()
    cleanup_user_data_store()
    
    if not field_name:
        # Fallback: start from beginning
        await update.message.reply_text(
            "❌ Ошибка: поле не определено. Начинаем заново.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END
    
    # Validate and normalize based on field type
    validation_passed = False
    normalized_value = text
    
    if field_name == 'phone':
        is_valid, error_msg, normalized = validate_phone(text)
        if is_valid:
            validation_passed = True
            normalized_value = normalized
        else:
            field_label = get_field_label('phone')
            requirements = get_field_format_requirements('phone')
            await update.message.reply_text(
                f"❌ {error_msg}\n\n📝 Введите {field_label}:\n\n{requirements}",
                reply_markup=get_navigation_keyboard(is_optional=True, show_back=True)
            )
            return current_state
    
    elif field_name == 'email':
        is_valid, error_msg = validate_email(text)
        if is_valid:
            validation_passed = True
        else:
            field_label = get_field_label('email')
            requirements = get_field_format_requirements('email')
            await update.message.reply_text(
                f"❌ {error_msg}\n\n📝 Введите {field_label}:\n\n{requirements}",
                reply_markup=get_navigation_keyboard(is_optional=True, show_back=True)
            )
            return current_state
    
    elif field_name == 'facebook_link':
        is_valid, error_msg, extracted = validate_facebook_link(text)
        if is_valid:
            validation_passed = True
            normalized_value = extracted
        else:
            field_label = get_field_label('facebook_link')
            requirements = get_field_format_requirements('facebook_link')
            await update.message.reply_text(
                f"❌ {error_msg}\n\n📝 Введите {field_label}:\n\n{requirements}",
                reply_markup=get_navigation_keyboard(is_optional=True, show_back=True)
            )
            return current_state
    
    elif field_name == 'telegram_name':
        is_valid, error_msg, normalized = validate_telegram_name(text)
        if is_valid:
            validation_passed = True
            normalized_value = normalized
        else:
            field_label = get_field_label('telegram_name')
            requirements = get_field_format_requirements('telegram_name')
            await update.message.reply_text(
                f"❌ {error_msg}\n\n📝 Введите {field_label}:\n\n{requirements}",
                reply_markup=get_navigation_keyboard(is_optional=True, show_back=True)
            )
            return current_state
    
    elif field_name == 'telegram_id':
        is_valid, error_msg = validate_telegram_id(text)
        if is_valid:
            validation_passed = True
            normalized_value = text.strip()
        else:
            field_label = get_field_label('telegram_id')
            requirements = get_field_format_requirements('telegram_id')
            await update.message.reply_text(
                f"❌ {error_msg}\n\n📝 Введите {field_label}:\n\n{requirements}",
                reply_markup=get_navigation_keyboard(is_optional=True, show_back=True)
            )
            return current_state
    
    else:
        # For other fields (fullname, country, manager_name), just check not empty
        if text:
            validation_passed = True
        else:
            field_label = get_field_label(field_name)
            requirements = get_field_format_requirements(field_name)
            is_optional = field_name not in ['fullname', 'manager_name']
            await update.message.reply_text(
                f"❌ Поле не может быть пустым.\n\n📝 Введите {field_label}:\n\n{requirements}",
                reply_markup=get_navigation_keyboard(is_optional=is_optional, show_back=(field_name != 'fullname'))
            )
            return current_state
    
    # Real-time duplicate check for critical fields
    if validation_passed and normalized_value:
        # Check duplicates for phone, email
        if field_name in ['phone', 'email']:
            client = get_supabase_client()
            if client:
                is_unique, existing_fullname = await check_duplicate_realtime(client, field_name, normalized_value)
                if not is_unique:
                    field_label = UNIQUENESS_FIELD_LABELS.get(field_name, field_name)
                    await update.message.reply_text(
                        f"⚠️ Внимание: {field_label} уже существует в базе.\n"
                        f"Существующий лид: {existing_fullname}\n\n"
                        f"Вы можете продолжить, но при сохранении будет ошибка."
                    )
                    # Still allow to continue, will be checked again on save
    
    # Save value only if validation passed
    if validation_passed and normalized_value:
        user_data_store[user_id][field_name] = normalized_value
    
    # Move to next field
    next_field, next_state = get_next_add_field(field_name)
    
    if next_field == 'review':
        # Show review and save option
        await show_add_review(update, context)
        return ADD_REVIEW
    else:
        # Show next field
        field_label = get_field_label(next_field)
        requirements = get_field_format_requirements(next_field)
        is_optional = next_field not in ['fullname', 'manager_name']
        
        message = f"📝 Введите {field_label}:\n\n{requirements}"
        
        context.user_data['current_field'] = next_field
        context.user_data['current_state'] = next_state
        
        # Работаем как с message, так и с callback_query
        if update.callback_query:
            await update.callback_query.edit_message_text(
                message,
                reply_markup=get_navigation_keyboard(is_optional=is_optional, show_back=True)
            )
        elif update.message:
            await update.message.reply_text(
                message,
                reply_markup=get_navigation_keyboard(is_optional=is_optional, show_back=True)
            )
        return next_state

async def show_add_review(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show review of entered data before saving"""
    user_id = update.effective_user.id
    user_data = user_data_store.get(user_id, {})
    
    message_parts = ["✅ Проверьте введенные данные:\n"]
    
    field_labels = {
        'fullname': 'Имя Фамилия',
        'manager_name': 'Агент',
        'phone': 'Телефон',
        'facebook_link': 'Facebook Link',
        'telegram_name': 'Telegram Name',
        'telegram_id': 'Telegram ID',
        'email': 'Email',
        'country': 'Country'
    }
    
    for field_name, field_label in field_labels.items():
        value = user_data.get(field_name)
        if value:
            escaped_value = escape_html(str(value))
            message_parts.append(f"{field_label}: <code>{escaped_value}</code>")
    
    message = "\n".join(message_parts)
    
    keyboard = [
        [InlineKeyboardButton("💾 Сохранить", callback_data="add_save")],
        [InlineKeyboardButton("◀️ Назад", callback_data="add_back")],
        [InlineKeyboardButton("❌ Отменить", callback_data="add_cancel")]
    ]
    
    # Работаем как с message, так и с callback_query
    if update.callback_query:
        await update.callback_query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    elif update.message:
        await update.message.reply_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )

# Specific field callbacks
async def add_field_fullname_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'fullname', 'Full Name', ADD_FULLNAME)

async def add_field_phone_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'phone', 'Phone', ADD_PHONE)

async def add_field_email_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'email', 'Email', ADD_EMAIL)

async def add_field_country_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'country', 'Country', ADD_COUNTRY)

# Facebook ID and Username callbacks removed - using only Facebook Link now

async def add_field_fb_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'facebook_link', 'Facebook Link', ADD_FB_LINK)

# Old telegram callback removed - using sequential flow now

async def add_field_manager_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'manager_name', 'Manager Name', ADD_MANAGER_NAME)

# Field labels for uniqueness check messages (Russian)
UNIQUENESS_FIELD_LABELS = {
    'phone': 'Номер телефона',
    'email': 'Email',
    'fullname': 'Имя',
    'facebook_link': 'Facebook Link',
    'telegram_name': 'Telegram Name',
    'telegram_id': 'Telegram ID'
}

def check_fields_uniqueness_batch(client, fields_to_check: dict) -> tuple[bool, str]:
    """
    Check uniqueness of multiple fields in a single query using OR conditions.
    Returns (is_unique, conflicting_field) where conflicting_field is empty if all unique.
    """
    if not fields_to_check:
        return True, ""
    
    # Check cache first
    cache_key = tuple(sorted(fields_to_check.items()))
    if cache_key in uniqueness_cache:
        cached_result, cached_time = uniqueness_cache[cache_key]
        if time.time() - cached_time < CACHE_TTL:
            return cached_result
    
    # Check each field but use caching and limit queries
    # Supabase Python client doesn't support OR conditions directly,
    # so we check each field but optimize with caching
    try:
        for field_name, field_value in fields_to_check.items():
            if field_value and field_value.strip():
                # Check individual field with retry
                response = client.table(TABLE_NAME).select("id").eq(field_name, field_value).limit(1).execute()
                if response.data and len(response.data) > 0:
                    # Found duplicate
                    result = (False, field_name)
                    uniqueness_cache[cache_key] = (result, time.time())
                    return result
        
        # All fields are unique
        result = (True, "")
        uniqueness_cache[cache_key] = (result, time.time())
        return result
        
    except Exception as e:
        logger.error(f"Error checking batch uniqueness: {e}", exc_info=True)
        # On error, assume not unique to prevent duplicate inserts
        return False, "unknown"

def check_field_uniqueness(client, field_name: str, field_value: str) -> bool:
    """Check if a field value already exists in the database (with retry and cache)"""
    if not field_value or field_value.strip() == '':
        return True  # Empty values are considered unique
    
    # Check cache
    cache_key = (field_name, field_value)
    if cache_key in uniqueness_cache:
        cached_result, cached_time = uniqueness_cache[cache_key]
        if time.time() - cached_time < CACHE_TTL:
            return cached_result
    
    # Use retry wrapper for the actual query
    @retry_supabase_query(max_retries=3, delay=1, backoff=2)
    def _execute_query():
        return client.table(TABLE_NAME).select("id").eq(field_name, field_value).limit(1).execute()
    
    try:
        response = _execute_query()
        # If any records found, field is not unique
        is_unique = not (response.data and len(response.data) > 0)
        
        # Cache result
        uniqueness_cache[cache_key] = (is_unique, time.time())
        return is_unique
    except Exception as e:
        logger.error(f"Error checking uniqueness for {field_name}: {e}", exc_info=True)
        # On error, assume not unique to prevent duplicate inserts
        return False

async def add_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Skip current optional field"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    field_name = context.user_data.get('current_field')
    current_state = context.user_data.get('current_state', ADD_FULLNAME)
    
    # Move to next field
    next_field, next_state = get_next_add_field(field_name)
    
    if next_field == 'review':
        # Show review and save option
        await show_add_review(update, context)
        return ADD_REVIEW
    else:
        # Show next field
        field_label = get_field_label(next_field)
        requirements = get_field_format_requirements(next_field)
        is_optional = next_field not in ['fullname', 'manager_name']
        
        message = f"📝 Введите {field_label}:\n\n{requirements}"
        
        context.user_data['current_field'] = next_field
        context.user_data['current_state'] = next_state
        
        await query.edit_message_text(
            message,
            reply_markup=get_navigation_keyboard(is_optional=is_optional, show_back=True)
        )
        return next_state

async def add_back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Go back to previous field"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    field_name = context.user_data.get('current_field')
    
    # Get previous field
    field_sequence = [
        ('fullname', ADD_FULLNAME),
        ('manager_name', ADD_MANAGER_NAME),
        ('phone', ADD_PHONE),
        ('facebook_link', ADD_FB_LINK),
        ('telegram_name', ADD_TELEGRAM_NAME),
        ('telegram_id', ADD_TELEGRAM_ID),
        ('email', ADD_EMAIL),
        ('country', ADD_COUNTRY),
    ]
    
    prev_field = None
    prev_state = ADD_FULLNAME
    
    for i, (field, state) in enumerate(field_sequence):
        if field == field_name:
            if i > 0:
                prev_field, prev_state = field_sequence[i - 1]
            break
    
    if prev_field:
        field_label = get_field_label(prev_field)
        requirements = get_field_format_requirements(prev_field)
        is_optional = prev_field not in ['fullname', 'manager_name']
        
        message = f"📝 Введите {field_label}:\n\n{requirements}"
        
        context.user_data['current_field'] = prev_field
        context.user_data['current_state'] = prev_state
        
        await query.edit_message_text(
            message,
            reply_markup=get_navigation_keyboard(is_optional=is_optional, show_back=(prev_field != 'fullname'))
        )
        return prev_state
    else:
        # Already at first field, go to main menu
        await query.edit_message_text(
            "👋 Главное меню\n\nВыберите действие:",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END

async def add_save_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Validate and save the lead"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user_data = user_data_store.get(user_id, {})
    
    # Validation
    if not user_data.get('fullname'):
        field_label = get_field_label('fullname')
        requirements = get_field_format_requirements('fullname')
        await query.edit_message_text(
            f"❌ Ошибка: {field_label} обязателен для заполнения!\n\n"
            f"📝 Введите {field_label}:\n\n{requirements}",
            reply_markup=get_navigation_keyboard(is_optional=False, show_back=False)
        )
        context.user_data['current_field'] = 'fullname'
        context.user_data['current_state'] = ADD_FULLNAME
        return ADD_FULLNAME
    
    if not user_data.get('manager_name'):
        field_label = get_field_label('manager_name')
        requirements = get_field_format_requirements('manager_name')
        await query.edit_message_text(
            f"❌ Ошибка: {field_label} обязателен для заполнения!\n\n"
            f"📝 Введите {field_label}:\n\n{requirements}",
            reply_markup=get_navigation_keyboard(is_optional=False, show_back=True)
        )
        context.user_data['current_field'] = 'manager_name'
        context.user_data['current_state'] = ADD_MANAGER_NAME
        return ADD_MANAGER_NAME
    
    # Check if at least one identifier is present
    required_fields = ['phone', 'facebook_link', 'telegram_name', 'telegram_id']
    has_identifier = any(user_data.get(field) for field in required_fields)
    
    if not has_identifier:
        await query.edit_message_text(
            "❌ Ошибка: Необходимо указать минимум одно из полей:\n"
            "Phone, Facebook Link, Telegram Name или Telegram ID!\n\n"
            "Начнем с первого опционального поля:",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END
    
    # Get Supabase client for uniqueness check
    client = get_supabase_client()
    if not client:
        await query.edit_message_text(
            "❌ Ошибка: Не удалось подключиться к базе данных.",
            reply_markup=get_main_menu_keyboard()
        )
        if user_id in user_data_store:
            del user_data_store[user_id]
        return ConversationHandler.END
    
    # Check uniqueness of fields - optimized batch check
    fields_to_check = {}
    for field_name in ['phone', 'email', 'fullname', 'facebook_link', 'telegram_name', 'telegram_id']:
        field_value = user_data.get(field_name)
        if field_value and field_value.strip():  # Only check non-empty fields
            # Normalize phone if checking phone field
            check_value = normalize_phone(field_value) if field_name == 'phone' else field_value
            fields_to_check[field_name] = check_value
    
    # Batch check uniqueness
    if fields_to_check:
        is_unique, conflicting_field = check_fields_uniqueness_batch(client, fields_to_check)
        if not is_unique:
            field_label = UNIQUENESS_FIELD_LABELS.get(conflicting_field, conflicting_field)
            await query.edit_message_text(
                f"❌ {field_label} уже существует в базе.\n\n"
                "Попробуйте добавить лид заново.",
                reply_markup=get_main_menu_keyboard()
            )
            if user_id in user_data_store:
                del user_data_store[user_id]
            if user_id in user_data_store_access_time:
                del user_data_store_access_time[user_id]
            return ConversationHandler.END
    
    # All fields are unique, proceed with saving
    try:
        # Prepare data for saving - map telegram_name to telegram_user for database compatibility
        save_data = user_data.copy()
        
        # Normalize phone in save_data before saving if present
        if 'phone' in save_data and save_data['phone']:
            save_data['phone'] = normalize_phone(save_data['phone'])
        
        # Map telegram_name to telegram_user for database (backward compatibility)
        if 'telegram_name' in save_data:
            save_data['telegram_user'] = save_data.pop('telegram_name')
        
        response = client.table(TABLE_NAME).insert(save_data).execute()
        
        if response.data:
            # Show success message with entered data
            message_parts = ["✅ Клиент успешно добавлен!\n"]
            field_labels = {
                'fullname': 'Имя Фамилия',
                'manager_name': 'Агент',
                'phone': 'Телефон',
                'facebook_link': 'Facebook Link',
                'telegram_name': 'Telegram Name',
                'telegram_id': 'Telegram ID',
                'email': 'Email',
                'country': 'Country'
            }
            
            for field_name, field_label in field_labels.items():
                # Check telegram_user for display (it was mapped from telegram_name)
                display_field = 'telegram_user' if field_name == 'telegram_name' else field_name
                value = save_data.get(display_field) or user_data.get(field_name)
                if value:
                    escaped_value = escape_html(str(value))
                    message_parts.append(f"{field_label}: <code>{escaped_value}</code>")
            
            # Add date
            from datetime import datetime
            message_parts.append(f"Дата добавления: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
            
            message = "\n".join(message_parts)
            
            keyboard = [
                [InlineKeyboardButton("➕ Добавить клиента", callback_data="add_new")],
                [InlineKeyboardButton("🔍 Искать клиента", callback_data="check_menu")]
            ]
            
            await query.edit_message_text(
                message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
            logger.info(f"Added new client: {save_data}")
        else:
            await query.edit_message_text(
                "❌ Ошибка: Данные не были сохранены. Попробуйте снова.",
                reply_markup=get_main_menu_keyboard()
            )
    
    except Exception as e:
        logger.error(f"Error adding client: {e}", exc_info=True)
        error_msg = "❌ Произошла ошибка при сохранении данных."
        if DEBUG_MODE:
            error_msg += f"\n\nДетали: {str(e)}"
        else:
            error_msg += " Попробуйте позже или обратитесь к администратору."
        await query.edit_message_text(
            error_msg,
            reply_markup=get_main_menu_keyboard()
        )
    
    # Clean up
    if user_id in user_data_store:
        del user_data_store[user_id]
    if user_id in user_data_store_access_time:
        del user_data_store_access_time[user_id]
    
    return ConversationHandler.END

async def add_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel adding new lead"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id in user_data_store:
        del user_data_store[user_id]
    if user_id in user_data_store_access_time:
        del user_data_store_access_time[user_id]
    
    await query.edit_message_text(
        "❌ Добавление отменено.",
        reply_markup=get_main_menu_keyboard()
    )
    return ConversationHandler.END

# Edit lead functionality
async def edit_lead_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, lead_id: int):
    """Start editing a lead"""
    query = update.callback_query
    await query.answer()
    
    # Get lead from database
    client = get_supabase_client()
    if not client:
        await query.edit_message_text(
            "❌ Ошибка: Не удалось подключиться к базе данных.",
            reply_markup=get_main_menu_keyboard()
        )
        return
    
    try:
        response = client.table(TABLE_NAME).select("*").eq("id", lead_id).execute()
        if not response.data or len(response.data) == 0:
            await query.edit_message_text(
                "❌ Ошибка: Лид не найден.",
                reply_markup=get_main_menu_keyboard()
            )
            return
        
        lead = response.data[0]
        user_id = query.from_user.id
        
        # Store lead data for editing
        # Map telegram_user to telegram_name for consistency
        lead_data = lead.copy()
        if 'telegram_user' in lead_data and 'telegram_name' not in lead_data:
            lead_data['telegram_name'] = lead_data.get('telegram_user')
        
        user_data_store[user_id] = lead_data
        user_data_store_access_time[user_id] = time.time()
        context.user_data['editing_lead_id'] = lead_id
        
        # Show edit menu (similar to add menu)
        message = f"✏️ Редактирование лида (ID: {lead_id})\n\nВыберите поле для редактирования:"
        await query.edit_message_text(
            message,
            reply_markup=get_edit_field_keyboard(user_id)
        )
        return EDIT_MENU
        
    except Exception as e:
        logger.error(f"Error loading lead for editing: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ Произошла ошибка при загрузке лида. Попробуйте позже.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END

def get_edit_field_keyboard(user_id: int):
    """Create keyboard for editing lead fields"""
    user_data = user_data_store.get(user_id, {})
    keyboard = []
    
    # Mandatory fields
    fullname_status = "🟢" if user_data.get('fullname') else "⚪"
    manager_status = "🟢" if user_data.get('manager_name') else "⚪"
    
    keyboard.append([InlineKeyboardButton(f"{fullname_status} Имя Фамилия *", callback_data="edit_field_fullname")])
    keyboard.append([InlineKeyboardButton(f"{manager_status} Агент *", callback_data="edit_field_manager")])
    
    # Identifier fields
    phone_status = "🟢" if user_data.get('phone') else "⚪"
    fb_link_status = "🟢" if user_data.get('facebook_link') else "⚪"
    # Check both telegram_user (old) and telegram_name (new)
    telegram_name_value = user_data.get('telegram_name') or user_data.get('telegram_user')
    telegram_name_status = "🟢" if telegram_name_value else "⚪"
    telegram_id_status = "🟢" if user_data.get('telegram_id') else "⚪"
    
    keyboard.append([InlineKeyboardButton(f"{phone_status} Phone", callback_data="edit_field_phone")])
    keyboard.append([InlineKeyboardButton(f"{fb_link_status} Facebook Link", callback_data="edit_field_fb_link")])
    keyboard.append([InlineKeyboardButton(f"{telegram_name_status} Telegram Name", callback_data="edit_field_telegram_name")])
    keyboard.append([InlineKeyboardButton(f"{telegram_id_status} Telegram ID", callback_data="edit_field_telegram_id")])
    
    # Optional fields
    email_status = "🟢" if user_data.get('email') else "⚪"
    country_status = "🟢" if user_data.get('country') else "⚪"
    
    keyboard.append([InlineKeyboardButton(f"{email_status} Email", callback_data="edit_field_email")])
    keyboard.append([InlineKeyboardButton(f"{country_status} Country", callback_data="edit_field_country")])
    
    # Action buttons
    keyboard.append([InlineKeyboardButton("💾 Сохранить изменения", callback_data="edit_save")])
    keyboard.append([InlineKeyboardButton("❌ Отменить", callback_data="edit_cancel")])
    
    return InlineKeyboardMarkup(keyboard)

# Edit field callbacks (must be defined before create_telegram_app)
async def edit_field_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, field_name: str, field_label: str, next_state: int):
    """Universal callback for editing field selection"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    # If field is already filled, ask if user wants to change it
    if user_data_store.get(user_id, {}).get(field_name):
        await query.edit_message_text(
            f"📝 {field_label} текущее значение: {user_data_store[user_id][field_name]}\n"
            f"Введите новое значение или отправьте /skip чтобы оставить текущее:"
        )
    else:
        await query.edit_message_text(f"📝 Введите {field_label}:")
    
    context.user_data['current_field'] = field_name
    context.user_data['current_state'] = next_state
    return next_state

async def edit_field_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Universal handler for edit field input"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    field_name = context.user_data.get('current_field')
    
    # Update access time
    user_data_store_access_time[user_id] = time.time()
    cleanup_user_data_store()
    
    if not field_name:
        await update.message.reply_text(
            "❌ Ошибка: поле не определено.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END
    
    # Validate and normalize based on field type (same logic as add_field_input)
    validation_passed = False
    normalized_value = text
    
    if field_name == 'phone':
        is_valid, error_msg, normalized = validate_phone(text)
        if is_valid:
            validation_passed = True
            normalized_value = normalized
        else:
            await update.message.reply_text(f"❌ {error_msg}\n\nПопробуйте снова:")
            return context.user_data.get('current_state', EDIT_MENU)
    
    elif field_name == 'email':
        is_valid, error_msg = validate_email(text)
        if is_valid:
            validation_passed = True
        else:
            await update.message.reply_text(f"❌ {error_msg}\n\nПопробуйте снова:")
            return context.user_data.get('current_state', EDIT_MENU)
    
    elif field_name == 'facebook_link':
        is_valid, error_msg, extracted = validate_facebook_link(text)
        if is_valid:
            validation_passed = True
            normalized_value = extracted
        else:
            await update.message.reply_text(f"❌ {error_msg}\n\nПопробуйте снова:")
            return context.user_data.get('current_state', EDIT_MENU)
    
    elif field_name == 'telegram_name':
        is_valid, error_msg, normalized = validate_telegram_name(text)
        if is_valid:
            validation_passed = True
            normalized_value = normalized
        else:
            await update.message.reply_text(f"❌ {error_msg}\n\nПопробуйте снова:")
            return context.user_data.get('current_state', EDIT_MENU)
    
    elif field_name == 'telegram_id':
        is_valid, error_msg = validate_telegram_id(text)
        if is_valid:
            validation_passed = True
            normalized_value = text.strip()
        else:
            await update.message.reply_text(f"❌ {error_msg}\n\nПопробуйте снова:")
            return context.user_data.get('current_state', EDIT_MENU)
    
    else:
        # For other fields (fullname, country, manager_name), just check not empty
        if text:
            validation_passed = True
        else:
            await update.message.reply_text(f"❌ Поле не может быть пустым.\n\nПопробуйте снова:")
            return context.user_data.get('current_state', EDIT_MENU)
    
    # Save value only if validation passed
    if validation_passed and normalized_value:
        user_data_store[user_id][field_name] = normalized_value
    
    # Show edit menu again
    await update.message.reply_text(
        "✏️ Выберите поле для редактирования:",
        reply_markup=get_edit_field_keyboard(user_id)
    )
    return EDIT_MENU

async def edit_save_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save edited lead"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user_data = user_data_store.get(user_id, {})
    lead_id = context.user_data.get('editing_lead_id')
    
    if not lead_id:
        await query.edit_message_text(
            "❌ Ошибка: ID лида не найден.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END
    
    # Validation (same as add_save_callback)
    if not user_data.get('fullname'):
        await query.edit_message_text(
            "❌ Ошибка: Full Name обязателен для заполнения!\n\n"
            "Выберите поле для редактирования:",
            reply_markup=get_edit_field_keyboard(user_id)
        )
        return EDIT_MENU
    
    if not user_data.get('manager_name'):
        await query.edit_message_text(
            "❌ Ошибка: Manager Name обязателен для заполнения!\n\n"
            "Выберите поле для редактирования:",
            reply_markup=get_edit_field_keyboard(user_id)
        )
        return EDIT_MENU
    
    # Check if at least one identifier is present
    required_fields = ['phone', 'facebook_link', 'telegram_name', 'telegram_id']
    # Also check telegram_user for backward compatibility
    has_identifier = any(user_data.get(field) for field in required_fields) or user_data.get('telegram_user')
    
    if not has_identifier:
        await query.edit_message_text(
            "❌ Ошибка: Необходимо указать минимум одно из полей:\n"
            "Phone, Facebook Link, Telegram Name или Telegram ID!\n\n"
            "Выберите поле для редактирования:",
            reply_markup=get_edit_field_keyboard(user_id)
        )
        return EDIT_MENU
    
    # Get Supabase client
    client = get_supabase_client()
    if not client:
        await query.edit_message_text(
            "❌ Ошибка: Не удалось подключиться к базе данных.",
            reply_markup=get_main_menu_keyboard()
        )
        if user_id in user_data_store:
            del user_data_store[user_id]
        if user_id in user_data_store_access_time:
            del user_data_store_access_time[user_id]
        return ConversationHandler.END
    
    # Prepare update data (remove id and created_at)
    update_data = {k: v for k, v in user_data.items() if k not in ['id', 'created_at']}
    
    # Normalize phone if present
    if 'phone' in update_data and update_data['phone']:
        update_data['phone'] = normalize_phone(update_data['phone'])
    
    # Map telegram_name to telegram_user for database (backward compatibility)
    if 'telegram_name' in update_data:
        update_data['telegram_user'] = update_data.pop('telegram_name')
    
    try:
        # Update lead in database
        response = client.table(TABLE_NAME).update(update_data).eq("id", lead_id).execute()
        
        if response.data:
            await query.edit_message_text(
                "✅ Лид успешно обновлен!",
                reply_markup=get_main_menu_keyboard()
            )
            logger.info(f"Updated lead {lead_id}: {update_data}")
        else:
            await query.edit_message_text(
                "❌ Ошибка: Данные не были обновлены. Попробуйте снова.",
                reply_markup=get_main_menu_keyboard()
            )
    
    except Exception as e:
        logger.error(f"Error updating lead: {e}", exc_info=True)
        error_msg = "❌ Произошла ошибка при обновлении данных."
        if DEBUG_MODE:
            error_msg += f"\n\nДетали: {str(e)}"
        else:
            error_msg += " Попробуйте позже или обратитесь к администратору."
        await query.edit_message_text(
            error_msg,
            reply_markup=get_main_menu_keyboard()
        )
    
    # Clean up
    if user_id in user_data_store:
        del user_data_store[user_id]
    if user_id in user_data_store_access_time:
        del user_data_store_access_time[user_id]
    if 'editing_lead_id' in context.user_data:
        del context.user_data['editing_lead_id']
    
    return ConversationHandler.END

async def edit_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel editing lead"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id in user_data_store:
        del user_data_store[user_id]
    if user_id in user_data_store_access_time:
        del user_data_store_access_time[user_id]
    if 'editing_lead_id' in context.user_data:
        del context.user_data['editing_lead_id']
    
    await query.edit_message_text(
        "❌ Редактирование отменено.",
        reply_markup=get_main_menu_keyboard()
    )
    return ConversationHandler.END

# Edit field callbacks
async def edit_field_fullname_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await edit_field_callback(update, context, 'fullname', 'Full Name', EDIT_FULLNAME)

async def edit_field_phone_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await edit_field_callback(update, context, 'phone', 'Phone', EDIT_PHONE)

async def edit_field_email_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await edit_field_callback(update, context, 'email', 'Email', EDIT_EMAIL)

async def edit_field_country_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await edit_field_callback(update, context, 'country', 'Country', EDIT_COUNTRY)

# Facebook ID and Username callbacks removed - using only Facebook Link now

async def edit_field_fb_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await edit_field_callback(update, context, 'facebook_link', 'Facebook Link', EDIT_FB_LINK)

async def edit_field_telegram_name_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await edit_field_callback(update, context, 'telegram_name', 'Telegram Name', EDIT_TELEGRAM_NAME)

async def edit_field_telegram_id_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await edit_field_callback(update, context, 'telegram_id', 'Telegram ID', EDIT_TELEGRAM_ID)

async def edit_field_manager_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await edit_field_callback(update, context, 'manager_name', 'Manager Name', EDIT_MANAGER_NAME)

# Flask routes
@app.route('/')
def index():
    """Health check endpoint for Koyeb"""
    return jsonify({
        "status": "ok",
        "service": "telegram-bot",
        "timestamp": datetime.utcnow().isoformat()
    }), 200

@app.route('/health')
def health():
    """Health check endpoint (alias for compatibility)"""
    return jsonify({
        "status": "ok",
        "service": "telegram-bot",
        "timestamp": datetime.utcnow().isoformat()
    }), 200

@app.route('/ready')
def ready():
    """Readiness probe endpoint for Koyeb"""
    checks = {
        "telegram_app": telegram_app is not None,
        "supabase_client": supabase is not None,
        "telegram_event_loop": telegram_event_loop is not None and telegram_event_loop.is_running() if telegram_event_loop else False
    }
    
    all_ready = all(checks.values())
    status_code = 200 if all_ready else 503
    
    return jsonify({
        "status": "ready" if all_ready else "not ready",
        "checks": checks,
        "timestamp": datetime.utcnow().isoformat()
    }), status_code

def cleanup_on_shutdown():
    """Cleanup resources on shutdown"""
    global shutdown_requested, telegram_app, telegram_event_loop, supabase
    
    shutdown_requested = True
    logger.info("Shutdown requested, cleaning up resources...")
    
    try:
        # Stop Telegram app
        if telegram_app:
            import asyncio
            if telegram_event_loop and telegram_event_loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    telegram_app.stop(),
                    telegram_event_loop
                )
                asyncio.run_coroutine_threadsafe(
                    telegram_app.shutdown(),
                    telegram_event_loop
                )
            logger.info("Telegram app stopped")
    except Exception as e:
        logger.error(f"Error stopping Telegram app: {e}", exc_info=True)
    
    # Clear cache
    uniqueness_cache.clear()
    logger.info("Cache cleared")
    
    logger.info("Shutdown complete")

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        cleanup_on_shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    logger.info("Signal handlers registered")

@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle incoming Telegram updates via webhook"""
    try:
        json_data = request.get_json()
        if json_data:
            logger.info(f"Received webhook update: {json_data.get('update_id', 'unknown')}")
            
            # Check if telegram_app is initialized
            if telegram_app is None:
                logger.error("Telegram app is not initialized yet")
                return "Service not ready", 503
            
            update = Update.de_json(json_data, telegram_app.bot)
            
            # Process update asynchronously using the telegram event loop
            import asyncio
            if telegram_event_loop and telegram_event_loop.is_running():
                # Schedule update processing in the telegram event loop
                asyncio.run_coroutine_threadsafe(
                    telegram_app.process_update(update),
                    telegram_event_loop
                )
            else:
                # Fallback: process synchronously if loop not ready
                logger.warning("Telegram event loop not ready, processing update synchronously")
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(telegram_app.process_update(update))
                loop.close()
            
            logger.info(f"Update queued for processing: {update.update_id}")
        else:
            logger.warning("Received empty webhook request")
        return "OK", 200
    except Exception as e:
        logger.error(f"Error processing webhook: {e}", exc_info=True)
        return "Error", 500

async def setup_webhook():
    """Set up the webhook for Telegram bot"""
    try:
        webhook_url = f"{WEBHOOK_URL}/webhook"
        await telegram_app.bot.set_webhook(url=webhook_url)
        logger.info(f"Webhook set to: {webhook_url}")
    except Exception as e:
        logger.error(f"Error setting webhook: {e}")

# Initialize Telegram application
telegram_app = None
telegram_event_loop = None

def initialize_telegram_app():
    """Initialize Telegram app - called on module import (needed for gunicorn)"""
    global telegram_app, telegram_event_loop
    
    logger.info("Starting Telegram app initialization...")
    
    # Validate environment variables first
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not found - Telegram app will not be initialized")
        return
    
    if not WEBHOOK_URL:
        logger.error("WEBHOOK_URL not found - Telegram app will not be initialized")
        return
    
    logger.info("Environment variables validated, creating Telegram app...")
    
    try:
        # Create Telegram app
        create_telegram_app()
        logger.info("Telegram app created successfully")
    except Exception as e:
        logger.error(f"Failed to create Telegram app: {e}", exc_info=True)
        return
    
    # Initialize Telegram bot in a separate thread
    import asyncio
    import threading
    
    def run_telegram_setup():
        """Run async webhook setup and start update processing in a separate thread"""
        global telegram_event_loop
        try:
            logger.info("Starting Telegram setup in background thread...")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            telegram_event_loop = loop  # Save reference for webhook
            logger.info("Event loop created, initializing Telegram app...")
            
            loop.run_until_complete(telegram_app.initialize())
            logger.info("Telegram app initialized, setting up webhook...")
            
            loop.run_until_complete(setup_webhook())
            logger.info("Webhook set up successfully, starting update processing...")
            
            # Start processing updates
            loop.run_until_complete(telegram_app.start())
            logger.info("Telegram app started successfully, entering event loop...")
            
            # Keep the loop running to process updates
            loop.run_forever()
        except Exception as e:
            logger.error(f"Error in Telegram setup thread: {e}", exc_info=True)
    
    # Start webhook setup in background
    setup_thread = threading.Thread(target=run_telegram_setup)
    setup_thread.daemon = True
    setup_thread.start()
    
    logger.info("Telegram app initialization started in background thread")

def create_telegram_app():
    """Create and configure Telegram application"""
    global telegram_app
    
    # Create application
    telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Add command handlers
    telegram_app.add_handler(CommandHandler("start", start_command))
    telegram_app.add_handler(CommandHandler("q", quit_command))
    # Note: /q command has high priority and will work from any state
    
    # Add callback query handler for menu navigation buttons and edit lead
    telegram_app.add_handler(CallbackQueryHandler(button_callback, pattern="^(main_menu|check_menu|edit_lead_)"))
    
    # Conversation handlers for checking
    check_telegram_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(check_telegram_callback, pattern="^check_telegram$")],
        states={CHECK_BY_TELEGRAM: [MessageHandler(filters.TEXT & ~filters.COMMAND, check_telegram_input)]},
        fallbacks=[CommandHandler("q", quit_command)],
        per_message=False,
    )
    
    check_fb_link_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(check_fb_link_callback, pattern="^check_fb_link$")],
        states={CHECK_BY_FB_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, check_fb_link_input)]},
        fallbacks=[CommandHandler("q", quit_command)],
        per_message=False,
    )
    
    check_fb_username_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(check_fb_username_callback, pattern="^check_fb_username$")],
        states={CHECK_BY_FB_USERNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, check_fb_username_input)]},
        fallbacks=[CommandHandler("q", quit_command)],
        per_message=False,
    )
    
    check_fb_id_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(check_fb_id_callback, pattern="^check_fb_id$")],
        states={CHECK_BY_FB_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, check_fb_id_input)]},
        fallbacks=[CommandHandler("q", quit_command)],
        per_message=False,
    )
    
    check_phone_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(check_phone_callback, pattern="^check_phone$")],
        states={CHECK_BY_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, check_phone_input)]},
        fallbacks=[CommandHandler("q", quit_command)],
        per_message=False,
    )
    
    check_fullname_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(check_fullname_callback, pattern="^check_fullname$")],
        states={CHECK_BY_FULLNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, check_fullname_input)]},
        fallbacks=[CommandHandler("q", quit_command)],
        per_message=False,
    )
    
    # Conversation handler for adding - sequential flow
    add_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(add_new_callback, pattern="^add_new$")],
        states={
            ADD_FULLNAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input),
                CallbackQueryHandler(add_back_callback, pattern="^add_back$"),
                CallbackQueryHandler(add_cancel_callback, pattern="^add_cancel$"),
            ],
            ADD_MANAGER_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input),
                CallbackQueryHandler(add_back_callback, pattern="^add_back$"),
                CallbackQueryHandler(add_cancel_callback, pattern="^add_cancel$"),
            ],
            ADD_PHONE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input),
                CallbackQueryHandler(add_skip_callback, pattern="^add_skip$"),
                CallbackQueryHandler(add_back_callback, pattern="^add_back$"),
                CallbackQueryHandler(add_cancel_callback, pattern="^add_cancel$"),
            ],
            ADD_FB_LINK: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input),
                CallbackQueryHandler(add_skip_callback, pattern="^add_skip$"),
                CallbackQueryHandler(add_back_callback, pattern="^add_back$"),
                CallbackQueryHandler(add_cancel_callback, pattern="^add_cancel$"),
            ],
            ADD_TELEGRAM_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input),
                CallbackQueryHandler(add_skip_callback, pattern="^add_skip$"),
                CallbackQueryHandler(add_back_callback, pattern="^add_back$"),
                CallbackQueryHandler(add_cancel_callback, pattern="^add_cancel$"),
            ],
            ADD_TELEGRAM_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input),
                CallbackQueryHandler(add_skip_callback, pattern="^add_skip$"),
                CallbackQueryHandler(add_back_callback, pattern="^add_back$"),
                CallbackQueryHandler(add_cancel_callback, pattern="^add_cancel$"),
            ],
            ADD_EMAIL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input),
                CallbackQueryHandler(add_skip_callback, pattern="^add_skip$"),
                CallbackQueryHandler(add_back_callback, pattern="^add_back$"),
                CallbackQueryHandler(add_cancel_callback, pattern="^add_cancel$"),
            ],
            ADD_COUNTRY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input),
                CallbackQueryHandler(add_skip_callback, pattern="^add_skip$"),
                CallbackQueryHandler(add_back_callback, pattern="^add_back$"),
                CallbackQueryHandler(add_cancel_callback, pattern="^add_cancel$"),
            ],
            ADD_REVIEW: [
                CallbackQueryHandler(add_save_callback, pattern="^add_save$"),
                CallbackQueryHandler(add_back_callback, pattern="^add_back$"),
                CallbackQueryHandler(add_cancel_callback, pattern="^add_cancel$"),
            ],
        },
        fallbacks=[CommandHandler("q", quit_command)],
        per_message=False,
    )
    
    # Register all handlers
    telegram_app.add_handler(check_telegram_conv)
    telegram_app.add_handler(check_fb_link_conv)
    telegram_app.add_handler(check_fb_username_conv)
    telegram_app.add_handler(check_fb_id_conv)
    telegram_app.add_handler(check_phone_conv)
    telegram_app.add_handler(check_fullname_conv)
    telegram_app.add_handler(add_conv)
    
    # Edit conversation handler
    edit_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(edit_field_fullname_callback, pattern="^edit_field_fullname$"),
            CallbackQueryHandler(edit_field_phone_callback, pattern="^edit_field_phone$"),
            CallbackQueryHandler(edit_field_email_callback, pattern="^edit_field_email$"),
            CallbackQueryHandler(edit_field_country_callback, pattern="^edit_field_country$"),
            CallbackQueryHandler(edit_field_fb_link_callback, pattern="^edit_field_fb_link$"),
            CallbackQueryHandler(edit_field_telegram_name_callback, pattern="^edit_field_telegram_name$"),
            CallbackQueryHandler(edit_field_telegram_id_callback, pattern="^edit_field_telegram_id$"),
            CallbackQueryHandler(edit_field_manager_callback, pattern="^edit_field_manager$"),
            CallbackQueryHandler(edit_save_callback, pattern="^edit_save$"),
            CallbackQueryHandler(edit_cancel_callback, pattern="^edit_cancel$"),
        ],
        states={
            EDIT_MENU: [
                CallbackQueryHandler(edit_field_fullname_callback, pattern="^edit_field_fullname$"),
                CallbackQueryHandler(edit_field_phone_callback, pattern="^edit_field_phone$"),
                CallbackQueryHandler(edit_field_email_callback, pattern="^edit_field_email$"),
                CallbackQueryHandler(edit_field_country_callback, pattern="^edit_field_country$"),
                CallbackQueryHandler(edit_field_fb_link_callback, pattern="^edit_field_fb_link$"),
                CallbackQueryHandler(edit_field_telegram_name_callback, pattern="^edit_field_telegram_name$"),
                CallbackQueryHandler(edit_field_telegram_id_callback, pattern="^edit_field_telegram_id$"),
                CallbackQueryHandler(edit_field_manager_callback, pattern="^edit_field_manager$"),
                CallbackQueryHandler(edit_save_callback, pattern="^edit_save$"),
                CallbackQueryHandler(edit_cancel_callback, pattern="^edit_cancel$"),
            ],
            EDIT_FULLNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
            EDIT_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
            EDIT_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
            EDIT_COUNTRY: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
            EDIT_FB_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
            EDIT_TELEGRAM_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
            EDIT_TELEGRAM_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
            EDIT_MANAGER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
        },
        fallbacks=[CommandHandler("q", quit_command)],
        per_message=False,
    )
    
    telegram_app.add_handler(edit_conv)
    
    logger.info("Telegram application initialized")
    return telegram_app

# Setup signal handlers for graceful shutdown
setup_signal_handlers()

# Initialize Telegram app when module is imported (needed for gunicorn)
# This ensures telegram_app is initialized even when running with gunicorn
try:
    initialize_telegram_app()
except Exception as e:
    logger.error(f"Failed to initialize Telegram app on module import: {e}", exc_info=True)

if __name__ == '__main__':
    # Validate environment variables
    missing_vars = []
    
    if not TELEGRAM_BOT_TOKEN:
        missing_vars.append("TELEGRAM_BOT_TOKEN")
    
    if not WEBHOOK_URL:
        missing_vars.append("WEBHOOK_URL")
    
    if not SUPABASE_URL:
        missing_vars.append("SUPABASE_URL")
    
    if not SUPABASE_KEY:
        missing_vars.append("SUPABASE_KEY")
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set all required environment variables in Koyeb dashboard")
        exit(1)
    
    # Note: Supabase client will be initialized lazily on first use
    logger.info("Supabase client will be initialized on first use")
    
    # Telegram app is already initialized by initialize_telegram_app() above
    # Give it a moment to initialize
    import time
    time.sleep(2)
    
    # For production, gunicorn will be used (see Procfile)
    # This code is kept for local development
    logger.info(f"Starting Flask development server on port {PORT}")
    logger.warning("For production, use: gunicorn -w 1 -b 0.0.0.0:$PORT main:app")
    app.run(host='0.0.0.0', port=PORT, debug=False)

import os
# Set environment variables to disable proxy before importing supabase
# This prevents httpx.Client from receiving 'proxy' argument which is not supported in newer httpx versions
os.environ.setdefault("NO_PROXY", "*")
os.environ.setdefault("HTTPX_NO_PROXY", "1")

# Configure logging - only errors and warnings in production
import logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.WARNING
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
            kwargs.pop('proxy')
        return original_httpx_client_init(self, *args, **kwargs)
    
    # Apply monkeypatch
    httpx.Client.__init__ = patched_httpx_client_init
except Exception:
    pass

from datetime import datetime
import time
import signal
import sys
import threading
from functools import wraps
from flask import Flask, request, jsonify
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes, ConversationHandler

from supabase import create_client, Client
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Initialize Flask app
app = Flask(__name__)

# Environment variables - all required except PORT (set by Koyeb)
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
WEBHOOK_URL = os.environ.get('WEBHOOK_URL')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
TABLE_NAME = os.environ.get('TABLE_NAME', 'facebook_leads')  # Default table name
PORT = int(os.environ.get('PORT', 8000))  # Default port, usually set by Koyeb

# Supabase client - thread-safe, can be used concurrently by multiple users
supabase: Client = None

# Keep-alive scheduler
scheduler = None

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
            if not SUPABASE_KEY:
                logger.error("SUPABASE_KEY not found in environment variables")
                return None
            
            if not SUPABASE_URL:
                logger.error("SUPABASE_URL not found in environment variables")
                return None
            
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
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

def normalize_telegram_id(tg_id: str) -> str:
    """Normalize Telegram ID: extract only digits (similar to phone)"""
    if not tg_id:
        return ""
    # Remove all non-digit characters
    return ''.join(filter(str.isdigit, tg_id))

def normalize_text_field(text: str) -> str:
    """Normalize text field (fullname, manager_name): trim spaces, collapse multiple spaces, limit length"""
    if not text:
        return ""
    # Trim leading/trailing whitespace
    normalized = text.strip()
    # Collapse multiple spaces into single space
    normalized = ' '.join(normalized.split())
    # Remove any control characters (keep only printable characters)
    normalized = ''.join(char for char in normalized if char.isprintable() or char.isspace())
    # Final trim after cleaning
    normalized = normalized.strip()
    # Limit to 500 characters to prevent database issues
    if len(normalized) > 500:
        normalized = normalized[:500]
    return normalized

def escape_html(text: str) -> str:
    """Escape HTML special characters"""
    if not text:
        return text
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def format_facebook_link_for_display(value: str) -> str:
    """Format Facebook link value to full URL for display
    
    Args:
        value: Facebook link value from database (can be ID or username)
    
    Returns:
        Formatted Facebook URL with https://
    """
    if not value:
        return value
    
    value = str(value).strip()
    
    # If already a full URL, return as is
    if value.startswith('http://') or value.startswith('https://'):
        return value
    
    # Check if value is only digits (Facebook ID)
    if value.isdigit():
        # Format as profile.php?id=...
        return f"https://www.facebook.com/profile.php?id={value}"
    else:
        # Format as username link
        return f"https://www.facebook.com/{value}"

def get_user_friendly_error(error: Exception, operation: str = "операция") -> str:
    """Convert technical errors to user-friendly messages"""
    error_str = str(error).lower()
    
    # Database connection errors
    if 'connection' in error_str or 'timeout' in error_str or 'network' in error_str:
        return (
            f"⚠️ Проблема с подключением к базе данных.\n\n"
            f"ℹ️ Что можно сделать:\n"
            f"• Проверьте интернет-соединение\n"
            f"• Попробуйте через несколько секунд\n"
            f"• Если проблема сохраняется, обратитесь к администратору"
        )
    
    # Database query errors
    if 'postgres' in error_str or 'database' in error_str or 'query' in error_str:
        return (
            f"⚠️ Ошибка при выполнении запроса к базе данных.\n\n"
            f"ℹ️ Попробуйте:\n"
            f"• Повторить операцию\n"
            f"• Проверить правильность введенных данных"
        )
    
    # Validation errors (already user-friendly)
    if 'не может быть пустым' in error_str or 'неверный формат' in error_str:
        return str(error)
    
    # Unknown errors
    return (
        f"❌ Произошла ошибка при {operation}.\n\n"
        f"ℹ️ Попробуйте:\n"
        f"• Повторить операцию\n"
        f"• Проверить введенные данные\n"
        f"• Обратиться к администратору, если проблема сохраняется"
    )

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

def validate_facebook_link(link: str) -> tuple[bool, str, str]:
    """
    Validate Facebook link and extract username or ID.
    Supports various Facebook URL formats:
    - https://www.facebook.com/profile.php?id=123456 → 123456 (only ID)
    - https://www.facebook.com/profile.php?id=123456&ref=... → 123456 (only ID)
    - https://www.facebook.com/markl1n → markl1n
    - www.facebook.com/markl1n → markl1n
    - facebook.com/markl1n → markl1n
    - https://www.facebook.com/profile/username → username
    - https://m.facebook.com/username → username
    - m.facebook.com/username → username
    """
    if not link:
        return False, "Facebook ссылка не может быть пустой", ""
    
    link_clean = link.strip()
    
    # Remove @ if present at the beginning
    if link_clean.startswith('@'):
        link_clean = link_clean[1:]
    
    # Check if it's a valid Facebook URL - support more formats
    # Accept: https://www.facebook.com/..., http://www.facebook.com/..., 
    # www.facebook.com/..., facebook.com/..., m.facebook.com/...
    facebook_patterns = [
        r'https?://(www\.)?(m\.)?facebook\.com/',
        r'^(www\.)?facebook\.com/',
        r'^m\.facebook\.com/'
    ]
    
    is_facebook_url = False
    for pattern in facebook_patterns:
        if re.search(pattern, link_clean, re.IGNORECASE):
            is_facebook_url = True
            break
    
    # Also check for formats without protocol explicitly
    if not is_facebook_url:
        link_lower = link_clean.lower()
        if (link_lower.startswith('www.facebook.com/') or 
            link_lower.startswith('facebook.com/') or 
            link_lower.startswith('m.facebook.com/')):
            is_facebook_url = True
    
    if not is_facebook_url:
        error_msg = (
            "❌ Неверный формат Facebook ссылки.\n\n"
            "📋 Примеры допустимых вариантов:\n"
            "• <code>https://www.facebook.com/username</code>\n"
            "• <code>www.facebook.com/username</code>\n"
            "• <code>facebook.com/username</code>\n"
            "• <code>https://m.facebook.com/profile.php?id=123456789012345</code>\n\n"
            "💡 Можно вставлять ссылку целиком, бот сам извлечёт username или ID."
        )
        return False, error_msg, ""
    
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
    
    # Handle profile.php?id= format or any link with id= parameter - extract ONLY the ID number
    if 'id=' in link_clean:
        # Extract ID from query string - look for id= parameter
        # Handle cases like:
        # - profile.php?id=123456
        # - profile.php?id=123456&ref=...
        # - profile.php?id=123456] (with trailing characters)
        # - profile.php?id=123456/extra/path (with additional paths)
        # - ?id=123456 (without profile.php)
        # - /people/Name/123456 (alternative format)
        
        # Extract everything after id=
        id_part_raw = link_clean.split('id=')[-1]
        
        # Remove query parameters (&), hash fragments (#), and any trailing characters
        # Extract only the numeric ID part, ignoring any non-digit characters after it
        id_part = ""
        for char in id_part_raw:
            if char.isdigit():
                id_part += char
            elif char in ['&', '#', '?', '/', '\\', ']', '[', ')', '(', '}', '{', ' ', '\t', '\n']:
                # Stop at first non-digit separator character
                break
            else:
                # If we encounter a non-digit, non-separator character, it might be part of the ID
                # But typically Facebook IDs are only digits, so we stop here
                break
        
        # Also check for alternative format: /people/Name/123456
        if not id_part or not id_part.isdigit():
            # Try to extract ID from path like /people/Name/123456
            path_parts = link_clean.split('/')
            for part in reversed(path_parts):
                # Extract only digits from the part
                digits_only = ''.join(filter(str.isdigit, part))
                if digits_only and len(digits_only) > 5:  # Facebook IDs are usually long numbers
                    id_part = digits_only
                    break
        
        # Validate that ID contains only digits and has reasonable length
        if id_part and id_part.isdigit() and len(id_part) >= 5:
            return True, "", id_part
        else:
            return False, "Не удалось извлечь Facebook ID из ссылки. Убедитесь, что ссылка содержит корректный ID.", ""
    
    # For username format: extract just the username (last part after /)
    # Remove query parameters if present
    if '?' in link_clean:
        link_clean = link_clean.split('?')[0]
    
    # Remove hash fragments if present
    if '#' in link_clean:
        link_clean = link_clean.split('#')[0]
    
    # Remove trailing slash and any trailing non-alphanumeric characters
    link_clean = link_clean.rstrip('/')
    
    # Remove any trailing brackets, parentheses, or other special characters
    # Keep only alphanumeric, dots, underscores, and hyphens for username
    while link_clean and not link_clean[-1].isalnum() and link_clean[-1] not in ['.', '_', '-']:
        link_clean = link_clean[:-1]
    
    # Extract username (last part after /)
    parts = link_clean.split('/')
    if len(parts) > 0:
        # Get the last non-empty part (username)
        extracted = parts[-1] if parts[-1] else (parts[-2] if len(parts) > 1 else "")
        
        # Clean extracted username from any trailing special characters
        if extracted:
            # Remove any trailing non-alphanumeric characters (except dots, underscores, hyphens)
            cleaned_username = extracted
            while cleaned_username and not cleaned_username[-1].isalnum() and cleaned_username[-1] not in ['.', '_', '-']:
                cleaned_username = cleaned_username[:-1]
            
            if cleaned_username:
                return True, "", cleaned_username
    
    error_msg = (
        "❌ Неверный формат Facebook ссылки.\n\n"
        "📋 Примеры допустимых вариантов:\n"
        "• <code>https://www.facebook.com/username</code>\n"
        "• <code>www.facebook.com/username</code>\n"
        "• <code>facebook.com/username</code>\n"
        "• <code>https://m.facebook.com/profile.php?id=123456789012345</code>\n\n"
        "💡 Можно вставлять ссылку целиком, бот сам извлечёт username или ID."
    )
    return False, error_msg, ""

def validate_telegram_name(tg_name: str) -> tuple[bool, str, str]:
    """Validate Telegram name: remove @ if present, remove all spaces, check not empty"""
    if not tg_name:
        return False, "Имя пользователя Telegram не может быть пустым", ""
    # Remove all spaces (not just trim)
    normalized = tg_name.replace(' ', '').replace('\t', '').replace('\n', '')
    # Remove all @ symbols (handle multiple @)
    normalized = normalized.replace('@', '')
    # Trim any remaining whitespace
    normalized = normalized.strip()
    if not normalized:
        return False, "Имя пользователя Telegram не может быть пустым", ""
    return True, "", normalized

def validate_telegram_id(tg_id: str) -> tuple[bool, str, str]:
    """Validate Telegram ID: must contain only digits"""
    if not tg_id:
        return False, "Telegram ID не может быть пустым", ""
    # Check that input contains only digits
    if not tg_id.isdigit():
        return False, "Telegram ID должен содержать только цифры", ""
    normalized = normalize_telegram_id(tg_id)
    if not normalized:
        return False, "Telegram ID не может быть пустым", ""
    return True, "", normalized

def get_field_format_requirements(field_name: str) -> str:
    """Get format requirements description for a field"""
    requirements = {
        'fullname': (
            "📋 <b>Требования к формату:</b>\n"
            "• Поле <b>обязательное</b> для заполнения\n"
            "• Введите имя и фамилию клиента\n"
            "• Можно использовать любые буквы (русские, латинские)\n"
            "• Пробелы между словами разрешены\n\n"
            "💡 <b>Примеры:</b>\n"
            "<code>Иван Иванов</code>\n"
            "<code>John Smith</code>\n"
            "<code>Мария Петрова-Сидорова</code>"
        ),
        'manager_name': (
            "📋 <b>Требования к формату:</b>\n"
            "• Поле <b>обязательное</b> для заполнения\n"
            "• Введите стейдж менеджера (так менеджер записан в отчётности)\n"
            "• Можно использовать любые буквы (русские, латинские)\n"
            "• Пробелы между словами разрешены\n\n"
            "💡 <b>Примеры:</b>\n"
            "<code>Анна</code>\n"
            "<code>Петр Сидоров</code>\n"
            "<code>Maria</code>"
        ),
        'phone': (
            "⚠️ Требования к формату:\n"
            "• Обязателен код страны\n"
            "• Только цифры (без пробелов)\n"
            "• Без знака \"+\" в начале\n"
            "• От 10 до 15 цифр\n"
            "Примеры: 79001234567, 380501234567"
        ),
        'facebook_link': (
            "Примеры:\n"
            "<code>https://www.facebook.com/username</code>\n"
            "<code>www.facebook.com/username</code>\n"
            "<code>facebook.com/username</code>\n"
            "<code>https://www.facebook.com/profile.php?id=123456789012345</code>\n"
            "<code>https://m.facebook.com/username</code>\n\n"
            "⚠️ Ссылка должна начинаться с http:// или https://\n\n"
            "‼️ Важно: добавляйте только прямую ссылку на профиль (без фото, информации и прочих вкладок).\n\n"
            "Пример: <code>facebook.com/username</code> ✅\n"
            "А не ссылки с лишними символами ❌"
        ),
        'telegram_name': (
            "📋 <b>Требования к формату:</b>\n"
            "• Поле <b>опциональное</b> (можно пропустить)\n"
            "• Введите username без символа @\n"
            "• Можно использовать буквы, цифры, подчёркивания\n"
            "• Пробелы не допускаются\n\n"
            "💡 <b>Примеры:</b>\n"
            "<code>username</code>\n"
            "<code>Ivan_123</code>\n"
            "<code>user123</code>\n"
            "<code>john_doe</code>\n\n"
            "⚠️ <b>Важно:</b> Не указывайте символ @ в начале"
        ),
        'telegram_id': (
            "⚠️ Требования к формату:\n"
            "• Только цифры (без букв и символов)\n"
            "• Без пробелов\n"
            "• Минимум 1 цифра\n"
            "Примеры: 12345, 789, 999888777"
        )
    }
    return requirements.get(field_name, "")

def get_field_label(field_name: str) -> str:
    """Get Russian label for field"""
    labels = {
        'fullname': 'имя клиента',
        'manager_name': 'имя агента',
        'phone': 'Номер телефона',
        'facebook_link': 'ссылку клиента',
        'telegram_name': 'username клиента',
        'telegram_id': 'ID клиента'
    }
    return labels.get(field_name, field_name)

def get_next_add_field(current_field: str) -> tuple[str, int, int, int]:
    """Get next field in the add flow. Returns (field_name, state, current_step, total_steps)"""
    field_sequence = [
        ('fullname', ADD_FULLNAME),
        ('manager_name', ADD_MANAGER_NAME),
        ('phone', ADD_PHONE),
        ('facebook_link', ADD_FB_LINK),
        ('telegram_name', ADD_TELEGRAM_NAME),
        ('telegram_id', ADD_TELEGRAM_ID),
    ]
    total_steps = len(field_sequence) + 1  # +1 for review step
    
    if not current_field:
        return field_sequence[0][0], field_sequence[0][1], 1, total_steps
    
    for i, (field, state) in enumerate(field_sequence):
        if field == current_field:
            if i + 1 < len(field_sequence):
                return field_sequence[i + 1][0], field_sequence[i + 1][1], i + 2, total_steps
            else:
                return ('review', ADD_REVIEW, total_steps, total_steps)
    
    return field_sequence[0][0], field_sequence[0][1], 1, total_steps

def get_navigation_keyboard(is_optional: bool = False, show_back: bool = True) -> InlineKeyboardMarkup:
    """Get navigation keyboard for field input"""
    keyboard = []
    
    # Кнопка "Пропустить" сверху на 100% ширины (если поле опциональное)
    if is_optional:
        keyboard.append([InlineKeyboardButton("⏭️ Пропустить", callback_data="add_skip")])
    
    # Кнопки "Назад" и "Главное меню" в один ряд (по 50% каждая)
    if show_back:
        keyboard.append([
            InlineKeyboardButton("◀️ Назад", callback_data="add_back"),
            InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
        ])
    else:
        # Если кнопка "Назад" не нужна, только "Главное меню" на 100%
        keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
    
    return InlineKeyboardMarkup(keyboard)


# Conversation states
(
    # Check states
    CHECK_BY_TELEGRAM,
    CHECK_BY_FB_LINK,
    CHECK_BY_TELEGRAM_ID,
    CHECK_BY_PHONE,
    CHECK_BY_FULLNAME,
    # Add states (sequential flow)
    ADD_FULLNAME,
    ADD_MANAGER_NAME,
    ADD_PHONE,
    ADD_FB_LINK,
    ADD_TELEGRAM_NAME,
    ADD_TELEGRAM_ID,
    ADD_REVIEW,  # Review before saving
    # Edit states
    EDIT_MENU,
    EDIT_FULLNAME,
    EDIT_PHONE,
    EDIT_FB_LINK,
    EDIT_TELEGRAM_NAME,
    EDIT_TELEGRAM_ID,
    EDIT_MANAGER_NAME,
    EDIT_PIN
) = range(20)

# Store user data during conversation - isolated per user_id for concurrent access
# Each user's data is stored separately, allowing 10+ managers to work simultaneously
user_data_store = {}
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
        [InlineKeyboardButton("📱 Имя пользователя Telegram", callback_data="check_telegram")],
        [InlineKeyboardButton("🔗 Facebook Ссылка", callback_data="check_fb_link")],
        [InlineKeyboardButton("🆔 Telegram ID", callback_data="check_telegram_id")],
        [InlineKeyboardButton("🔢 Номер телефона", callback_data="check_phone")],
        [InlineKeyboardButton("👤 Клиент", callback_data="check_fullname")],
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

# Command handlers
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command - show main menu"""
    try:
        # Clean up all intermediate messages before showing main menu
        await cleanup_all_messages_before_main_menu(update, context)
        
        welcome_message = (
            "👋 Добро пожаловать в ClientsBot!\n\n"
            "Выберите действие:"
        )
        await update.message.reply_text(
            welcome_message,
            reply_markup=get_main_menu_keyboard()
        )
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
        # Clear any conversation state - explicitly clear all keys to ensure ConversationHandler ends
        if context.user_data:
            # Remove all conversation-related keys
            keys_to_remove = [
                'current_field', 'current_state', 'add_step', 'editing_lead_id',
                'last_check_messages', 'add_message_ids', 'check_by', 'check_value',
                'check_results', 'selected_lead_id'
            ]
            for key in keys_to_remove:
                if key in context.user_data:
                    del context.user_data[key]
            # Also clear any remaining state
            context.user_data.clear()
        
        # Clear user data store if exists
        user_id = update.effective_user.id
        if user_id in user_data_store:
            del user_data_store[user_id]
        if user_id in user_data_store_access_time:
            del user_data_store_access_time[user_id]
        
        # Show main menu FIRST (fast response)
        welcome_message = (
            "👋 Главное меню\n\n"
            "Выберите действие:"
        )
        sent_message = await update.message.reply_text(
            welcome_message,
            reply_markup=get_main_menu_keyboard()
        )
        
        # Clean up messages AFTER showing menu (in background, don't wait)
        # This ensures fast response to user
        # Use application.create_task to ensure it runs in the correct event loop
        context.application.create_task(cleanup_all_messages_before_main_menu(update, context))
        
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
    
    # Note: edit_lead_* callbacks are now handled by ConversationHandler (edit_lead_entry_callback)
    
    if data == "main_menu":
        # Get current message ID to exclude from cleanup
        current_message_id = query.message.message_id if query.message else None
        
        # IMPORTANT: Remove current message from cleanup lists BEFORE cleanup
        # This prevents it from being deleted even if cleanup runs quickly
        if current_message_id:
            if 'add_message_ids' in context.user_data and current_message_id in context.user_data['add_message_ids']:
                context.user_data['add_message_ids'].remove(current_message_id)
            if 'last_check_messages' in context.user_data and current_message_id in context.user_data['last_check_messages']:
                context.user_data['last_check_messages'].remove(current_message_id)
        
        try:
            # Show main menu FIRST (fast response)
            await query.edit_message_text(
                "👋 Главное меню\n\nВыберите действие:",
                reply_markup=get_main_menu_keyboard()
            )
            
            # Clean up messages AFTER showing menu (in background, don't wait)
            # This ensures fast response to user
            # Add small delay to ensure edit completes before cleanup starts
            async def delayed_cleanup():
                import asyncio
                await asyncio.sleep(0.2)  # Small delay to ensure edit completes
                await cleanup_all_messages_before_main_menu(update, context, exclude_message_id=current_message_id)
            
            # Use application.create_task to ensure it runs in the correct event loop
            context.application.create_task(delayed_cleanup())
        except Exception as e:
            # If edit fails (message was deleted), send new message
            if "not found" in str(e) or "BadRequest" in str(type(e).__name__):
                try:
                    await query.message.reply_text(
                        "👋 Главное меню\n\nВыберите действие:",
                        reply_markup=get_main_menu_keyboard()
                    )
                    # Clean up in background with delay
                    async def delayed_cleanup_fallback():
                        import asyncio
                        await asyncio.sleep(0.2)
                        await cleanup_all_messages_before_main_menu(update, context, exclude_message_id=current_message_id)
                    context.application.create_task(delayed_cleanup_fallback())
                except Exception as reply_error:
                    logger.error(f"Error sending reply message: {reply_error}", exc_info=True)
                    # Last resort: try to send without cleanup
                    try:
                        await context.bot.send_message(
                            chat_id=query.message.chat_id,
                            text="👋 Главное меню\n\nВыберите действие:",
                            reply_markup=get_main_menu_keyboard()
                        )
                    except Exception as send_error:
                        logger.error(f"Error sending message directly: {send_error}", exc_info=True)
            else:
                logger.error(f"Error in main_menu callback: {e}", exc_info=True)
                raise
    
    elif data == "check_menu":
        await query.edit_message_text(
            "✅ Проверить клиента\n\nВыберите способ проверки:",
            reply_markup=get_check_menu_keyboard()
        )
    
    elif data == "add_menu":
        await query.edit_message_text(
            "➕ Добавить клиента\n\nВыберите способ добавления:",
            reply_markup=get_add_menu_keyboard()
        )
    
    elif data == "add_new":
        # Fallback: if ConversationHandler doesn't catch this, handle it here
        logger.warning(f"add_new callback received in button_callback (should be handled by ConversationHandler)")
        try:
            # Try to call add_new_callback directly as fallback
            result = await add_new_callback(update, context)
            if result:
                return result
        except Exception as e:
            logger.error(f"Error in add_new fallback handler: {e}", exc_info=True)
            await query.answer("❌ Произошла ошибка. Попробуйте снова.")
            await query.edit_message_text(
                "❌ Произошла ошибка при запуске добавления лида.\n\n"
                "Попробуйте снова или обратитесь к администратору.",
                reply_markup=get_main_menu_keyboard()
            )
    else:
        # Unknown callback data - should not happen, but handle gracefully
        logger.warning(f"Unknown callback data received: {data}")
        await query.answer("⚠️ Неизвестная команда. Используйте меню.", show_alert=True)
        try:
            if query.message:
                await query.edit_message_text(
                    "❌ Неизвестная команда.\n\n"
                    "Используйте кнопки меню для навигации.",
                    reply_markup=get_main_menu_keyboard()
                )
        except Exception as e:
            logger.error(f"Error handling unknown callback: {e}", exc_info=True)

# Global fallback handlers
async def unknown_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle callback queries that don't match any pattern"""
    query = update.callback_query
    if query:
        try:
            await query.answer("⚠️ Неизвестная команда. Используйте меню.", show_alert=True)
            if query.message:
                await query.edit_message_text(
                    "❌ Неизвестная команда.\n\n"
                    "Используйте кнопки меню для навигации.",
                    reply_markup=get_main_menu_keyboard()
                )
        except Exception as e:
            logger.error(f"Error in unknown_callback_handler: {e}", exc_info=True)
    return ConversationHandler.END

async def unknown_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle commands sent during ConversationHandler (except /q and /start)"""
    if not update.message or not update.message.text:
        return
    
    command = update.message.text.strip().split()[0] if update.message.text else ""
    
    # Ignore /q and /start as they are handled separately
    if command in ["/q", "/start"]:
        return
    
    # Show message that command is not available during conversation
    try:
        await update.message.reply_text(
            f"⚠️ Команда {command} недоступна во время выполнения операции.\n\n"
            "Используйте /q для возврата в главное меню или завершите текущую операцию.",
            reply_markup=get_main_menu_keyboard()
        )
    except Exception as e:
        logger.error(f"Error in unknown_command_handler: {e}", exc_info=True)
    
    # Don't end conversation, let user continue or use /q
    return None

# Message cleanup functions
async def cleanup_check_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clean up old check messages when starting a new check"""
    if 'last_check_messages' in context.user_data and context.user_data['last_check_messages']:
        chat_id = update.effective_chat.id
        bot = context.bot
        message_ids = context.user_data['last_check_messages']
        
        for msg_id in message_ids:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception:
                # Message might be too old or already deleted, ignore
                pass
        
        # Clear the list
        context.user_data['last_check_messages'] = []

async def save_check_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int):
    """Save message ID for later cleanup"""
    if 'last_check_messages' not in context.user_data:
        context.user_data['last_check_messages'] = []
    context.user_data['last_check_messages'].append(message_id)

async def cleanup_add_messages(update: Update, context: ContextTypes.DEFAULT_TYPE, exclude_message_id: int = None):
    """Clean up all add flow messages except the final success message and optionally exclude a specific message"""
    user_id = update.effective_user.id
    if 'add_message_ids' in context.user_data and context.user_data['add_message_ids']:
        chat_id = update.effective_chat.id
        bot = context.bot
        message_ids = context.user_data['add_message_ids'].copy()
        
        for msg_id in message_ids:
            # Skip the message we want to exclude (e.g., review screen)
            if exclude_message_id and msg_id == exclude_message_id:
                continue
            try:
                await bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception:
                # Message might be too old or already deleted, ignore
                pass
        
        # Clear the list
        context.user_data['add_message_ids'] = []

async def save_add_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int):
    """Save message ID for later cleanup after successful add"""
    if 'add_message_ids' not in context.user_data:
        context.user_data['add_message_ids'] = []
    context.user_data['add_message_ids'].append(message_id)

async def cleanup_all_messages_before_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, exclude_message_id: int = None):
    """Clean up all intermediate bot messages before showing main menu"""
    chat_id = update.effective_chat.id
    bot = context.bot
    
    # Collect all message IDs to delete
    message_ids_to_delete = []
    
    # Clean up add flow messages
    if 'add_message_ids' in context.user_data and context.user_data['add_message_ids']:
        message_ids_to_delete.extend(context.user_data['add_message_ids'])
        context.user_data['add_message_ids'] = []
    
    # Clean up check flow messages
    if 'last_check_messages' in context.user_data and context.user_data['last_check_messages']:
        message_ids_to_delete.extend(context.user_data['last_check_messages'])
        context.user_data['last_check_messages'] = []
    
    # Remove excluded message ID if provided (double-check to be safe)
    if exclude_message_id:
        # Remove from list if present (multiple times to be absolutely sure)
        while exclude_message_id in message_ids_to_delete:
            message_ids_to_delete.remove(exclude_message_id)
    
    # Delete messages in parallel for better performance
    if message_ids_to_delete:
        import asyncio
        delete_tasks = []
        for msg_id in message_ids_to_delete:
            delete_tasks.append(
                bot.delete_message(chat_id=chat_id, message_id=msg_id)
            )
        
        # Execute deletions in parallel, but don't wait for all to complete
        # This speeds up the response
        try:
            # Use asyncio.gather with return_exceptions=True to not fail on individual errors
            results = await asyncio.gather(*delete_tasks, return_exceptions=True)
        except Exception:
            pass

# Check callbacks
async def check_telegram_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for check by telegram conversation"""
    query = update.callback_query
    if not query:
        logger.error("check_telegram_callback: query is None")
        return ConversationHandler.END
    
    await query.answer()
    
    # Clean up old check messages if any
    await cleanup_check_messages(update, context)
    
    try:
        await query.edit_message_text("📱 Введите Имя пользователя Telegram для проверки:")
    except Exception as e:
        # If message can't be edited (e.g., already deleted), send new message
        logger.warning(f"Could not edit message in check_telegram_callback: {e}")
        if query.message:
            await query.message.reply_text("📱 Введите Имя пользователя Telegram для проверки:")
        else:
            logger.error("check_telegram_callback: query.message is None")
            return ConversationHandler.END
    
    return CHECK_BY_TELEGRAM

async def check_fb_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for check by facebook link conversation"""
    query = update.callback_query
    if not query:
        logger.error("check_fb_link_callback: query is None")
        return ConversationHandler.END
    
    await query.answer()
    
    # Clean up old check messages if any
    await cleanup_check_messages(update, context)
    
    try:
        await query.edit_message_text("🔗 Введите Facebook Ссылка для проверки:")
    except Exception as e:
        # If message can't be edited (e.g., already deleted), send new message
        logger.warning(f"Could not edit message in check_fb_link_callback: {e}")
        if query.message:
            await query.message.reply_text("🔗 Введите Facebook Ссылка для проверки:")
        else:
            logger.error("check_fb_link_callback: query.message is None")
            return ConversationHandler.END
    
    return CHECK_BY_FB_LINK

async def check_phone_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for check by phone conversation"""
    query = update.callback_query
    if not query:
        logger.error("check_phone_callback: query is None")
        return ConversationHandler.END
    
    await query.answer()
    
    # Clean up old check messages if any
    await cleanup_check_messages(update, context)
    
    try:
        await query.edit_message_text("🔢 Введите номер телефона для проверки:")
    except Exception as e:
        # If message can't be edited (e.g., already deleted), send new message
        logger.warning(f"Could not edit message in check_phone_callback: {e}")
        if query.message:
            await query.message.reply_text("🔢 Введите номер телефона для проверки:")
        else:
            logger.error("check_phone_callback: query.message is None")
            return ConversationHandler.END
    
    return CHECK_BY_PHONE

async def check_fullname_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for check by fullname conversation"""
    query = update.callback_query
    if not query:
        logger.error("check_fullname_callback: query is None")
        return ConversationHandler.END
    
    await query.answer()
    
    # Clean up old check messages if any
    await cleanup_check_messages(update, context)
    
    try:
        await query.edit_message_text("👤 Введите имя клиента (или фамилию):")
    except Exception as e:
        # If message can't be edited (e.g., already deleted), send new message
        logger.warning(f"Could not edit message in check_fullname_callback: {e}")
        if query.message:
            await query.message.reply_text("👤 Введите имя клиента (или фамилию):")
        else:
            logger.error("check_fullname_callback: query.message is None")
            return ConversationHandler.END
    
    return CHECK_BY_FULLNAME

# Add callback - new sequential flow
async def add_new_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start adding new lead - sequential flow"""
    query = update.callback_query
    try:
        await query.answer()
        user_id = query.from_user.id
        
        # Clear any existing conversation state before starting new one
        # This ensures we can start a new conversation even if user is in another ConversationHandler state
        if context.user_data:
            # Keep only essential data, clear conversation states
            keys_to_remove = ['current_field', 'current_state', 'add_step', 'editing_lead_id', 
                            'last_check_messages', 'add_message_ids']
            for key in keys_to_remove:
                if key in context.user_data:
                    del context.user_data[key]
        
        user_data_store[user_id] = {}
        user_data_store_access_time[user_id] = time.time()
        context.user_data['current_field'] = 'fullname'
        context.user_data['add_step'] = 0
        
        # Start with first field: Full Name
        field_label = get_field_label('fullname')
        _, _, current_step, total_steps = get_next_add_field('')
        
        # Убираем описание для первого шага
        message = f"<b>Шаг {current_step} из {total_steps}</b>\n\n📝 Введите {field_label}:"
        
        await query.edit_message_text(
            message,
            reply_markup=get_navigation_keyboard(is_optional=False, show_back=False),
            parse_mode='HTML'
        )
        # Save message ID for cleanup
        if query.message:
            await save_add_message(update, context, query.message.message_id)
        return ADD_FULLNAME
    except Exception as e:
        logger.error(f"Error in add_new_callback: {e}", exc_info=True)
        try:
            await query.answer("❌ Произошла ошибка. Попробуйте снова.")
            await query.edit_message_text(
                "❌ Произошла ошибка при запуске добавления лида.\n\n"
                "Попробуйте снова или обратитесь к администратору.",
                reply_markup=get_main_menu_keyboard()
            )
        except Exception as fallback_error:
            logger.error(f"Error in add_new_callback fallback: {fallback_error}", exc_info=True)
        return ConversationHandler.END

# Universal check function
async def check_by_field(update: Update, context: ContextTypes.DEFAULT_TYPE, field_name: str, field_label: str, current_state: int):
    """Universal function to check by any field"""
    search_value = update.message.text.strip()
    
    if not search_value:
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]])
        sent_message = await update.message.reply_text(
            f"❌ {field_label} не может быть пустым.",
            reply_markup=keyboard
        )
        await save_check_message(update, context, sent_message.message_id)
        return ConversationHandler.END
    
    # Map internal field names to database column names
    FIELD_NAME_MAPPING = {
        'telegram_name': 'telegram_user',  # Map telegram_name to telegram_user for database
    }
    
    # Get database column name
    db_field_name = FIELD_NAME_MAPPING.get(field_name, field_name)
    
    # Validate minimum length for search
    if field_name == "phone":
        normalized = normalize_phone(search_value)
        if len(normalized) < 7:
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]])
            sent_message = await update.message.reply_text(
                "❌ Для поиска по телефону необходимо минимум 7 цифр.",
                reply_markup=keyboard
            )
            await save_check_message(update, context, sent_message.message_id)
            return ConversationHandler.END
        search_value = normalized
    
    # Normalize Facebook link if checking by facebook_link
    elif field_name == "facebook_link":
        # Use validate_facebook_link to normalize the link (same logic as when adding)
        is_valid, error_msg, normalized = validate_facebook_link(search_value)
        if not is_valid:
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]])
            sent_message = await update.message.reply_text(
                f"❌ {error_msg}",
                reply_markup=keyboard,
                parse_mode='HTML'
            )
            await save_check_message(update, context, sent_message.message_id)
            return ConversationHandler.END
        search_value = normalized
    
    # Normalize Telegram Name if checking by telegram_user
    elif field_name == "telegram_user":
        # Use same normalization as when adding (remove @, spaces)
        is_valid, error_msg, normalized = validate_telegram_name(search_value)
        if not is_valid:
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]])
            sent_message = await update.message.reply_text(
                f"❌ {error_msg}",
                reply_markup=keyboard
            )
            await save_check_message(update, context, sent_message.message_id)
            return ConversationHandler.END
        search_value = normalized
    
    # Get Supabase client (for all fields, not just phone)
    client = get_supabase_client()
    if not client:
        error_msg = get_user_friendly_error(Exception("Database connection failed"), "подключении к базе данных")
        await update.message.reply_text(
            error_msg,
            reply_markup=get_main_menu_keyboard(),
            parse_mode='HTML'
        )
        return ConversationHandler.END
    
    try:
        # For phone: search by last 7-9 digits
        if db_field_name == "phone":
            # Extract last 7-9 digits
            if len(search_value) >= 9:
                last_digits = search_value[-9:]
            elif len(search_value) >= 7:
                last_digits = search_value[-7:]
            else:
                last_digits = search_value
            
            # For Supabase Python client, use % as wildcard (SQL standard)
            # Search by suffix: any characters before the last digits
            pattern = f"%{last_digits}"
            
            # Search by suffix using ilike (case-insensitive pattern matching)
            # Limit results to 50 for performance
            response = (
                client.table(TABLE_NAME)
                .select("*")
                .ilike(db_field_name, pattern)
                .limit(50)
                .execute()
            )
        else:
            # For other fields: exact match, limit to 50 results
            response = client.table(TABLE_NAME).select("*").eq(db_field_name, search_value).limit(50).execute()
        
        # Field labels mapping (Russian) - use database column names
        field_labels = {
            'fullname': 'Клиент',
            'phone': 'Номер телефона',
            'facebook_link': 'Facebook Ссылка',
            'telegram_user': 'Имя пользователя Telegram',  # Changed from telegram_name to telegram_user
            'telegram_id': 'Telegram ID',
            'manager_name': 'Добавил',
            'created_at': 'Дата'
        }
        
        if response.data and len(response.data) > 0:
            results = response.data
            
            # If multiple results, show all
            if len(results) > 1:
                message_parts = [f"✅ <b>Найдено клиентов: {len(results)}</b>\n"]
                
                for idx, result in enumerate(results, 1):
                    if idx > 1:
                        message_parts.append("")  # Empty line between leads
                    message_parts.append(f"<b>━━━ Клиент {idx} ━━━</b>")
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
                        
                        # Format Facebook link to full URL
                        if field_name_key == 'facebook_link':
                            value = format_facebook_link_for_display(value)
                        
                        # Format value in code tags for easy copying
                        escaped_value = escape_html(str(value))
                        message_parts.append(f"{field_label}: <code>{escaped_value}</code>")
            else:
                # Single result
                result = results[0]
                message_parts = ["✅ <b>Лид найден</b>", ""]  # Empty line after header
                
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
                    
                    # Format Facebook link to full URL
                    if field_name_key == 'facebook_link':
                        value = format_facebook_link_for_display(value)
                    
                    # Format value in code tags for easy copying
                    escaped_value = escape_html(str(value))
                    message_parts.append(f"{field_label}: <code>{escaped_value}</code>")
            
            message = "\n".join(message_parts)

            # Build inline keyboard for editing
            keyboard = []
            if len(results) == 1:
                lead_id = results[0].get('id')
                if lead_id is not None:
                    keyboard.append([InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_lead_{lead_id}")])
            else:
                for idx, result in enumerate(results, 1):
                    lead_id = result.get('id')
                    if lead_id is None:
                        continue
                    name = result.get('fullname') or "без имени"
                    label = f"✏️ Клиент {idx} ({name})"
                    if len(label) > 60:
                        label = label[:57] + "..."
                    keyboard.append([InlineKeyboardButton(label, callback_data=f"edit_lead_{lead_id}")])
            
            # Add main menu button
            keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
            reply_markup = InlineKeyboardMarkup(keyboard)
        else:
            message = "❌ <b>Клиент не найден</b>."
            reply_markup = get_main_menu_keyboard()
        
        sent_message = await update.message.reply_text(
            message,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        # Save message ID for cleanup
        await save_check_message(update, context, sent_message.message_id)
        
    except Exception as e:
        logger.error(f"Error checking by {field_name}: {e}", exc_info=True)
        error_msg = get_user_friendly_error(e, "проверке")
        sent_message = await update.message.reply_text(
            error_msg,
            reply_markup=get_main_menu_keyboard(),
            parse_mode='HTML'
        )
        await save_check_message(update, context, sent_message.message_id)
    
    return ConversationHandler.END

async def check_by_fullname(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check by fullname using contains search with limit of 10 results"""
    search_value = update.message.text.strip()
    
    logger.info(f"[FULLNAME SEARCH] Starting search with value: '{search_value}' (length: {len(search_value)}, type: {type(search_value)})")
    
    if not search_value:
        await update.message.reply_text("❌ Имя не может быть пустым. Попробуйте снова:")
        return CHECK_BY_FULLNAME
    
    # Validate minimum length for fullname search
    if len(search_value) < 3:
        await update.message.reply_text("❌ Для поиска по имени необходимо минимум 3 символа. Попробуйте снова:")
        return CHECK_BY_FULLNAME
    
    # Normalize search value: remove extra spaces, trim
    search_value = re.sub(r'\s+', ' ', search_value).strip()
    
    # Escape special characters for LIKE/ILIKE pattern matching
    # In SQL LIKE/ILIKE, % and _ are special characters that need to be escaped
    # Escape % and _ by replacing them with \% and \_
    escaped_search_value = search_value.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
    
    logger.info(f"[FULLNAME SEARCH] Normalized search value: '{search_value}' -> escaped: '{escaped_search_value}'")
    
    # Get Supabase client
    client = get_supabase_client()
    if not client:
        logger.error("[FULLNAME SEARCH] Failed to get Supabase client")
        error_msg = get_user_friendly_error(Exception("Database connection failed"), "подключении к базе данных")
        await update.message.reply_text(
            error_msg,
            reply_markup=get_main_menu_keyboard(),
            parse_mode='HTML'
        )
        return ConversationHandler.END
    
    try:
        # Search using ilike with contains pattern (case-insensitive)
        # Limit to 10 results at DB level for better performance
        # Sort by created_at descending (newest first)
        # For ilike in Supabase Python client, use % as wildcard (SQL standard)
        # Pattern: %escaped_value% - finds records where fullname contains the search value
        # This works for both full matches and partial matches
        pattern = f"%{escaped_search_value}%"
        logger.info(f"[FULLNAME SEARCH] Using pattern: '{pattern}' for field 'fullname'")
        logger.info(f"[FULLNAME SEARCH] Executing query: SELECT * FROM {TABLE_NAME} WHERE fullname ILIKE '{pattern}' ORDER BY created_at DESC LIMIT 10")
        
        response = client.table(TABLE_NAME).select("*").ilike("fullname", pattern).order("created_at", desc=True).limit(10).execute()
        
        logger.info(f"[FULLNAME SEARCH] Query executed. Response type: {type(response)}, has data: {hasattr(response, 'data')}")
        logger.info(f"[FULLNAME SEARCH] Response.data type: {type(response.data) if hasattr(response, 'data') else 'N/A'}")
        logger.info(f"[FULLNAME SEARCH] Response.data length: {len(response.data) if hasattr(response, 'data') and response.data else 0}")
        
        if hasattr(response, 'data') and response.data:
            logger.info(f"[FULLNAME SEARCH] ✅ Found {len(response.data)} results for pattern '{pattern}'")
            for idx, result in enumerate(response.data[:5], 1):  # Log first 5 results
                fullname = result.get('fullname', 'N/A')
                logger.info(f"[FULLNAME SEARCH] Result {idx}: id={result.get('id')}, fullname='{fullname}' (matches: {escaped_search_value.lower() in str(fullname).lower() if fullname else False})")
        else:
            logger.warning(f"[FULLNAME SEARCH] ❌ No results found for pattern '{pattern}' (search_value: '{search_value}', escaped: '{escaped_search_value}')")
        
        # Field labels mapping (Russian)
        field_labels = {
            'fullname': 'Клиент',
            'phone': 'Номер телефона',
            'facebook_link': 'Facebook Ссылка',
            'telegram_user': 'Имя пользователя Telegram',  # Changed from telegram_name to telegram_user
            'telegram_id': 'Telegram ID',
            'manager_name': 'Добавил',
            'created_at': 'Дата'
        }
        
        if response.data and len(response.data) > 0:
            results = response.data
            
            # Check if more than 10 results
            if len(results) > 10:
                await update.message.reply_text(
                    "❌ Слишком много совпадений. Попробуйте другой фильтр поиска.",
                    reply_markup=get_main_menu_keyboard()
                )
                return ConversationHandler.END
            
            # If multiple results, show all
            if len(results) > 1:
                message_parts = [f"✅ <b>Найдено клиентов: {len(results)}</b>\n"]
                
                for idx, result in enumerate(results, 1):
                    if idx > 1:
                        message_parts.append("")  # Empty line between leads
                    message_parts.append(f"<b>━━━ Клиент {idx} ━━━</b>")
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
                        
                        # Format Facebook link to full URL
                        if field_name_key == 'facebook_link':
                            value = format_facebook_link_for_display(value)
                        
                        # Format value in code tags for easy copying
                        escaped_value = escape_html(str(value))
                        message_parts.append(f"{field_label}: <code>{escaped_value}</code>")
            else:
                # Single result
                result = results[0]
                message_parts = ["✅ <b>Лид найден</b>", ""]  # Empty line after header
                
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
                    
                    # Format Facebook link to full URL
                    if field_name_key == 'facebook_link':
                        value = format_facebook_link_for_display(value)
                    
                    # Format value in code tags for easy copying
                    escaped_value = escape_html(str(value))
                    message_parts.append(f"{field_label}: <code>{escaped_value}</code>")
            
            message = "\n".join(message_parts)

            # Build inline keyboard for editing
            keyboard = []
            if len(results) == 1:
                lead_id = results[0].get('id')
                if lead_id is not None:
                    keyboard.append([InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_lead_{lead_id}")])
            else:
                for idx, result in enumerate(results, 1):
                    lead_id = result.get('id')
                    if lead_id is None:
                        continue
                    name = result.get('fullname') or "без имени"
                    label = f"✏️ Клиент {idx} ({name})"
                    if len(label) > 60:
                        label = label[:57] + "..."
                    keyboard.append([InlineKeyboardButton(label, callback_data=f"edit_lead_{lead_id}")])
            
            # Add main menu button
            keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
            reply_markup = InlineKeyboardMarkup(keyboard)
        else:
            logger.warning(f"[FULLNAME SEARCH] ❌ No results found for pattern '{pattern}' (search_value: '{search_value}', escaped: '{escaped_search_value}')")
            message = "❌ <b>Клиент не найден</b>."
            reply_markup = get_main_menu_keyboard()
        
        await update.message.reply_text(
            message,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
    except Exception as e:
        logger.error(f"[FULLNAME SEARCH] ❌ Error checking by fullname: {e}", exc_info=True)
        logger.error(f"[FULLNAME SEARCH] Search value was: '{search_value}', escaped: '{escaped_search_value if 'escaped_search_value' in locals() else 'N/A'}', pattern was: '{pattern if 'pattern' in locals() else 'N/A'}'")
        error_msg = "❌ Произошла ошибка при проверке. Попробуйте позже или обратитесь к администратору."
        await update.message.reply_text(
            error_msg,
            reply_markup=get_main_menu_keyboard()
        )
    
    return ConversationHandler.END

# Check input handlers
async def check_telegram_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await check_by_field(update, context, "telegram_user", "Имя пользователя Telegram", CHECK_BY_TELEGRAM)

async def check_fb_link_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await check_by_field(update, context, "facebook_link", "Facebook Ссылка", CHECK_BY_FB_LINK)

async def check_telegram_id_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for check by telegram ID conversation"""
    query = update.callback_query
    await query.answer()
    
    # Clean up old check messages if any
    await cleanup_check_messages(update, context)
    
    try:
        await query.edit_message_text("🆔 Введите Telegram ID для проверки:")
    except Exception as e:
        # If message can't be edited (e.g., already deleted), send new message
        logger.warning(f"Could not edit message in check_telegram_id_callback: {e}")
        await query.message.reply_text("🆔 Введите Telegram ID для проверки:")
    
    return CHECK_BY_TELEGRAM_ID

async def check_telegram_id_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await check_by_field(update, context, "telegram_id", "Telegram ID", CHECK_BY_TELEGRAM_ID)

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
    

async def check_duplicate_realtime(client, field_name: str, field_value: str) -> tuple[bool, str]:
    """Check if a field value already exists in the database (for real-time validation)"""
    if not field_value or field_value.strip() == '':
        return True, ""  # Empty values are considered unique
    
    # Map internal field names to database column names
    FIELD_NAME_MAPPING = {
        'telegram_name': 'telegram_user',  # Map telegram_name to telegram_user for database
    }
    
    try:
        # Map field name to database column name
        db_field_name = FIELD_NAME_MAPPING.get(field_name, field_name)
        response = client.table(TABLE_NAME).select("id, fullname").eq(db_field_name, field_value).limit(1).execute()
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
    if not update.message or not update.message.text:
        logger.error("add_field_input: update.message or update.message.text is None")
        return ConversationHandler.END
    
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
            "❌ Неизвестная команда. Выберите действие:",
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
            sent_message = await update.message.reply_text(
                f"❌ {error_msg}\n\n📝 Введите {field_label}:\n\n{requirements}",
                reply_markup=get_navigation_keyboard(is_optional=True, show_back=True),
                parse_mode='HTML'
            )
            await save_add_message(update, context, sent_message.message_id)
            return current_state
    
    elif field_name == 'facebook_link':
        is_valid, error_msg, extracted = validate_facebook_link(text)
        if is_valid:
            validation_passed = True
            normalized_value = extracted
        else:
            field_label = get_field_label('facebook_link')
            requirements = get_field_format_requirements('facebook_link')
            sent_message = await update.message.reply_text(
                f"❌ {error_msg}\n\n📝 Введите {field_label}:\n\n{requirements}",
                reply_markup=get_navigation_keyboard(is_optional=True, show_back=True),
                parse_mode='HTML'
            )
            await save_add_message(update, context, sent_message.message_id)
            return current_state
    
    elif field_name == 'telegram_name':
        is_valid, error_msg, normalized = validate_telegram_name(text)
        if is_valid:
            validation_passed = True
            normalized_value = normalized
        else:
            field_label = get_field_label('telegram_name')
            requirements = get_field_format_requirements('telegram_name')
            sent_message = await update.message.reply_text(
                f"❌ {error_msg}\n\n📝 Введите {field_label}:\n\n{requirements}",
                reply_markup=get_navigation_keyboard(is_optional=True, show_back=True),
                parse_mode='HTML'
            )
            await save_add_message(update, context, sent_message.message_id)
            return current_state
    
    elif field_name == 'telegram_id':
        is_valid, error_msg, normalized = validate_telegram_id(text)
        if is_valid:
            validation_passed = True
            normalized_value = normalized
        else:
            field_label = get_field_label('telegram_id')
            requirements = get_field_format_requirements('telegram_id')
            sent_message = await update.message.reply_text(
                f"❌ {error_msg}\n\n📝 Введите {field_label}:\n\n{requirements}",
                reply_markup=get_navigation_keyboard(is_optional=True, show_back=True),
                parse_mode='HTML'
            )
            await save_add_message(update, context, sent_message.message_id)
            return current_state
    
    else:
        # For other fields (fullname, manager_name), normalize text and check not empty
        if text:
            # Check length before normalization
            if len(text) > 500:
                field_label = get_field_label(field_name)
                await update.message.reply_text(
                    f"❌ {field_label} слишком длинное (максимум 500 символов).\n\n"
                    f"📝 Введите {field_label}:",
                    reply_markup=get_navigation_keyboard(is_optional=(field_name not in ['fullname', 'manager_name']), show_back=True),
                    parse_mode='HTML'
                )
                await save_add_message(update, context, update.message.message_id)
                return current_state
            
            # Normalize text fields (fullname, manager_name)
            normalized_value = normalize_text_field(text)
            if normalized_value:
                validation_passed = True
            else:
                # Text was only whitespace
                validation_passed = False
        else:
            field_label = get_field_label(field_name)
            is_optional = field_name not in ['fullname', 'manager_name']
            
            # Для обязательных полей (fullname, manager_name) не показываем требования к формату
            if field_name == 'manager_name':
                message = f"❌ Поле не может быть пустым.\n\n📝 Введите стейдж менеджера:\n\n ⚠️ Так менеджер записан в отчётности"
                use_html = False
            elif field_name == 'fullname':
                message = f"❌ Поле не может быть пустым.\n\n📝 Введите {field_label}:"
                use_html = False
            else:
                requirements = get_field_format_requirements(field_name)
                message = f"❌ Поле не может быть пустым.\n\n📝 Введите {field_label}:\n\n{requirements}"
                use_html = True
            
            sent_message = await update.message.reply_text(
                message,
                reply_markup=get_navigation_keyboard(is_optional=is_optional, show_back=(field_name != 'fullname')),
                parse_mode='HTML' if use_html else None
            )
            await save_add_message(update, context, sent_message.message_id)
            return current_state
    
    # Real-time duplicate check for critical fields
    if validation_passed and normalized_value and field_name == 'phone':
        client = get_supabase_client()
        if client:
            is_unique, existing_fullname = await check_duplicate_realtime(client, field_name, normalized_value)
            if not is_unique:
                field_label = UNIQUENESS_FIELD_LABELS.get(field_name, field_name)
                # If duplicate found, stop adding and return to main menu
                await update.message.reply_text(
                    f"⚠️ Внимание: {field_label} уже существует в базе.\n"
                    f"Существующий лид: {existing_fullname}\n\n"
                    f"Добавление нового лида отменено.",
                    reply_markup=get_main_menu_keyboard()
                )
                # Clean up user data for this user
                if user_id in user_data_store:
                    del user_data_store[user_id]
                if user_id in user_data_store_access_time:
                    del user_data_store_access_time[user_id]
                return ConversationHandler.END
    
    # Save value only if validation passed
    if validation_passed and normalized_value:
        user_data_store[user_id][field_name] = normalized_value
    
    # Move to next field
    next_field, next_state, current_step, total_steps = get_next_add_field(field_name)
    
    if next_field == 'review':
        # Show review and save option
        await show_add_review(update, context)
        return ADD_REVIEW
    else:
        # Show next field with progress indicator
        field_label = get_field_label(next_field)
        is_optional = next_field not in ['fullname', 'manager_name']
        
        # Add progress indicator
        progress_text = f"<b>Шаг {current_step} из {total_steps}</b>\n\n"
        
        # Для обязательных полей (fullname, manager_name) не показываем требования к формату
        if next_field == 'manager_name':
            message = f"{progress_text}📝 Введите стейдж менеджера:\n\n ⚠️ Так менеджер записан в отчётности"
        elif next_field == 'fullname':
            message = f"{progress_text}📝 Введите {field_label}:"
        else:
            requirements = get_field_format_requirements(next_field)
            message = f"{progress_text}📝 Введите {field_label}:\n\n{requirements}"
        
        context.user_data['current_field'] = next_field
        context.user_data['current_state'] = next_state
        
        # Работаем как с message, так и с callback_query
        # Always use HTML when progress indicator is present
        if update.callback_query:
            await update.callback_query.edit_message_text(
                message,
                reply_markup=get_navigation_keyboard(is_optional=is_optional, show_back=True),
                parse_mode='HTML'
            )
            # Save message ID for cleanup (edit_message_text doesn't return new message, use existing)
            if update.callback_query.message:
                await save_add_message(update, context, update.callback_query.message.message_id)
        elif update.message:
            sent_message = await update.message.reply_text(
                message,
                reply_markup=get_navigation_keyboard(is_optional=is_optional, show_back=True),
                parse_mode='HTML'
            )
            await save_add_message(update, context, sent_message.message_id)
        return next_state

async def show_add_review(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show review of entered data before saving"""
    user_id = update.effective_user.id
    user_data = user_data_store.get(user_id, {})
    
    message_parts = ["✅ <b>Проверьте введенные данные:</b>\n"]
    
    field_labels = {
        'fullname': 'Имя Фамилия',
        'manager_name': 'Агент',
        'phone': 'Номер телефона',
        'facebook_link': 'Facebook Ссылка',
        'telegram_name': 'Имя пользователя Telegram',
        'telegram_id': 'Telegram ID'
    }
    
    for field_name, field_label in field_labels.items():
        value = user_data.get(field_name)
        if value:
            # Format Facebook link to full URL for display
            if field_name == 'facebook_link':
                formatted_value = format_facebook_link_for_display(str(value))
                escaped_value = escape_html(formatted_value)
            else:
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
        # Save message ID for cleanup
        if update.callback_query.message:
            await save_add_message(update, context, update.callback_query.message.message_id)
    elif update.message:
        sent_message = await update.message.reply_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
        await save_add_message(update, context, sent_message.message_id)

# Field labels for uniqueness check messages (Russian)
UNIQUENESS_FIELD_LABELS = {
    'phone': 'Номер телефона',
    'fullname': 'Клиент',
    'facebook_link': 'Facebook Ссылка',
    'telegram_name': 'Имя пользователя Telegram',
    'telegram_id': 'Telegram ID'
}

def check_fields_uniqueness_batch(client, fields_to_check: dict) -> tuple[bool, str]:
    """
    Check uniqueness of multiple fields in a single query using OR conditions.
    Returns (is_unique, conflicting_field) where conflicting_field is empty if all unique.
    """
    if not fields_to_check:
        return True, ""
    
    # Map internal field names to database column names
    FIELD_NAME_MAPPING = {
        'telegram_name': 'telegram_user',  # Map telegram_name to telegram_user for database
    }
    
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
                # Map field name to database column name
                db_field_name = FIELD_NAME_MAPPING.get(field_name, field_name)
                # Check individual field with retry
                response = client.table(TABLE_NAME).select("id").eq(db_field_name, field_value).limit(1).execute()
                if response.data and len(response.data) > 0:
                    # Found duplicate - return original field name for error message
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
    next_field, next_state, current_step, total_steps = get_next_add_field(field_name)
    
    if next_field == 'review':
        # Show review and save option
        await show_add_review(update, context)
        return ADD_REVIEW
    else:
        # Show next field with progress indicator
        field_label = get_field_label(next_field)
        is_optional = next_field not in ['fullname', 'manager_name']
        
        # Add progress indicator
        progress_text = f"<b>Шаг {current_step} из {total_steps}</b>\n\n"
        
        # Для обязательных полей (fullname, manager_name) не показываем требования к формату
        if next_field == 'manager_name':
            message = f"{progress_text}📝 Введите стейдж менеджера:\n\n ⚠️ Так менеджер записан в отчётности"
        elif next_field == 'fullname':
            message = f"{progress_text}📝 Введите {field_label}:"
        else:
            requirements = get_field_format_requirements(next_field)
            message = f"{progress_text}📝 Введите {field_label}:\n\n{requirements}"
        
        context.user_data['current_field'] = next_field
        context.user_data['current_state'] = next_state
        
        await query.edit_message_text(
            message,
            reply_markup=get_navigation_keyboard(is_optional=is_optional, show_back=True),
            parse_mode='HTML'
        )
        # Save message ID for cleanup
        if query.message:
            await save_add_message(update, context, query.message.message_id)
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
        is_optional = prev_field not in ['fullname', 'manager_name']
        
        # Calculate step number for previous field
        _, _, current_step, total_steps = get_next_add_field(prev_field)
        progress_text = f"<b>Шаг {current_step} из {total_steps}</b>\n\n"
        
        # Для обязательных полей (fullname, manager_name) не показываем требования к формату
        if prev_field == 'manager_name':
            message = f"{progress_text}📝 Введите стейдж менеджера:\n\n ⚠️ Так менеджер записан в отчётности"
        elif prev_field == 'fullname':
            message = f"{progress_text}📝 Введите {field_label}:"
        else:
            requirements = get_field_format_requirements(prev_field)
            message = f"{progress_text}📝 Введите {field_label}:\n\n{requirements}"
        
        context.user_data['current_field'] = prev_field
        context.user_data['current_state'] = prev_state
        
        await query.edit_message_text(
            message,
            reply_markup=get_navigation_keyboard(is_optional=is_optional, show_back=(prev_field != 'fullname')),
            parse_mode='HTML'
        )
        # Save message ID for cleanup
        if query.message:
            await save_add_message(update, context, query.message.message_id)
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
        _, _, current_step, total_steps = get_next_add_field('')
        progress_text = f"<b>Шаг {current_step} из {total_steps}</b>\n\n"
        await query.edit_message_text(
            f"{progress_text}❌ <b>Ошибка:</b> {field_label} обязателен для заполнения!\n\n"
            f"📝 Введите {field_label}:",
            reply_markup=get_navigation_keyboard(is_optional=False, show_back=False),
            parse_mode='HTML'
        )
        context.user_data['current_field'] = 'fullname'
        context.user_data['current_state'] = ADD_FULLNAME
        return ADD_FULLNAME
    
    if not user_data.get('manager_name'):
        field_label = get_field_label('manager_name')
        _, _, current_step, total_steps = get_next_add_field('fullname')
        progress_text = f"<b>Шаг {current_step} из {total_steps}</b>\n\n"
        await query.edit_message_text(
            f"{progress_text}❌ <b>Ошибка:</b> {field_label} обязателен для заполнения!\n\n"
            f"📝 Введите стейдж менеджера:\n\n ⚠️ Так менеджер записан в отчётности",
            reply_markup=get_navigation_keyboard(is_optional=False, show_back=True),
            parse_mode='HTML'
        )
        context.user_data['current_field'] = 'manager_name'
        context.user_data['current_state'] = ADD_MANAGER_NAME
        return ADD_MANAGER_NAME
    
    # Check if at least one identifier is present
    required_fields = ['phone', 'facebook_link', 'telegram_name', 'telegram_id']
    has_identifier = any(user_data.get(field) for field in required_fields)
    
    if not has_identifier:
        await query.edit_message_text(
            "❌ <b>Ошибка:</b> Необходимо указать минимум одно из полей:\n\n"
            "• Номер телефона\n"
            "• Facebook Ссылка\n"
            "• Имя пользователя Telegram\n"
            "• Telegram ID\n\n"
            "ℹ️ Начнем с первого опционального поля:",
            reply_markup=get_main_menu_keyboard(),
            parse_mode='HTML'
        )
        return ConversationHandler.END
    
    # Get Supabase client for uniqueness check
    client = get_supabase_client()
    if not client:
        error_msg = get_user_friendly_error(Exception("Database connection failed"), "подключении к базе данных")
        await query.edit_message_text(
            error_msg,
            reply_markup=get_main_menu_keyboard(),
            parse_mode='HTML'
        )
        if user_id in user_data_store:
            del user_data_store[user_id]
        return ConversationHandler.END
    
    # Check uniqueness of fields - optimized batch check
    fields_to_check = {}
    for field_name in ['phone', 'fullname', 'facebook_link', 'telegram_name', 'telegram_id']:
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
            
            # Get review screen message ID to exclude from cleanup
            review_message_id = None
            if query.message:
                review_message_id = query.message.message_id
            
            # Clean up all add flow messages BEFORE showing error message
            # Exclude review screen message so we can edit it
            await cleanup_add_messages(update, context, exclude_message_id=review_message_id)
            
            try:
                await query.edit_message_text(
                    f"❌ <b>Ошибка:</b> {field_label} уже существует в базе.\n\n"
                    "ℹ️ Попробуйте добавить лид заново с другими данными.",
                    reply_markup=get_main_menu_keyboard(),
                    parse_mode='HTML'
                )
            except Exception as e:
                # If edit fails (message was deleted), send new message
                if "not found" in str(e) or "BadRequest" in str(type(e).__name__):
                    await query.message.reply_text(
                        f"❌ <b>Ошибка:</b> {field_label} уже существует в базе.\n\n"
                        "ℹ️ Попробуйте добавить лид заново с другими данными.",
                        reply_markup=get_main_menu_keyboard(),
                        parse_mode='HTML'
                    )
                else:
                    raise
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
            message_parts = ["✅ <b>Клиент успешно добавлен!</b>\n"]
            field_labels = {
                'fullname': 'Имя Фамилия',
                'manager_name': 'Агент',
                'phone': 'Номер телефона',
                'facebook_link': 'Facebook Ссылка',
                'telegram_name': 'Имя пользователя Telegram',
                'telegram_id': 'Telegram ID'
            }
            
            for field_name, field_label in field_labels.items():
                # Check telegram_user for display (it was mapped from telegram_name)
                display_field = 'telegram_user' if field_name == 'telegram_name' else field_name
                value = save_data.get(display_field) or user_data.get(field_name)
                if value:
                    # Format Facebook link to full URL for display
                    if field_name == 'facebook_link':
                        formatted_value = format_facebook_link_for_display(str(value))
                        escaped_value = escape_html(formatted_value)
                    else:
                        escaped_value = escape_html(str(value))
                    message_parts.append(f"{field_label}: <code>{escaped_value}</code>")
            
            # Add date
            from datetime import datetime
            message_parts.append(f"Дата добавления: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
            
            message = "\n".join(message_parts)
            
            # Get review screen message ID to exclude from cleanup
            review_message_id = None
            if query.message:
                review_message_id = query.message.message_id
            
            # Clean up all add flow messages BEFORE showing final success message
            # Exclude review screen message so we can edit it
            await cleanup_add_messages(update, context, exclude_message_id=review_message_id)
            
            try:
                await query.edit_message_text(
                    message,
                    reply_markup=get_main_menu_keyboard(),
                    parse_mode='HTML'
                )
            except Exception as e:
                # If edit fails (message was deleted), send new message
                if "not found" in str(e) or "BadRequest" in str(type(e).__name__):
                    await query.message.reply_text(
                        message,
                        reply_markup=get_main_menu_keyboard(),
                        parse_mode='HTML'
                    )
                else:
                    raise
        else:
            # Get review screen message ID to exclude from cleanup
            review_message_id = None
            if query.message:
                review_message_id = query.message.message_id
            
            # Clean up all add flow messages BEFORE showing error message
            # Exclude review screen message so we can edit it
            await cleanup_add_messages(update, context, exclude_message_id=review_message_id)
            
            try:
                await query.edit_message_text(
                    "❌ <b>Ошибка:</b> Данные не были сохранены.\n\n"
                    "ℹ️ Попробуйте снова или обратитесь к администратору.",
                    reply_markup=get_main_menu_keyboard(),
                    parse_mode='HTML'
                )
            except Exception as e:
                # If edit fails (message was deleted), send new message
                if "not found" in str(e) or "BadRequest" in str(type(e).__name__):
                    await query.message.reply_text(
                        "❌ <b>Ошибка:</b> Данные не были сохранены.\n\n"
                        "ℹ️ Попробуйте снова или обратитесь к администратору.",
                        reply_markup=get_main_menu_keyboard(),
                        parse_mode='HTML'
                    )
                else:
                    raise
    
    except Exception as e:
        logger.error(f"Error adding client: {e}", exc_info=True)
        error_msg = get_user_friendly_error(e, "сохранении данных")
        
        # Get review screen message ID to exclude from cleanup
        review_message_id = None
        if query.message:
            review_message_id = query.message.message_id
        
        # Clean up all add flow messages BEFORE showing error message
        # Exclude review screen message so we can edit it
        await cleanup_add_messages(update, context, exclude_message_id=review_message_id)
        
        try:
            await query.edit_message_text(
                error_msg,
                reply_markup=get_main_menu_keyboard(),
                parse_mode='HTML'
            )
        except Exception as edit_error:
            # If edit fails (message was deleted), send new message
            if "not found" in str(edit_error) or "BadRequest" in str(type(edit_error).__name__):
                await query.message.reply_text(
                    error_msg,
                    reply_markup=get_main_menu_keyboard(),
                    parse_mode='HTML'
                )
            else:
                raise
    
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
        return ConversationHandler.END
    
    try:
        response = client.table(TABLE_NAME).select("*").eq("id", lead_id).execute()
        if not response.data or len(response.data) == 0:
            await query.edit_message_text(
                "❌ Ошибка: Лид не найден.",
                reply_markup=get_main_menu_keyboard()
            )
            return ConversationHandler.END
        
        lead = response.data[0]
        user_id = query.from_user.id
        
        # Store lead data for editing
        # Map telegram_user to telegram_name for consistency
        lead_data = lead.copy()
        if 'telegram_user' in lead_data and lead_data.get('telegram_user'):
            # Map telegram_user to telegram_name for consistency in UI
            if 'telegram_name' not in lead_data or not lead_data.get('telegram_name'):
                lead_data['telegram_name'] = lead_data.get('telegram_user')
        
        # Ensure all fields are present (even if None/empty) for proper display
        # This ensures indicators work correctly
        for field in ['fullname', 'manager_name', 'phone', 'facebook_link', 'telegram_name', 'telegram_id']:
            if field not in lead_data:
                lead_data[field] = None
        
        user_data_store[user_id] = lead_data
        user_data_store_access_time[user_id] = time.time()
        context.user_data['editing_lead_id'] = lead_id
        
        # Request PIN code before allowing editing
        message = f"🔒 Для редактирования лида (ID: {lead_id}) требуется PIN-код.\n\nВведите PIN-код:"
        # Use reply_text instead of edit_message_text to ensure ConversationHandler works correctly
        await query.message.reply_text(message)
        return EDIT_PIN
        
    except Exception as e:
        logger.error(f"Error loading lead for editing: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ Произошла ошибка при загрузке лида. Попробуйте позже.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END

async def edit_pin_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle PIN code input for editing"""
    user_id = update.effective_user.id
    
    # Check if message exists and has text
    if not update.message or not update.message.text:
        if update.message:
            await update.message.reply_text(
                "❌ Ошибка: Неверный формат сообщения. Пожалуйста, введите PIN-код текстом."
            )
        else:
            logger.error("edit_pin_input: update.message is None")
        return EDIT_PIN
    
    text = update.message.text.strip()
    lead_id = context.user_data.get('editing_lead_id')
    
    if not lead_id:
        await update.message.reply_text(
            "❌ Ошибка: ID лида не найден. Пожалуйста, начните редактирование заново.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END
    
    # PIN code is "2025"
    PIN_CODE = "2025"
    
    if text == PIN_CODE:
        # PIN is correct, show edit menu
        # Clear any old field editing state to prevent automatic transitions
        if 'current_field' in context.user_data:
            del context.user_data['current_field']
        if 'current_state' in context.user_data:
            del context.user_data['current_state']
        
        # Always reload lead data to ensure we have the latest from DB
        client = get_supabase_client()
        if client:
            try:
                response = client.table(TABLE_NAME).select("*").eq("id", lead_id).execute()
                if response.data and len(response.data) > 0:
                    lead = response.data[0]
                    lead_data = lead.copy()
                    if 'telegram_user' in lead_data and lead_data.get('telegram_user'):
                        if 'telegram_name' not in lead_data or not lead_data.get('telegram_name'):
                            lead_data['telegram_name'] = lead_data.get('telegram_user')
                    # Ensure all fields are present (even if None)
                    for field in ['fullname', 'manager_name', 'phone', 'facebook_link', 'telegram_name', 'telegram_id']:
                        if field not in lead_data:
                            lead_data[field] = None
                    
                    # Save original data for comparison (deep copy)
                    context.user_data['original_lead_data'] = lead_data.copy()
                    
                    # Initialize user_data_store with current data
                    user_data_store[user_id] = lead_data.copy()
                    user_data_store_access_time[user_id] = time.time()
                else:
                    await update.message.reply_text(
                        "❌ Ошибка: Лид не найден в базе данных.",
                        reply_markup=get_main_menu_keyboard()
                    )
                    return ConversationHandler.END
            except Exception as e:
                logger.error(f"Error reloading lead data in PIN handler: {e}", exc_info=True)
                await update.message.reply_text(
                    "❌ Ошибка: Не удалось загрузить данные лида.",
                    reply_markup=get_main_menu_keyboard()
                )
                return ConversationHandler.END
        else:
            await update.message.reply_text(
                "❌ Ошибка: Не удалось подключиться к базе данных.",
                reply_markup=get_main_menu_keyboard()
            )
            return ConversationHandler.END
        
        message = f"✏️ Редактирование лида (ID: {lead_id})\n\nВыберите поле для редактирования:"
        await update.message.reply_text(
            message,
            reply_markup=get_edit_field_keyboard(user_id, context.user_data.get('original_lead_data', {}))
        )
        return EDIT_MENU
    else:
        # PIN is incorrect, ask again
        await update.message.reply_text(
            "❌ Неверный PIN-код. Попробуйте снова.\n\nВведите PIN-код:"
        )
        return EDIT_PIN

def get_edit_field_keyboard(user_id: int, original_data: dict = None):
    """Create keyboard for editing lead fields with change indicators"""
    user_data = user_data_store.get(user_id, {})
    keyboard = []
    
    # Helper function to check if field has a value
    def has_value(field_name):
        value = user_data.get(field_name)
        return value is not None and value != '' and (not isinstance(value, str) or value.strip() != '')
    
    # Helper function to check if field was changed
    def is_changed(field_name):
        if not original_data:
            return False
        current_value = user_data.get(field_name)
        original_value = original_data.get(field_name)
        # Handle telegram_user/telegram_name mapping
        if field_name == 'telegram_name':
            original_value = original_data.get('telegram_name') or original_data.get('telegram_user')
        # Compare values (handle None and empty strings)
        if current_value is None and (original_value is None or original_value == ''):
            return False
        if current_value == '' and (original_value is None or original_value == ''):
            return False
        return str(current_value).strip() != str(original_value).strip() if original_value else current_value is not None
    
    # Helper function to get status indicator
    def get_status(field_name):
        if is_changed(field_name):
            return "🟡"  # Changed
        elif has_value(field_name):
            return "🟢"  # Filled, not changed
        else:
            return "⚪"  # Empty
    
    # Mandatory fields
    fullname_status = get_status('fullname')
    manager_status = get_status('manager_name')
    
    keyboard.append([InlineKeyboardButton(f"{fullname_status} Имя Фамилия *", callback_data="edit_field_fullname")])
    keyboard.append([InlineKeyboardButton(f"{manager_status} Агент *", callback_data="edit_field_manager")])
    
    # Identifier fields
    phone_status = get_status('phone')
    fb_link_status = get_status('facebook_link')
    telegram_name_status = get_status('telegram_name')
    telegram_id_status = get_status('telegram_id')
    
    keyboard.append([InlineKeyboardButton(f"{phone_status} Номер телефона", callback_data="edit_field_phone")])
    keyboard.append([InlineKeyboardButton(f"{fb_link_status} Facebook Ссылка", callback_data="edit_field_fb_link")])
    keyboard.append([InlineKeyboardButton(f"{telegram_name_status} Имя пользователя Telegram", callback_data="edit_field_telegram_name")])
    keyboard.append([InlineKeyboardButton(f"{telegram_id_status} Telegram ID", callback_data="edit_field_telegram_id")])
    
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
    if not update.message or not update.message.text:
        logger.error("edit_field_input: update.message or update.message.text is None")
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    
    # Check for /skip command first
    if update.message.text.strip() == "/skip":
        # User wants to skip changing this field, return to edit menu
        await update.message.reply_text(
            "✏️ Выберите поле для редактирования:",
            reply_markup=get_edit_field_keyboard(user_id, context.user_data.get('original_lead_data', {}))
        )
        # Clear current_field to prevent issues
        if 'current_field' in context.user_data:
            del context.user_data['current_field']
        if 'current_state' in context.user_data:
            del context.user_data['current_state']
        return EDIT_MENU
    
    text = update.message.text.strip()
    
    # Check length for text fields (fullname, manager_name)
    field_name = context.user_data.get('current_field')
    if field_name in ['fullname', 'manager_name'] and len(text) > 500:
        field_label = get_field_label(field_name)
        await update.message.reply_text(
            f"❌ {field_label} слишком длинное (максимум 500 символов).\n\n"
            f"Попробуйте снова:",
            reply_markup=get_edit_field_keyboard(user_id, context.user_data.get('original_lead_data', {}))
        )
        return context.user_data.get('current_state', EDIT_MENU)
    
    # Update access time BEFORE cleanup to prevent deletion
    user_data_store_access_time[user_id] = time.time()
    
    # Ensure user_data_store entry exists before cleanup
    if user_id not in user_data_store:
        # Re-initialize from database if missing
        lead_id = context.user_data.get('editing_lead_id')
        if lead_id:
            client = get_supabase_client()
            if client:
                try:
                    response = client.table(TABLE_NAME).select("*").eq("id", lead_id).execute()
                    if response.data and len(response.data) > 0:
                        lead = response.data[0]
                        lead_data = lead.copy()
                        if 'telegram_user' in lead_data and lead_data.get('telegram_user'):
                            if 'telegram_name' not in lead_data or not lead_data.get('telegram_name'):
                                lead_data['telegram_name'] = lead_data.get('telegram_user')
                        for field in ['fullname', 'manager_name', 'phone', 'facebook_link', 'telegram_name', 'telegram_id']:
                            if field not in lead_data:
                                lead_data[field] = None
                        # Save original data if not already saved
                        if 'original_lead_data' not in context.user_data:
                            context.user_data['original_lead_data'] = lead_data.copy()
                        user_data_store[user_id] = lead_data
                        user_data_store_access_time[user_id] = time.time()
                except Exception as e:
                    logger.error(f"Error reloading lead data: {e}", exc_info=True)
    
    cleanup_user_data_store()
    
    if not field_name:
        await update.message.reply_text(
            "❌ Ошибка: поле не определено.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END
    
    # Ensure user_data_store[user_id] exists
    if user_id not in user_data_store:
        user_data_store[user_id] = {}
    
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
        is_valid, error_msg, normalized = validate_telegram_id(text)
        if is_valid:
            validation_passed = True
            normalized_value = normalized
        else:
            await update.message.reply_text(f"❌ {error_msg}\n\nПопробуйте снова:")
            return context.user_data.get('current_state', EDIT_MENU)
    
    else:
        # For other fields (fullname, manager_name), normalize text and check not empty
        if text:
            # Normalize text fields (fullname, manager_name)
            normalized_value = normalize_text_field(text)
            if normalized_value:
                validation_passed = True
            else:
                # Text was only whitespace
                validation_passed = False
        else:
            await update.message.reply_text(f"❌ Поле не может быть пустым.\n\nПопробуйте снова:")
            return context.user_data.get('current_state', EDIT_MENU)
    
    # Save value only if validation passed
    if validation_passed and normalized_value:
        # Ensure user_data_store[user_id] exists before assignment
        if user_id not in user_data_store:
            user_data_store[user_id] = {}
        user_data_store[user_id][field_name] = normalized_value
        # Update access time after saving
        user_data_store_access_time[user_id] = time.time()
        
        # Show confirmation message
        field_label = get_field_label(field_name)
        await update.message.reply_text(
            f"✅ <b>{field_label}</b> успешно изменено!\n\n"
            f"Новое значение: <code>{normalized_value}</code>\n\n"
            f"Выберите следующее поле для редактирования:",
            parse_mode='HTML',
            reply_markup=get_edit_field_keyboard(user_id, context.user_data.get('original_lead_data', {}))
        )
    else:
        # Show edit menu again (validation failed or value is empty)
        await update.message.reply_text(
            "✏️ Выберите поле для редактирования:",
            reply_markup=get_edit_field_keyboard(user_id, context.user_data.get('original_lead_data', {}))
        )
    return EDIT_MENU

async def edit_save_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save edited lead"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    lead_id = context.user_data.get('editing_lead_id')
    
    if not lead_id:
        await query.edit_message_text(
            "❌ Ошибка: ID лида не найден.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END
    
    # Get Supabase client first
    client = get_supabase_client()
    if not client:
        await query.edit_message_text(
            "❌ Ошибка: Не удалось подключиться к базе данных.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END
    
    # Load current data from database for merge logic
    try:
        response = client.table(TABLE_NAME).select("*").eq("id", lead_id).execute()
        if not response.data or len(response.data) == 0:
            await query.edit_message_text(
                "❌ Ошибка: Лид не найден в базе данных.",
                reply_markup=get_main_menu_keyboard()
            )
            return ConversationHandler.END
        
        current_db_data = response.data[0].copy()
        # Map telegram_user to telegram_name for comparison
        if 'telegram_user' in current_db_data and current_db_data.get('telegram_user'):
            if 'telegram_name' not in current_db_data or not current_db_data.get('telegram_name'):
                current_db_data['telegram_name'] = current_db_data.get('telegram_user')
    except Exception as e:
        logger.error(f"Error loading current lead data in save: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ Ошибка: Не удалось загрузить данные лида из базы.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END
    
    # Get user_data (changes made by user)
    user_data = user_data_store.get(user_id, {})
    
    # Validation (same as add_save_callback)
    # Check if fullname is empty or None
    fullname_value = user_data.get('fullname')
    if not fullname_value or (isinstance(fullname_value, str) and not fullname_value.strip()):
        await query.edit_message_text(
            "❌ <b>Ошибка:</b> Имя Фамилия обязателен для заполнения!\n\n"
            "⚠️ Пожалуйста, заполните это поле перед сохранением.\n\n"
            "Выберите поле для редактирования:",
            reply_markup=get_edit_field_keyboard(user_id, context.user_data.get('original_lead_data', {})),
            parse_mode='HTML'
        )
        return EDIT_MENU
    
    # Check if manager_name is empty or None
    manager_value = user_data.get('manager_name')
    if not manager_value or (isinstance(manager_value, str) and not manager_value.strip()):
        await query.edit_message_text(
            "❌ <b>Ошибка:</b> Агент обязателен для заполнения!\n\n"
            "⚠️ Пожалуйста, заполните это поле перед сохранением.\n\n"
            "Выберите поле для редактирования:",
            reply_markup=get_edit_field_keyboard(user_id, context.user_data.get('original_lead_data', {})),
            parse_mode='HTML'
        )
        return EDIT_MENU
    
    # Check if at least one identifier is present
    required_fields = ['phone', 'facebook_link', 'telegram_name', 'telegram_id']
    # Also check telegram_user for backward compatibility
    has_identifier = any(user_data.get(field) for field in required_fields) or user_data.get('telegram_user')
    
    if not has_identifier:
        await query.edit_message_text(
            "❌ Ошибка: Необходимо указать минимум одно из полей:\n"
            "Номер телефона, Facebook Ссылка, Имя пользователя Telegram или Telegram ID!\n\n"
            "Выберите поле для редактирования:",
            reply_markup=get_edit_field_keyboard(user_id, context.user_data.get('original_lead_data', {}))
        )
        return EDIT_MENU
    
    # Merge logic: Start with current DB data, apply only changes from user_data_store
    # This ensures unchanged fields are not lost
    update_data = current_db_data.copy()
    
    # Remove fields that shouldn't be updated
    for field in ['id', 'created_at']:
        update_data.pop(field, None)
    
    # Apply only fields that were changed by the user
    # Compare with original data to determine what was actually changed
    original_data = context.user_data.get('original_lead_data', {})
    
    # Fields that can be edited
    editable_fields = ['fullname', 'manager_name', 'phone', 'facebook_link', 'telegram_name', 'telegram_id']
    
    for field in editable_fields:
        if field in user_data:
            # Get current value from user_data (what user entered/changed)
            current_value = user_data.get(field)
            # Get original value (when editing started)
            original_value = original_data.get(field)
            # Handle telegram_user/telegram_name mapping for original
            if field == 'telegram_name':
                original_value = original_data.get('telegram_name') or original_data.get('telegram_user')
            
            # Normalize values for comparison (handle None, empty strings, whitespace)
            def normalize_for_compare(val):
                if val is None:
                    return ''
                if isinstance(val, str):
                    return val.strip()
                return str(val).strip()
            
            current_normalized = normalize_for_compare(current_value)
            original_normalized = normalize_for_compare(original_value)
            
            # If value changed, update it
            if current_normalized != original_normalized:
                update_data[field] = current_value
    
    # Normalize phone if present
    if 'phone' in update_data and update_data['phone']:
        update_data['phone'] = normalize_phone(update_data['phone'])
    
    # Map telegram_name to telegram_user for database (backward compatibility)
    if 'telegram_name' in update_data:
        telegram_name_value = update_data.pop('telegram_name')
        # Only set telegram_user if telegram_name has a value
        if telegram_name_value:
            update_data['telegram_user'] = telegram_name_value
        # If telegram_name is empty, also clear telegram_user
        elif 'telegram_user' in update_data:
            update_data['telegram_user'] = None
    
    try:
        # Update lead in database
        # Remove None values to avoid clearing fields unintentionally, but keep empty strings
        # This ensures we update all fields that were in user_data
        clean_update_data = {}
        for k, v in update_data.items():
            # Keep all values except None (None means field wasn't set)
            # Empty strings are valid and should be saved
            if v is not None:
                clean_update_data[k] = v
        
        response = client.table(TABLE_NAME).update(clean_update_data).eq("id", lead_id).execute()
        
        if response.data:
            await query.edit_message_text(
                "✅ <b>Лид успешно обновлен!</b>",
                reply_markup=get_main_menu_keyboard(),
                parse_mode='HTML'
            )
        else:
            await query.edit_message_text(
                "❌ Ошибка: Данные не были обновлены. Попробуйте снова.",
                reply_markup=get_main_menu_keyboard()
            )
    
    except Exception as e:
        logger.error(f"Error updating lead: {e}", exc_info=True)
        error_msg = "❌ Произошла ошибка при обновлении данных. Попробуйте позже или обратитесь к администратору."
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
    if 'original_lead_data' in context.user_data:
        del context.user_data['original_lead_data']
    
    return ConversationHandler.END

async def edit_lead_entry_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for editing a lead - parses lead_id from callback_data"""
    query = update.callback_query
    if not query or not query.data:
        logger.error("edit_lead_entry_callback: query or query.data is None")
        return ConversationHandler.END
    
    data = query.data
    
    # Parse lead_id from callback_data (format: "edit_lead_123")
    try:
        lead_id = int(data.split("_")[-1])
        return await edit_lead_callback(update, context, lead_id)
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing edit_lead callback: {e}")
        await query.answer("❌ Ошибка: Неверный формат запроса.", show_alert=True)
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
    if 'original_lead_data' in context.user_data:
        del context.user_data['original_lead_data']
    
    await query.edit_message_text(
        "❌ Редактирование отменено.",
        reply_markup=get_main_menu_keyboard()
    )
    return ConversationHandler.END

# Edit field callbacks
async def edit_field_fullname_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await edit_field_callback(update, context, 'fullname', 'Клиент', EDIT_FULLNAME)

async def edit_field_phone_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await edit_field_callback(update, context, 'phone', 'Номер телефона', EDIT_PHONE)

async def edit_field_fb_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await edit_field_callback(update, context, 'facebook_link', 'Facebook Ссылка', EDIT_FB_LINK)

async def edit_field_telegram_name_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await edit_field_callback(update, context, 'telegram_name', 'Имя пользователя Telegram', EDIT_TELEGRAM_NAME)

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
    except Exception as e:
        logger.error(f"Error stopping Telegram app: {e}", exc_info=True)
    
    # Clear cache
    uniqueness_cache.clear()
    

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        cleanup_on_shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle incoming Telegram updates via webhook"""
    try:
        json_data = request.get_json()
        if json_data:
            
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
    except Exception as e:
        logger.error(f"Error setting webhook: {e}")

# Initialize Telegram application
telegram_app = None
telegram_event_loop = None

def initialize_telegram_app():
    """Initialize Telegram app - called on module import (needed for gunicorn)"""
    global telegram_app, telegram_event_loop
    
    
    # Validate environment variables first
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not found - Telegram app will not be initialized")
        return
    
    if not WEBHOOK_URL:
        logger.error("WEBHOOK_URL not found - Telegram app will not be initialized")
        return
    
    
    try:
        # Create Telegram app
        create_telegram_app()
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
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            telegram_event_loop = loop  # Save reference for webhook
            
            loop.run_until_complete(telegram_app.initialize())
            
            loop.run_until_complete(setup_webhook())
            
            # Start processing updates
            loop.run_until_complete(telegram_app.start())
            
            # Setup keep-alive scheduler after bot is started
            setup_keep_alive_scheduler()
            
            # Keep the loop running to process updates
            loop.run_forever()
        except Exception as e:
            logger.error(f"Error in Telegram setup thread: {e}", exc_info=True)
    
    # Start webhook setup in background
    setup_thread = threading.Thread(target=run_telegram_setup)
    setup_thread.daemon = True
    setup_thread.start()
    

def create_telegram_app():
    """Create and configure Telegram application"""
    global telegram_app
    
    # Create application
    telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Add command handlers
    telegram_app.add_handler(CommandHandler("start", start_command))
    telegram_app.add_handler(CommandHandler("q", quit_command))
    # Note: /q command has high priority and will work from any state
    
    # Conversation handlers for checking (register BEFORE button_callback to have priority)
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
    
    check_telegram_id_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(check_telegram_id_callback, pattern="^check_telegram_id$")],
        states={CHECK_BY_TELEGRAM_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, check_telegram_id_input)]},
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
            ADD_REVIEW: [
                CallbackQueryHandler(add_save_callback, pattern="^add_save$"),
                CallbackQueryHandler(add_back_callback, pattern="^add_back$"),
                CallbackQueryHandler(add_cancel_callback, pattern="^add_cancel$"),
            ],
        },
        fallbacks=[CommandHandler("q", quit_command)],
        per_message=False,
    )
    
    # Register ConversationHandlers FIRST (before button_callback) to have priority
    telegram_app.add_handler(check_telegram_conv)
    telegram_app.add_handler(check_fb_link_conv)
    telegram_app.add_handler(check_telegram_id_conv)
    telegram_app.add_handler(check_phone_conv)
    telegram_app.add_handler(check_fullname_conv)
    telegram_app.add_handler(add_conv)
    
    # Add callback query handler for menu navigation buttons and edit lead
    # Registered AFTER ConversationHandlers so they have priority
    # Note: add_new is included here as fallback, but ConversationHandler should catch it first
    telegram_app.add_handler(CallbackQueryHandler(button_callback, pattern="^(main_menu|check_menu|add_menu|add_new)$"))
    
    # Add handler for unknown commands during conversations (must be after command handlers)
    telegram_app.add_handler(MessageHandler(filters.COMMAND & ~filters.Regex("^(/start|/q)$"), unknown_command_handler))
    
    # Edit conversation handler
    edit_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(edit_lead_entry_callback, pattern="^edit_lead_\\d+$"),
            CallbackQueryHandler(edit_field_fullname_callback, pattern="^edit_field_fullname$"),
            CallbackQueryHandler(edit_field_phone_callback, pattern="^edit_field_phone$"),
            CallbackQueryHandler(edit_field_fb_link_callback, pattern="^edit_field_fb_link$"),
            CallbackQueryHandler(edit_field_telegram_name_callback, pattern="^edit_field_telegram_name$"),
            CallbackQueryHandler(edit_field_telegram_id_callback, pattern="^edit_field_telegram_id$"),
            CallbackQueryHandler(edit_field_manager_callback, pattern="^edit_field_manager$"),
            CallbackQueryHandler(edit_save_callback, pattern="^edit_save$"),
            CallbackQueryHandler(edit_cancel_callback, pattern="^edit_cancel$"),
        ],
        states={
            EDIT_PIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_pin_input)],
            EDIT_MENU: [
                CallbackQueryHandler(edit_field_fullname_callback, pattern="^edit_field_fullname$"),
                CallbackQueryHandler(edit_field_phone_callback, pattern="^edit_field_phone$"),
                CallbackQueryHandler(edit_field_fb_link_callback, pattern="^edit_field_fb_link$"),
                CallbackQueryHandler(edit_field_telegram_name_callback, pattern="^edit_field_telegram_name$"),
                CallbackQueryHandler(edit_field_telegram_id_callback, pattern="^edit_field_telegram_id$"),
                CallbackQueryHandler(edit_field_manager_callback, pattern="^edit_field_manager$"),
                CallbackQueryHandler(edit_save_callback, pattern="^edit_save$"),
                CallbackQueryHandler(edit_cancel_callback, pattern="^edit_cancel$"),
            ],
            EDIT_FULLNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
            EDIT_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
            EDIT_FB_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
            EDIT_TELEGRAM_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
            EDIT_TELEGRAM_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
            EDIT_MANAGER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_field_input)],
        },
        fallbacks=[
            CommandHandler("q", quit_command),
            CommandHandler("skip", lambda u, c: edit_field_input(u, c)),
        ],
        per_message=False,
    )
    
    telegram_app.add_handler(edit_conv)
    
    # Add global fallback for unknown callback queries (must be last, after all ConversationHandlers)
    telegram_app.add_handler(CallbackQueryHandler(unknown_callback_handler))
    
    return telegram_app

# Setup signal handlers for graceful shutdown
setup_signal_handlers()

async def single_keep_alive():
    """Keep bot alive by calling bot.get_me() - works even in Sleeping state"""
    global telegram_app
    if telegram_app is None or telegram_app.bot is None:
        logger.warning("Keep-alive: Telegram app not initialized")
        return
    
    try:
        await telegram_app.bot.get_me()
        logger.debug("Keep-alive OK: bot.get_me() successful")
    except Exception as e:
        logger.warning(f"Keep-alive failed: {e}")

def setup_keep_alive_scheduler():
    """Setup APScheduler to keep bot alive"""
    global scheduler, telegram_event_loop
    
    if telegram_event_loop is None:
        logger.warning("Keep-alive scheduler: Telegram event loop not ready, will retry later")
        return
    
    try:
        # Create scheduler with the telegram event loop
        scheduler = AsyncIOScheduler(event_loop=telegram_event_loop)
        
        # Add job to call bot.get_me() every 5 minutes
        scheduler.add_job(
            single_keep_alive,
            trigger=IntervalTrigger(minutes=5),
            id='keep_alive',
            name='Keep bot alive',
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("Keep-alive scheduler started: bot.get_me() will be called every 5 minutes")
    except Exception as e:
        logger.error(f"Failed to setup keep-alive scheduler: {e}", exc_info=True)

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
    
    # Telegram app is already initialized by initialize_telegram_app() above
    # Give it a moment to initialize
    import time
    time.sleep(2)
    
    # For production, gunicorn will be used (see Procfile)
    # This code is kept for local development
    logger.warning("For production, use: gunicorn -w 1 -b 0.0.0.0:$PORT main:app")
    app.run(host='0.0.0.0', port=PORT, debug=False)

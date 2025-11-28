import os
# Set environment variables to disable proxy before importing supabase
# This prevents httpx.Client from receiving 'proxy' argument which is not supported in newer httpx versions
os.environ.setdefault("NO_PROXY", "*")
os.environ.setdefault("HTTPX_NO_PROXY", "1")

# DEBUG: Configure logging early to capture all debug info
import logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# DEBUG: Log proxy-related environment variables
proxy_env_vars = {
    'NO_PROXY': os.environ.get('NO_PROXY'),
    'HTTPX_NO_PROXY': os.environ.get('HTTPX_NO_PROXY'),
    'HTTP_PROXY': os.environ.get('HTTP_PROXY'),
    'HTTPS_PROXY': os.environ.get('HTTPS_PROXY'),
    'http_proxy': os.environ.get('http_proxy'),
    'https_proxy': os.environ.get('https_proxy'),
}
logger.info(f"DEBUG: Proxy environment variables: {proxy_env_vars}")

# DEBUG: Try to monkeypatch httpx.Client to intercept proxy argument
try:
    import httpx
    logger.info(f"DEBUG: httpx version: {httpx.__version__}")
    
    # Store original Client.__init__
    original_httpx_client_init = httpx.Client.__init__
    
    def patched_httpx_client_init(self, *args, **kwargs):
        """Patched httpx.Client.__init__ to remove proxy argument"""
        if 'proxy' in kwargs:
            logger.warning(f"DEBUG: httpx.Client.__init__ called with proxy={kwargs['proxy']}, removing it")
            kwargs.pop('proxy')
        return original_httpx_client_init(self, *args, **kwargs)
    
    # Apply monkeypatch
    httpx.Client.__init__ = patched_httpx_client_init
    logger.info("DEBUG: httpx.Client monkeypatch applied successfully")
except Exception as e:
    logger.warning(f"DEBUG: Failed to apply httpx.Client monkeypatch: {e}")

from datetime import datetime
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes, ConversationHandler

# DEBUG: Log versions before importing supabase
try:
    import importlib.metadata
    try:
        supabase_version = importlib.metadata.version('supabase')
        logger.info(f"DEBUG: supabase version: {supabase_version}")
    except:
        logger.warning("DEBUG: Could not get supabase version")
    
    try:
        httpx_version = importlib.metadata.version('httpx')
        logger.info(f"DEBUG: httpx version (from metadata): {httpx_version}")
    except:
        logger.warning("DEBUG: Could not get httpx version from metadata")
except:
    logger.warning("DEBUG: Could not import importlib.metadata")

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

def get_supabase_client():
    """Initialize and return Supabase client"""
    global supabase
    if supabase is None:
        try:
            logger.info("DEBUG: Starting Supabase client initialization...")
            
            if not SUPABASE_KEY:
                logger.error("SUPABASE_KEY not found in environment variables")
                return None
            
            if not SUPABASE_URL:
                logger.error("SUPABASE_URL not found in environment variables")
                return None
            
            logger.info(f"DEBUG: SUPABASE_URL={SUPABASE_URL[:20]}... (truncated)")
            logger.info(f"DEBUG: SUPABASE_KEY length={len(SUPABASE_KEY) if SUPABASE_KEY else 0}")
            
            # DEBUG: Check current environment variables again
            proxy_env_vars = {
                'NO_PROXY': os.environ.get('NO_PROXY'),
                'HTTPX_NO_PROXY': os.environ.get('HTTPX_NO_PROXY'),
            }
            logger.info(f"DEBUG: Proxy env vars at client creation: {proxy_env_vars}")
            
            # DEBUG: Try to inspect what create_client will do
            logger.info("DEBUG: Calling create_client()...")
            
            # Create Supabase client - environment variables are set at module level to prevent proxy issues
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            
            logger.info("DEBUG: Supabase client created successfully")
            logger.info("Supabase client initialized successfully")
        except TypeError as e:
            error_msg = str(e)
            logger.error(f"DEBUG: TypeError in get_supabase_client: {error_msg}")
            logger.error(f"DEBUG: Error type: {type(e)}")
            logger.error(f"DEBUG: Error args: {e.args}")
            if "proxy" in error_msg.lower():
                logger.error("DEBUG: Proxy-related error detected!")
                logger.error("DEBUG: Attempting to check httpx.Client signature...")
                try:
                    import inspect
                    import httpx
                    sig = inspect.signature(httpx.Client.__init__)
                    logger.error(f"DEBUG: httpx.Client.__init__ signature: {sig}")
                    logger.error(f"DEBUG: httpx.Client.__init__ parameters: {list(sig.parameters.keys())}")
                except Exception as inspect_error:
                    logger.error(f"DEBUG: Could not inspect httpx.Client: {inspect_error}")
            logger.error(f"Error initializing Supabase client: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"DEBUG: Unexpected exception type: {type(e)}")
            logger.error(f"DEBUG: Exception message: {str(e)}")
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
    """Validate phone number: minimum 7 digits, maximum 15 digits"""
    normalized = normalize_phone(phone)
    if not normalized:
        return False, "Номер телефона не может быть пустым", ""
    if len(normalized) < 7:
        return False, "Номер телефона должен содержать минимум 7 цифр", ""
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
    """Validate Facebook link and extract the part ending with id (subpage)"""
    if not link:
        return False, "Facebook ссылка не может быть пустой", ""
    
    # Remove http:// or https:// if present
    link_clean = link.strip()
    if link_clean.startswith('http://'):
        link_clean = link_clean[7:]
    elif link_clean.startswith('https://'):
        link_clean = link_clean[8:]
    
    # Remove www. if present
    if link_clean.startswith('www.'):
        link_clean = link_clean[4:]
    
    # Remove facebook.com/ or m.facebook.com/ if present
    if link_clean.startswith('facebook.com/'):
        link_clean = link_clean[13:]
    elif link_clean.startswith('m.facebook.com/'):
        link_clean = link_clean[15:]
    
    # Extract the part that ends with id (subpage)
    # Facebook links typically have format: profile/username or profile/id or just username/id
    # We want to extract the part ending with /id or just id
    # Examples:
    # https://www.facebook.com/profile.php?id=123456 -> profile.php?id=123456
    # https://www.facebook.com/username -> username
    # https://www.facebook.com/profile/username -> profile/username
    # https://www.facebook.com/people/First-Last/123456 -> people/First-Last/123456
    
    parts = link_clean.split('/')
    if len(parts) > 0:
        # Get the last part (usually the id or username)
        extracted = parts[-1]
        # If there's a part before that, include it to get the full path
        if len(parts) > 1:
            # Include the path that leads to the id
            extracted = '/'.join(parts)
        return True, "", extracted
    
    return False, "Неверный формат Facebook ссылки", ""

def validate_telegram_user(tg_user: str) -> tuple[bool, str, str]:
    """Validate Telegram username: remove @ if present, check not empty"""
    if not tg_user:
        return False, "Telegram username не может быть пустым", ""
    normalized = tg_user.strip()
    if normalized.startswith('@'):
        normalized = normalized[1:]
    if not normalized:
        return False, "Telegram username не может быть пустым", ""
    return True, "", normalized

def validate_facebook_username(username: str) -> tuple[bool, str, str]:
    """Validate Facebook username: remove @ if present"""
    if not username:
        return False, "Facebook username не может быть пустым", ""
    normalized = username.strip()
    if normalized.startswith('@'):
        normalized = normalized[1:]
    if not normalized:
        return False, "Facebook username не может быть пустым", ""
    return True, "", normalized

# Conversation states
(
    # Check states
    CHECK_BY_TELEGRAM,
    CHECK_BY_FB_LINK,
    CHECK_BY_FB_USERNAME,
    CHECK_BY_FB_ID,
    CHECK_BY_PHONE,
    CHECK_BY_FULLNAME,
    # Add states
    ADD_MENU,
    ADD_FULLNAME,
    ADD_PHONE,
    ADD_EMAIL,
    ADD_COUNTRY,
    ADD_FB_ID,
    ADD_FB_USERNAME,
    ADD_FB_LINK,
    ADD_TELEGRAM_USER,
    ADD_MANAGER_NAME
) = range(16)

# Store user data during conversation
user_data_store = {}

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
    
    keyboard.append([InlineKeyboardButton(f"{fullname_status} * Full Name", callback_data="add_field_fullname")])
    keyboard.append([InlineKeyboardButton(f"{manager_status} * Manager Name", callback_data="add_field_manager")])
    
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

# Callback query handlers
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button callbacks for menu navigation"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    logger.info(f"DEBUG: button_callback received data: {data}")
    
    if data == "main_menu":
        await query.edit_message_text(
            "👋 Главное меню\n\nВыберите действие:",
            reply_markup=get_main_menu_keyboard()
        )
    
    elif data == "check_menu":
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

# Add callback
async def add_new_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start adding new lead"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user_data_store[user_id] = {}
    
    message = (
        "➕ Добавление нового лида\n\n"
        "Обязательные поля:\n"
        "• * Full Name\n"
        "• * Manager Name\n"
        "• Минимум одно из: Phone, Facebook Link, Telegram, Facebook Username, Facebook ID\n\n"
        "Выберите поле для заполнения:"
    )
    
    await query.edit_message_text(message, reply_markup=get_add_field_keyboard(user_id))
    return ADD_MENU

# Universal check function
async def check_by_field(update: Update, context: ContextTypes.DEFAULT_TYPE, field_name: str, field_label: str, current_state: int):
    """Universal function to check by any field"""
    search_value = update.message.text.strip()
    
    if not search_value:
        await update.message.reply_text(f"❌ {field_label} не может быть пустым. Попробуйте снова:")
        return current_state
    
    # Normalize phone if checking by phone
    if field_name == "phone":
        search_value = normalize_phone(search_value)
        logger.info(f"DEBUG: Checking phone, normalized: {search_value}")
    
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
            
            logger.info(f"DEBUG: Searching phone by last digits: {last_digits}")
            # Search by suffix using ilike (case-insensitive pattern matching)
            response = client.table(TABLE_NAME).select("*").ilike(field_name, f"%{last_digits}").execute()
        else:
            # For other fields: exact match
            response = client.table(TABLE_NAME).select("*").eq(field_name, search_value).execute()
        
        # Field labels mapping (Russian)
        field_labels = {
            'fullname': 'Имя',
            'phone': 'Телефон',
            'email': 'Email',
            'country': 'Страна',
            'facebook_id': 'Facebook ID',
            'facebook_username': 'Facebook Username',
            'facebook_link': 'Facebook Link',
            'telegram_user': 'Telegram',
            'manager_name': 'Добавил',
            'created_at': 'Дата'
        }
        
        if response.data and len(response.data) > 0:
            results = response.data
            logger.info(f"DEBUG: Found {len(results)} result(s)")
            
            # If multiple results, show all
            if len(results) > 1:
                message_parts = [f"✅ Найдено клиентов: {len(results)}\n"]
                
                for idx, result in enumerate(results, 1):
                    message_parts.append(f"\n--- Клиент {idx} ---")
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
                message_parts = ["✅ Клиент найден."]
                
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
        response = client.table(TABLE_NAME).select("*").ilike("fullname", f"%{search_value}%").execute()
        
        # Field labels mapping (Russian)
        field_labels = {
            'fullname': 'Имя',
            'phone': 'Телефон',
            'email': 'Email',
            'country': 'Страна',
            'facebook_id': 'Facebook ID',
            'facebook_username': 'Facebook Username',
            'facebook_link': 'Facebook Link',
            'telegram_user': 'Telegram',
            'manager_name': 'Добавил',
            'created_at': 'Дата'
        }
        
        if response.data and len(response.data) > 0:
            results = response.data
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
                    message_parts.append(f"\n--- Клиент {idx} ---")
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
                message_parts = ["✅ Клиент найден."]
                
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
        await update.message.reply_text(
            "❌ Произошла ошибка при проверке. Попробуйте позже.",
            reply_markup=get_main_menu_keyboard()
        )
    
    return ConversationHandler.END

# Check input handlers
async def check_telegram_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await check_by_field(update, context, "telegram_user", "Telegram username", CHECK_BY_TELEGRAM)

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

# Add field handlers
async def add_field_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, field_name: str, field_label: str, next_state: int):
    """Universal callback for field selection"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    # If field is already filled, ask if user wants to change it
    if user_data_store.get(user_id, {}).get(field_name):
        await query.edit_message_text(
            f"📝 {field_label} уже заполнено: {user_data_store[user_id][field_name]}\n"
            f"Введите новое значение или отправьте /skip чтобы оставить текущее:"
        )
    else:
        await query.edit_message_text(f"📝 Введите {field_label}:")
    
    context.user_data['current_field'] = field_name
    context.user_data['current_state'] = next_state
    return next_state

async def add_field_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Universal handler for field input"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    field_name = context.user_data.get('current_field')
    
    if text.lower() == '/skip':
        # Skip this field
        pass
    elif field_name:
        # Validate and normalize based on field type
        validation_passed = False
        normalized_value = text
        
        if field_name == 'phone':
            is_valid, error_msg, normalized = validate_phone(text)
            if is_valid:
                validation_passed = True
                normalized_value = normalized
            else:
                await update.message.reply_text(f"❌ {error_msg}\n\nПопробуйте снова:")
                return context.user_data.get('current_state', ADD_MENU)
        
        elif field_name == 'email':
            is_valid, error_msg = validate_email(text)
            if is_valid:
                validation_passed = True
            else:
                await update.message.reply_text(f"❌ {error_msg}\n\nПопробуйте снова:")
                return context.user_data.get('current_state', ADD_MENU)
        
        elif field_name == 'facebook_id':
            is_valid, error_msg = validate_facebook_id(text)
            if is_valid:
                validation_passed = True
            else:
                await update.message.reply_text(f"❌ {error_msg}\n\nПопробуйте снова:")
                return context.user_data.get('current_state', ADD_MENU)
        
        elif field_name == 'facebook_link':
            is_valid, error_msg, extracted = validate_facebook_link(text)
            if is_valid:
                validation_passed = True
                normalized_value = extracted
            else:
                await update.message.reply_text(f"❌ {error_msg}\n\nПопробуйте снова:")
                return context.user_data.get('current_state', ADD_MENU)
        
        elif field_name == 'telegram_user':
            is_valid, error_msg, normalized = validate_telegram_user(text)
            if is_valid:
                validation_passed = True
                normalized_value = normalized
            else:
                await update.message.reply_text(f"❌ {error_msg}\n\nПопробуйте снова:")
                return context.user_data.get('current_state', ADD_MENU)
        
        elif field_name == 'facebook_username':
            is_valid, error_msg, normalized = validate_facebook_username(text)
            if is_valid:
                validation_passed = True
                normalized_value = normalized
            else:
                await update.message.reply_text(f"❌ {error_msg}\n\nПопробуйте снова:")
                return context.user_data.get('current_state', ADD_MENU)
        
        else:
            # For other fields (fullname, country, manager_name), just check not empty
            if text:
                validation_passed = True
            else:
                await update.message.reply_text(f"❌ Поле не может быть пустым.\n\nПопробуйте снова:")
                return context.user_data.get('current_state', ADD_MENU)
        
        # Save value only if validation passed
        if validation_passed and normalized_value:
            user_data_store[user_id][field_name] = normalized_value
    
    # Show menu again
    message = (
        "➕ Добавление нового лида\n\n"
        "Выберите поле для заполнения:"
    )
    await update.message.reply_text(message, reply_markup=get_add_field_keyboard(user_id))
    
    # Return to menu state
    return ADD_MENU

# Specific field callbacks
async def add_field_fullname_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'fullname', 'Full Name', ADD_FULLNAME)

async def add_field_phone_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'phone', 'Phone', ADD_PHONE)

async def add_field_email_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'email', 'Email', ADD_EMAIL)

async def add_field_country_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'country', 'Country', ADD_COUNTRY)

async def add_field_fb_id_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'facebook_id', 'Facebook ID', ADD_FB_ID)

async def add_field_fb_username_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'facebook_username', 'Facebook Username', ADD_FB_USERNAME)

async def add_field_fb_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'facebook_link', 'Facebook Link', ADD_FB_LINK)

async def add_field_telegram_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'telegram_user', 'Telegram', ADD_TELEGRAM_USER)

async def add_field_manager_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_field_callback(update, context, 'manager_name', 'Manager Name', ADD_MANAGER_NAME)

# Field labels for uniqueness check messages (Russian)
UNIQUENESS_FIELD_LABELS = {
    'phone': 'Номер телефона',
    'email': 'Email',
    'fullname': 'Имя',
    'facebook_id': 'Facebook ID',
    'facebook_username': 'Facebook Username',
    'facebook_link': 'Facebook Link'
}

def check_field_uniqueness(client, field_name: str, field_value: str) -> bool:
    """Check if a field value already exists in the database"""
    if not field_value or field_value.strip() == '':
        return True  # Empty values are considered unique
    
    try:
        response = client.table(TABLE_NAME).select("id").eq(field_name, field_value).execute()
        # If any records found, field is not unique
        return not (response.data and len(response.data) > 0)
    except Exception as e:
        logger.error(f"Error checking uniqueness for {field_name}: {e}", exc_info=True)
        # On error, assume not unique to prevent duplicate inserts
        return False

async def add_save_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Validate and save the lead"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user_data = user_data_store.get(user_id, {})
    
    # Validation
    if not user_data.get('fullname'):
        await query.edit_message_text(
            "❌ Ошибка: Full Name обязателен для заполнения!\n\n"
            "Выберите поле для заполнения:",
            reply_markup=get_add_field_keyboard(user_id)
        )
        return ADD_MENU
    
    if not user_data.get('manager_name'):
        await query.edit_message_text(
            "❌ Ошибка: Manager Name обязателен для заполнения!\n\n"
            "Выберите поле для заполнения:",
            reply_markup=get_add_field_keyboard(user_id)
        )
        return ADD_MENU
    
    # Check if at least one identifier is present
    required_fields = ['phone', 'facebook_link', 'telegram_user', 'facebook_username', 'facebook_id']
    has_identifier = any(user_data.get(field) for field in required_fields)
    
    if not has_identifier:
        await query.edit_message_text(
            "❌ Ошибка: Необходимо указать минимум одно из полей:\n"
            "Phone, Facebook Link, Telegram, Facebook Username или Facebook ID!\n\n"
            "Выберите поле для заполнения:",
            reply_markup=get_add_field_keyboard(user_id)
        )
        return ADD_MENU
    
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
    
    # Check uniqueness of fields
    fields_to_check = ['phone', 'email', 'fullname', 'facebook_id', 'facebook_username', 'facebook_link']
    
    for field_name in fields_to_check:
        field_value = user_data.get(field_name)
        if field_value and field_value.strip():  # Only check non-empty fields
            # Normalize phone if checking phone field
            check_value = normalize_phone(field_value) if field_name == 'phone' else field_value
            
            is_unique = check_field_uniqueness(client, field_name, check_value)
            if not is_unique:
                field_label = UNIQUENESS_FIELD_LABELS.get(field_name, field_name)
                await query.edit_message_text(
                    f"❌ {field_label} уже существует в базе.\n\n"
                    "Выберите поле для заполнения:",
                    reply_markup=get_add_field_keyboard(user_id)
                )
                return ADD_MENU
    
    # All fields are unique, proceed with saving
    try:
        # Normalize phone in user_data before saving if present
        if 'phone' in user_data and user_data['phone']:
            user_data['phone'] = normalize_phone(user_data['phone'])
        
        response = client.table(TABLE_NAME).insert(user_data).execute()
        
        if response.data:
            await query.edit_message_text(
                "✅ Клиент успешно добавлен!",
                reply_markup=get_main_menu_keyboard()
            )
            logger.info(f"Added new client: {user_data}")
        else:
            await query.edit_message_text(
                "❌ Ошибка: Данные не были сохранены. Попробуйте снова.",
                reply_markup=get_main_menu_keyboard()
            )
    
    except Exception as e:
        logger.error(f"Error adding client: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ Произошла ошибка при сохранении данных. Попробуйте позже.",
            reply_markup=get_main_menu_keyboard()
        )
    
    # Clean up
    if user_id in user_data_store:
        del user_data_store[user_id]
    
    return ConversationHandler.END

async def add_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel adding new lead"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id in user_data_store:
        del user_data_store[user_id]
    
    await query.edit_message_text(
        "❌ Добавление отменено.",
        reply_markup=get_main_menu_keyboard()
    )
    return ConversationHandler.END

# Flask routes
@app.route('/')
def index():
    """Health check endpoint"""
    return "Telegram Bot is running! 🤖", 200

@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle incoming Telegram updates via webhook"""
    try:
        json_data = request.get_json()
        if json_data:
            logger.info(f"Received webhook update: {json_data.get('update_id', 'unknown')}")
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

def create_telegram_app():
    """Create and configure Telegram application"""
    global telegram_app
    
    # Create application
    telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Add command handlers
    telegram_app.add_handler(CommandHandler("start", start_command))
    # Note: /cancel is still available as fallback in ConversationHandler's
    
    # Add callback query handler for menu navigation buttons
    telegram_app.add_handler(CallbackQueryHandler(button_callback, pattern="^(main_menu|check_menu)$"))
    
    # Conversation handlers for checking
    check_telegram_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(check_telegram_callback, pattern="^check_telegram$")],
        states={CHECK_BY_TELEGRAM: [MessageHandler(filters.TEXT & ~filters.COMMAND, check_telegram_input)]},
        fallbacks=[],
        per_message=False,
    )
    
    check_fb_link_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(check_fb_link_callback, pattern="^check_fb_link$")],
        states={CHECK_BY_FB_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, check_fb_link_input)]},
        fallbacks=[],
        per_message=False,
    )
    
    check_fb_username_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(check_fb_username_callback, pattern="^check_fb_username$")],
        states={CHECK_BY_FB_USERNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, check_fb_username_input)]},
        fallbacks=[],
        per_message=False,
    )
    
    check_fb_id_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(check_fb_id_callback, pattern="^check_fb_id$")],
        states={CHECK_BY_FB_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, check_fb_id_input)]},
        fallbacks=[],
        per_message=False,
    )
    
    check_phone_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(check_phone_callback, pattern="^check_phone$")],
        states={CHECK_BY_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, check_phone_input)]},
        fallbacks=[],
        per_message=False,
    )
    
    check_fullname_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(check_fullname_callback, pattern="^check_fullname$")],
        states={CHECK_BY_FULLNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, check_fullname_input)]},
        fallbacks=[],
        per_message=False,
    )
    
    # Conversation handler for adding
    add_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(add_new_callback, pattern="^add_new$")],
        states={
            ADD_MENU: [
                CallbackQueryHandler(add_field_fullname_callback, pattern="^add_field_fullname$"),
                CallbackQueryHandler(add_field_phone_callback, pattern="^add_field_phone$"),
                CallbackQueryHandler(add_field_fb_link_callback, pattern="^add_field_fb_link$"),
                CallbackQueryHandler(add_field_telegram_callback, pattern="^add_field_telegram$"),
                CallbackQueryHandler(add_field_fb_username_callback, pattern="^add_field_fb_username$"),
                CallbackQueryHandler(add_field_fb_id_callback, pattern="^add_field_fb_id$"),
                CallbackQueryHandler(add_field_email_callback, pattern="^add_field_email$"),
                CallbackQueryHandler(add_field_country_callback, pattern="^add_field_country$"),
                CallbackQueryHandler(add_field_manager_callback, pattern="^add_field_manager$"),
                CallbackQueryHandler(add_save_callback, pattern="^add_save$"),
                CallbackQueryHandler(add_cancel_callback, pattern="^add_cancel$"),
            ],
            ADD_FULLNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input)],
            ADD_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input)],
            ADD_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input)],
            ADD_COUNTRY: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input)],
            ADD_FB_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input)],
            ADD_FB_USERNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input)],
            ADD_FB_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input)],
            ADD_TELEGRAM_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input)],
            ADD_MANAGER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_field_input)],
        },
        fallbacks=[],
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
    
    logger.info("Telegram application initialized")
    return telegram_app

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
    # This prevents startup errors if there are dependency conflicts
    logger.info("Supabase client will be initialized on first use")
    
    # Create Telegram app
    create_telegram_app()
    
    # Initialize Telegram bot in a separate thread
    import asyncio
    import threading
    
    def run_telegram_setup():
        """Run async webhook setup and start update processing in a separate thread"""
        global telegram_event_loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        telegram_event_loop = loop  # Save reference for webhook
        loop.run_until_complete(telegram_app.initialize())
        loop.run_until_complete(setup_webhook())
        # Start processing updates
        loop.run_until_complete(telegram_app.start())
        # Keep the loop running to process updates
        loop.run_forever()
    
    # Start webhook setup in background
    setup_thread = threading.Thread(target=run_telegram_setup)
    setup_thread.daemon = True
    setup_thread.start()
    
    # Give it a moment to initialize
    import time
    time.sleep(2)
    
    # Start Flask server
    logger.info(f"Starting Flask server on port {PORT}")
    app.run(host='0.0.0.0', port=PORT, debug=False)

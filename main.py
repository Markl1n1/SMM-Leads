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
    """Normalize phone number: remove + and spaces"""
    if not phone:
        return ""
    return phone.replace("+", "").replace(" ", "").strip()

# Conversation states
(
    # Check states
    CHECK_BY_TELEGRAM,
    CHECK_BY_FB_LINK,
    CHECK_BY_FB_USERNAME,
    CHECK_BY_FB_ID,
    CHECK_BY_PHONE,
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
) = range(15)

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
    
    # Get Supabase client (for all fields, not just phone)
        client = get_supabase_client()
        if not client:
            await update.message.reply_text(
            "❌ Ошибка: Не удалось подключиться к базе данных.",
            reply_markup=get_main_menu_keyboard()
        )
        return ConversationHandler.END
    
    try:
        response = client.table(TABLE_NAME).select("*").eq(field_name, search_value).execute()
        
        if response.data and len(response.data) > 0:
            result = response.data[0]
            
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
            
            # Build message with all non-null fields (except id)
            message_parts = ["✅ Клиент найден."]
            
            for field_name, field_label in field_labels.items():
                value = result.get(field_name)
                
                # Skip if None, empty string, or 'Не указано'
                if value is None or value == '' or value == 'Не указано':
                    continue
        
                # Format date field
                if field_name == 'created_at':
                    try:
                        dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                        value = dt.strftime('%d.%m.%Y %H:%M')
                    except:
                        pass
                
                message_parts.append(f"{field_label}: {value}")
            
            message = "\n".join(message_parts)
        else:
            message = "❌ Клиент не найден."
        
            await update.message.reply_text(
            message,
            reply_markup=get_main_menu_keyboard()
            )
        
    except Exception as e:
        logger.error(f"Error checking by {field_name}: {e}", exc_info=True)
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
    context.user_data['next_state'] = next_state
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
        if field_name == 'phone':
            text = normalize_phone(text)
        if text:  # Only save non-empty values
            user_data_store[user_id][field_name] = text
    
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
    
    # Save to database
    client = get_supabase_client()
    if not client:
        await query.edit_message_text(
            "❌ Ошибка: Не удалось подключиться к базе данных.",
            reply_markup=get_main_menu_keyboard()
        )
        if user_id in user_data_store:
            del user_data_store[user_id]
        return ConversationHandler.END
    
    try:
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

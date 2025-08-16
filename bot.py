import os
import logging
import threading
import time
import traceback
import asyncio
import html
import secrets
import string
import random
import aiohttp
import re
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ApplicationBuilder
)
from telegram.error import RetryAfter, BadRequest
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
import concurrent.futures

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Global variables
bot_start_time = time.time()
BOT_VERSION = "8.0"  # Performance optimized version
temp_params = {}
DB = None  # Global async database instance
MONGO_CLIENT = None  # Global MongoDB client
SESSION = None  # Global aiohttp session

# API Configuration
AD_API = os.getenv('AD_API', '446b3a3f0039a2826f1483f22e9080963974ad3b')
WEBSITE_URL = os.getenv('WEBSITE_URL', 'upshrink.com')
YOUTUBE_TUTORIAL = "https://youtu.be/WeqpaV6VnO4?si=Y0pDondqe-nmIuht"
GITHUB_REPO = "https://github.com/yourusername/your-repo"

# Caches for performance
SUDO_CACHE = {}
TOKEN_CACHE = {}
CACHE_EXPIRY = 60  # seconds

# Flask app for health checks
app = Flask(__name__)

@app.route('/')
@app.route('/health')
@app.route('/status')
def health_check():
    return "Bot is running", 200

def run_flask():
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, threaded=True)

# Async MongoDB connection
async def init_db():
    global DB, MONGO_CLIENT
    try:
        mongo_uri = os.getenv('MONGO_URI')
        if not mongo_uri:
            logger.error("MONGO_URI environment variable not set")
            return None
            
        MONGO_CLIENT = AsyncIOMotorClient(mongo_uri, maxPoolSize=100, minPoolSize=10)
        DB = MONGO_CLIENT.get_database("telegram_bot")
        await DB.command('ping')  # Test connection
        logger.info("MongoDB connection successful")
        return DB
    except Exception as e:
        logger.error(f"MongoDB connection error: {e}")
        return None

# Create TTL index for token expiration
async def create_ttl_index():
    try:
        if DB is not None:
            await DB.tokens.create_index("expires_at", expireAfterSeconds=0)
            logger.info("Created TTL index for token expiration")
    except Exception as e:
        logger.error(f"Error creating TTL index: {e}")

# Create index for sudo users
async def create_sudo_index():
    try:
        if DB is not None:
            await DB.sudo_users.create_index("user_id", unique=True)
            logger.info("Created index for sudo_users")
    except Exception as e:
        logger.error(f"Error creating sudo index: {e}")

# Create index for cloned bots
async def create_clone_index():
    try:
        if DB is not None:
            await DB.cloned_bots.create_index("user_id")
            await DB.cloned_bots.create_index("token")
            logger.info("Created index for cloned_bots")
    except Exception as e:
        logger.error(f"Error creating clone index: {e}")

# Optimized user interaction recording
async def record_user_interaction(update: Update):
    try:
        # Check if DB is initialized (not None)
        if DB is None:
            return
            
        user = update.effective_user
        if not user:
            return
            
        # Use update with upsert
        await DB.users.update_one(
            {"user_id": user.id},
            {"$set": {
                "first_name": user.first_name,
                "last_name": user.last_name,
                "username": user.username,
                "last_interaction": datetime.utcnow()
            }},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error saving user data: {e}")

# Generate a random parameter
def generate_random_param(length=8):
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# Optimized URL shortening with connection pooling
async def get_shortened_url(deep_link):
    global SESSION
    try:
        if SESSION is None:
            SESSION = aiohttp.ClientSession()
            
        api_url = f"https://{WEBSITE_URL}/api?api={AD_API}&url={deep_link}"
        async with SESSION.get(api_url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                if data.get("status") == "success":
                    return data.get("shortenedUrl")
        return None
    except asyncio.TimeoutError:
        logger.warning("URL shortening timed out")
        return None
    except Exception as e:
        logger.error(f"URL shortening failed: {e}")
        return None

# Optimized sudo check with caching
async def is_sudo(user_id):
    # Check cache first
    cached = SUDO_CACHE.get(user_id)
    if cached and time.time() < cached['expiry']:
        return cached['result']
        
    owner_id = os.getenv('OWNER_ID')
    if owner_id and str(user_id) == owner_id:
        result = True
    else:
        result = False
        # Check if DB is initialized (not None)
        if DB is not None:
            try:
                result = await DB.sudo_users.find_one({"user_id": user_id}) is not None
            except Exception as e:
                logger.error(f"Sudo check error: {e}")
    
    # Update cache
    SUDO_CACHE[user_id] = {
        'result': result,
        'expiry': time.time() + CACHE_EXPIRY
    }
    return result

# Clone command handler
async def clone_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Check if user is sudo
    if not await is_sudo(update.effective_user.id):
        await update.message.reply_text(
            "🔒 This command is only available to sudo users!",
            parse_mode='Markdown'
        )
        return
    
    # Check if message is a reply
    if not update.message.reply_to_message or not update.message.reply_to_message.text:
        await update.message.reply_text(
            "🔍 Please reply to a message containing a bot token.\n\n"
            "Format: /clone [reply to token message]",
            parse_mode='Markdown'
        )
        return
    
    # Extract token from replied message
    token_message = update.message.reply_to_message.text
    token_match = re.search(r'(\d{8,10}:[a-zA-Z0-9_-]{35})', token_message)
    
    if not token_match:
        await update.message.reply_text(
            "❌ No valid bot token found in the replied message.\n\n"
            "A bot token should look like: 123456789:ABCdefGHIJKlmnoPQRSTUVWXYZ0123456789",
            parse_mode='Markdown'
        )
        return
    
    bot_token = token_match.group(1)
    
    # Verify the token
    try:
        # Create a temporary application to verify the token
        temp_app = ApplicationBuilder().token(bot_token).build()
        await temp_app.initialize()
        await temp_app.start()
        bot_info = await temp_app.bot.get_me()
        bot_username = bot_info.username
        
        # Save to database - check if DB is initialized (not None)
        if DB is not None:
            await DB.cloned_bots.insert_one({
                "user_id": update.effective_user.id,
                "token": bot_token,
                "bot_username": bot_username,
                "created_at": datetime.utcnow()
            })
        else:
            logger.error("Database connection failed during clone operation")
        
        # Create response
        response = (
            f"✅ Bot cloned successfully!\n\n"
            f"• Bot: @{bot_username}\n"
            f"• Token: `{bot_token[:10]}...`\n\n"
            f"Your bot has been added to the clone system and will receive updates."
        )
        
        keyboard = [[
            InlineKeyboardButton(
                "🔗 Open Bot",
                url=f"https://t.me/{bot_username}"
            ),
            InlineKeyboardButton(
                "📦 GitHub Repo",
                url=GITHUB_REPO
            )
        ]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            response,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        
        # Initialize and start the cloned bot in a new thread
        threading.Thread(target=run_cloned_bot, args=(bot_token,), daemon=True).start()
        
    except Exception as e:
        logger.error(f"Token verification failed: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ Failed to verify bot token: {str(e)}\n\n"
            "Please check that the token is correct and try again.",
            parse_mode='Markdown'
        )

# Function to run a cloned bot
def run_cloned_bot(token: str):
    """Run a cloned bot in a separate thread"""
    try:
        logger.info(f"Starting cloned bot with token: {token[:10]}...")
        
        # Create application with optimized settings
        application = ApplicationBuilder().token(token).pool_timeout(30).build()
        
        # Add handlers for the cloned bot
        application.add_handler(CommandHandler("start", start_wrapper))
        application.add_handler(CommandHandler("help", help_command_wrapper))
        application.add_handler(CommandHandler("createquiz", create_quiz_wrapper))
        application.add_handler(CommandHandler("token", token_command))
        application.add_handler(MessageHandler(filters.Document.TEXT, handle_document_wrapper))
        
        # Start polling with optimized parameters
        application.run_polling(
            poll_interval=0.1, 
            timeout=10,
            connect_timeout=10,
            read_timeout=10
        )
        logger.info(f"Cloned bot with token {token[:10]}... is now running")
    except Exception as e:
        logger.error(f"Failed to start cloned bot: {e}", exc_info=True)

# Token command
async def token_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    user = update.effective_user
    user_id = user.id
    
    # Sudo users don't need tokens
    if await is_sudo(user_id):
        await update.message.reply_text(
            "🌟 You are a sudo user! You don't need a token to use the bot.",
            parse_mode='Markdown'
        )
        return
    
    # Check if user already has valid token
    if await has_valid_token(user_id):
        await update.message.reply_text(
            "✅ Your access token is already active! Enjoy your 24-hour access.",
            parse_mode='Markdown'
        )
        return
    
    # Generate new verification param
    param = generate_random_param()
    temp_params[user_id] = param
    
    # Create deep link
    bot_username = os.getenv('BOT_USERNAME', context.bot.username)
    deep_link = f"https://t.me/{bot_username}?start={param}"
    
    # Get shortened URL
    short_url = await get_shortened_url(deep_link)
    if not short_url:
        await update.message.reply_text(
            "⚠️ Failed to generate verification link. Please try again.",
            parse_mode='Markdown'
        )
        return
    
    # Create response message
    response_text = (
        "🔑 Click the button below to verify your access token:\n\n"
        "✨ <b>What you'll get:</b>\n"
        "1. Full access for 24 hours\n"
        "2. Increased command limits\n"
        "3. All features unlocked\n\n"
        "This link is valid for 5 minutes"
    )
    
    # Create inline button
    keyboard = [[
        InlineKeyboardButton(
            "✅ Verify Token Now",
            url=short_url
        )
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        response_text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )

# Token verification helper
async def check_token_or_sudo(update: Update, context: ContextTypes.DEFAULT_TYPE, handler):
    user_id = update.effective_user.id
    if await is_sudo(user_id) or await has_valid_token(user_id):
        return await handler(update, context)
    
    await update.message.reply_text(
        "🔒 Access restricted! You need a valid token to use this feature.\n\n"
        "Use /token to get your access token.",
        parse_mode='Markdown'
    )

# Wrapper functions for token verification
async def start_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Handle token activation
    if context.args and context.args[0]:
        token = context.args[0]
        user = update.effective_user
        user_id = user.id
        
        # Check if it's a verification token
        if user_id in temp_params and temp_params[user_id] == token:
            # Store token in database - check if DB is initialized (not None)
            if DB is not None:
                await DB.tokens.update_one(
                    {"user_id": user_id},
                    {"$set": {
                        "token": token,
                        "created_at": datetime.utcnow(),
                        "expires_at": datetime.utcnow() + timedelta(hours=24)
                    }},
                    upsert=True
                )
            
            # Remove temp param and notify user
            del temp_params[user_id]
            await update.message.reply_text(
                "✅ Token activated successfully! Enjoy your 24-hour access.",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "⚠️ Invalid or expired verification token. Generate a new one with /token.",
                parse_mode='Markdown'
            )
        return
    
    # Skip token check for the start command itself
    await start(update, context)

async def help_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_token_or_sudo(update, context, help_command)

async def create_quiz_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_token_or_sudo(update, context, create_quiz)

async def stats_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_token_or_sudo(update, context, stats_command)

async def broadcast_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_token_or_sudo(update, context, broadcast_command)

async def confirm_broadcast_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_token_or_sudo(update, context, confirm_broadcast)

async def cancel_broadcast_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_token_or_sudo(update, context, cancel_broadcast)

async def handle_document_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_token_or_sudo(update, context, handle_document)

# Original command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    welcome_msg = (
        "🌟 *Welcome to Quiz Bot!* 🌟\n\n"
        "I can turn your text files into interactive 10-second quizzes!\n\n"
        "🔹 Use /createquiz - Start quiz creation\n"
        "🔹 Use /help - Show formatting guide\n"
        "🔹 Use /token - Get your access token\n"
        "🔹 Sudo users: /clone - Clone bots\n\n"
    )
    
    # Add token status for non-sudo users
    if not await is_sudo(update.effective_user.id):
        welcome_msg += (
            "🔒 You need a token to access all features\n"
            "Get your access token with /token - Valid for 24 hours\n\n"
        )
    
    welcome_msg += "Let's make learning fun!"
    
    # Create keyboard with tutorial button
    keyboard = [[
        InlineKeyboardButton(
            "🎥 Watch Tutorial",
            url=YOUTUBE_TUTORIAL
        )
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        welcome_msg, 
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    keyboard = [[
        InlineKeyboardButton(
            "🎥 Watch Tutorial",
            url=YOUTUBE_TUTORIAL
        )
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "📝 *Quiz File Format Guide:*\n\n"
        "```\n"
        "What is 2+2?\n"
        "A) 3\n"
        "B) 4\n"
        "C) 5\n"
        "D) 6\n"
        "Answer: 2\n"
        "The correct answer is 4\n\n"
        "Python is a...\n"
        "A. Snake\n"
        "B. Programming language\n"
        "C. Coffee brand\n"
        "D. Movie\n"
        "Answer: 2\n"
        "```\n\n"
        "📌 *Rules:*\n"
        "• One question per block (separated by blank lines)\n"
        "• Exactly 4 options (any prefix format accepted)\n"
        "• Answer format: 'Answer: <1-4>' (1=first option, 2=second, etc.)\n"
        "• Optional 7th line for explanation (any text)",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def create_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    await update.message.reply_text(
        "📤 *Ready to create your quiz!*\n\n"
        "Please send me a .txt file containing your questions.\n\n"
        "Need format help? Use /help",
        parse_mode='Markdown'
    )

def parse_quiz_file(content: str) -> tuple:
    """Optimized quiz parser"""
    blocks = content.split('\n\n')
    valid_questions = []
    errors = []
    
    for i, block in enumerate(blocks, 1):
        if not block.strip():
            continue
            
        lines = block.split('\n')
        # Fast validation
        if len(lines) < 6 or len(lines) > 7:
            errors.append(f"❌ Question {i}: Invalid line count ({len(lines)})")
            continue
            
        # Process lines
        question = lines[0].strip()
        options = [line.strip() for line in lines[1:5]]
        answer_line = lines[5].strip()
        
        # Answer validation
        if not answer_line.lower().startswith('answer:'):
            errors.append(f"❌ Q{i}: Missing 'Answer:' prefix")
            continue
            
        try:
            answer_num = int(answer_line.split(':', 1)[1].strip())
            if not 1 <= answer_num <= 4:
                errors.append(f"❌ Q{i}: Invalid answer number {answer_num}")
                continue
        except (ValueError, IndexError):
            errors.append(f"❌ Q{i}: Malformed answer line")
            continue
            
        explanation = lines[6].strip() if len(lines) > 6 else None
        valid_questions.append((question, options, answer_num - 1, explanation))
    
    return valid_questions, errors

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    if not update.message.document.file_name.endswith('.txt'):
        await update.message.reply_text("❌ Please send a .txt file")
        return
    
    try:
        # Download directly to memory
        file = await context.bot.get_file(update.message.document.file_id)
        content = await file.download_as_bytearray()
        content = content.decode('utf-8')
        
        # Parse and validate
        valid_questions, errors = parse_quiz_file(content)
        
        # Report errors
        if errors:
            error_msg = "\n".join(errors[:5])
            if len(errors) > 5:
                error_msg += f"\n\n...and {len(errors)-5} more errors"
            await update.message.reply_text(
                f"⚠️ Found {len(errors)} error(s):\n\n{error_msg}"
            )
        
        # Send quizzes with rate limiting
        if valid_questions:
            msg = await update.message.reply_text(
                f"✅ Sending {len(valid_questions)} quiz question(s)..."
            )
            
            sent_count = 0
            for question, options, correct_id, explanation in valid_questions:
                try:
                    poll_params = {
                        "chat_id": update.effective_chat.id,
                        "question": question,
                        "options": options,
                        "type": 'quiz',
                        "correct_option_id": correct_id,
                        "is_anonymous": False,
                        "open_period": 10
                    }
                    
                    if explanation:
                        poll_params["explanation"] = explanation
                    
                    await context.bot.send_poll(**poll_params)
                    sent_count += 1
                    
                    # Update progress every 5 questions
                    if sent_count % 5 == 0:
                        await msg.edit_text(
                            f"✅ Sent {sent_count}/{len(valid_questions)} questions..."
                        )
                    
                    # Rate limit: 20 messages per second (Telegram limit)
                    await asyncio.sleep(0.05)
                    
                except RetryAfter as e:
                    # Handle flood control
                    wait_time = e.retry_after + 1
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds")
                    await asyncio.sleep(wait_time)
                    continue
                except Exception as e:
                    logger.error(f"Poll creation error: {str(e)}")
            
            await msg.edit_text(
                f"✅ Successfully sent {sent_count} quiz questions!"
            )
        else:
            await update.message.reply_text("❌ No valid questions found in file")
            
    except Exception as e:
        logger.error(f"File processing error: {str(e)}")
        await update.message.reply_text("⚠️ Error processing file. Please try again.")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Check if user is owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return

    # Check if DB is initialized (not None)
    if DB is None:
        await update.message.reply_text("⚠️ Database connection error. Stats unavailable.")
        return
        
    try:
        # Calculate stats concurrently
        tasks = [
            DB.users.count_documents({}),
            DB.tokens.count_documents({}),
            DB.sudo_users.count_documents({}),
            DB.cloned_bots.count_documents({})
        ]
        total_users, active_tokens, sudo_count, clone_count = await asyncio.gather(*tasks)
        
        # Ping calculation
        start_time = time.time()
        ping_msg = await update.message.reply_text("🏓 Pong!")
        ping_time = (time.time() - start_time) * 1000
        
        # Uptime calculation
        uptime_seconds = int(time.time() - bot_start_time)
        uptime = str(timedelta(seconds=uptime_seconds))
        
        # Format stats message
        stats_message = (
            f"📊 *Bot Statistics*\n\n"
            f"• Total Users: `{total_users}`\n"
            f"• Active Tokens: `{active_tokens}`\n"
            f"• Sudo Users: `{sudo_count}`\n"
            f"• Cloned Bots: `{clone_count}`\n"
            f"• Current Ping: `{ping_time:.2f} ms`\n"
            f"• Uptime: `{uptime}`\n"
            f"• Version: `{BOT_VERSION}`\n\n"
            f"_Updated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}_"
        )
        
        # Edit the ping message with full stats
        await ping_msg.edit_text(stats_message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Stats command error: {e}")
        await update.message.reply_text("⚠️ Error retrieving statistics. Please try again later.")

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Check if user is owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return
        
    # Check if message is a reply
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "📢 <b>Usage Instructions:</b>\n\n"
            "1. Reply to any message with /broadcast\n"
            "2. Confirm with /confirm_broadcast\n\n"
            "Supports: text, photos, videos, documents, stickers, audio",
            parse_mode='HTML'
        )
        return
        
    # Get the replied message
    replied_msg = update.message.reply_to_message
        
    # Check if DB is initialized (not None)
    if DB is None:
        await update.message.reply_text("⚠️ Database connection error. Broadcast unavailable.")
        return
        
    try:
        # Get user IDs efficiently
        user_ids = []
        async for user in DB.users.find({}, {"user_id": 1}):
            user_ids.append(user["user_id"])
        
        total_users = len(user_ids)
        
        if not user_ids:
            await update.message.reply_text("⚠️ No users found in database.")
            return
            
        # Create preview message
        preview_html = "📢 <b>Broadcast Preview</b>\n\n"
        preview_html += f"• Recipients: {total_users} users\n\n"
        
        if replied_msg.text:
            safe_content = html.escape(replied_msg.text)
            display_text = safe_content[:300] + ("..." if len(safe_content) > 300 else "")
            preview_html += f"Content:\n<pre>{display_text}</pre>"
        elif replied_msg.caption:
            safe_caption = html.escape(replied_msg.caption)
            caption_snippet = safe_caption[:100] + ("..." if len(safe_caption) > 100 else "")
            preview_html += f"Caption:\n<pre>{caption_snippet}</pre>"
        else:
            media_type = "media"
            if replied_msg.photo: media_type = "photo"
            elif replied_msg.video: media_type = "video"
            elif replied_msg.document: media_type = "document"
            elif replied_msg.sticker: media_type = "sticker"
            elif replied_msg.audio: media_type = "audio"
            preview_html += f"✅ Ready to send {html.escape(media_type)} message"
            
        preview_html += "\n\nType /confirm_broadcast to send or /cancel to abort."
        
        # Send preview
        preview_msg = await update.message.reply_text(
            preview_html,
            parse_mode='HTML'
        )
        
        # Store broadcast data in context
        context.user_data["broadcast_data"] = {
            "message": replied_msg,
            "user_ids": user_ids,
            "preview_msg_id": preview_msg.message_id
        }
        
    except Exception as e:
        logger.error(f"Broadcast preparation error: {e}")
        await update.message.reply_text("⚠️ Error preparing broadcast. Please try again later.")

async def send_broadcast_message(user_id, message):
    """Send broadcast message with better error handling"""
    try:
        await message.copy(chat_id=user_id)
        return True, None
    except RetryAfter as e:
        wait_time = e.retry_after + 0.5
        logger.warning(f"Rate limited for {user_id}: Waiting {wait_time} seconds")
        await asyncio.sleep(wait_time)
        return await send_broadcast_message(user_id, message)
    except (BadRequest, Exception) as e:
        return False, f"{user_id}: {type(e).__name__}"

async def confirm_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Check if user is owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        return
        
    broadcast_data = context.user_data.get("broadcast_data")
    if not broadcast_data:
        await update.message.reply_text("⚠️ No pending broadcast. Start with /broadcast.")
        return
        
    try:
        user_ids = broadcast_data["user_ids"]
        message_to_broadcast = broadcast_data["message"]
        total_users = len(user_ids)
        
        status_msg = await update.message.reply_text(
            f"📤 Broadcasting to {total_users} users...\n\n"
            f"0/{total_users} (0%)\n"
            f"✅ Success: 0 | ❌ Failed: 0"
        )
        
        success = 0
        failed = 0
        failed_details = []
        
        # Use semaphore to limit concurrency
        semaphore = asyncio.Semaphore(5)  # 5 concurrent sends
        
        async def send_to_user(user_id):
            nonlocal success, failed
            async with semaphore:
                result, error = await send_broadcast_message(user_id, message_to_broadcast)
                if result:
                    success += 1
                else:
                    failed += 1
                    if error and len(failed_details) < 20:
                        failed_details.append(error)
                return user_id
        
        # Batch processing
        batch_size = 50
        for i in range(0, total_users, batch_size):
            batch = user_ids[i:i+batch_size]
            await asyncio.gather(*(send_to_user(uid) for uid in batch))
            
            # Update progress
            percent = min((i + len(batch)) * 100 // total_users, 100)
            await status_msg.edit_text(
                f"📤 Broadcasting to {total_users} users...\n\n"
                f"{i+len(batch)}/{total_users} ({percent}%)\n"
                f"✅ Success: {success} | ❌ Failed: {failed}"
            )
            
            # Short pause between batches
            await asyncio.sleep(0.5)
        
        # Prepare final report
        report_text = (
            f"✅ Broadcast Complete!\n\n"
            f"• Recipients: {total_users}\n"
            f"• Success: {success}\n"
            f"• Failed: {failed}"
        )
        
        # Add error details if any failures
        if failed > 0:
            report_text += f"\n\n📛 Failed Users (Sample):\n"
            report_text += "\n".join(failed_details[:5])
            if failed > 5:
                report_text += f"\n\n...and {failed - 5} more failures"
        
        # Update final status
        await status_msg.edit_text(report_text)
        
        # Cleanup
        del context.user_data["broadcast_data"]
        
    except Exception as e:
        logger.error(f"Broadcast error: {e}")
        await update.message.reply_text(f"⚠️ Critical broadcast error: {str(e)}")

async def cancel_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Check if user is owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        return
        
    if "broadcast_data" in context.user_data:
        del context.user_data["broadcast_data"]
        await update.message.reply_text("✅ Broadcast canceled.")
    else:
        await update.message.reply_text("ℹ️ No pending broadcast to cancel.")

# Sudo management commands
async def add_sudo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Verify owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return
        
    # Get target user
    target_user = None
    if context.args:
        try:
            target_user = int(context.args[0])
        except ValueError:
            pass
    elif update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user.id
    
    if not target_user:
        await update.message.reply_text(
            "ℹ️ Usage:\n"
            "Reply to user's message with /addsudo\n"
            "Or use /addsudo <user_id>"
        )
        return
        
    # Add to sudo list - check if DB is initialized (not None)
    if DB is None:
        await update.message.reply_text("⚠️ Database connection error")
        return
        
    try:
        result = await DB.sudo_users.update_one(
            {"user_id": target_user},
            {"$set": {"user_id": target_user, "added_at": datetime.utcnow()}},
            upsert=True
        )
        
        if result.upserted_id or result.modified_count:
            # Clear sudo cache for this user
            if target_user in SUDO_CACHE:
                del SUDO_CACHE[target_user]
            await update.message.reply_text(f"✅ Added user {target_user} to sudo list!")
        else:
            await update.message.reply_text("⚠️ Failed to add user to sudo list")
    except Exception as e:
        logger.error(f"Add sudo error: {e}")
        await update.message.reply_text("⚠️ Database error during sudo add")

async def rem_sudo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Verify owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return
        
    # Get target user
    target_user = None
    if context.args:
        try:
            target_user = int(context.args[0])
        except ValueError:
            pass
    elif update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user.id
    
    if not target_user:
        await update.message.reply_text(
            "ℹ️ Usage:\n"
            "Reply to user's message with /remsudo\n"
            "Or use /remsudo <user_id>"
        )
        return
        
    # Remove from sudo list - check if DB is initialized (not None)
    if DB is None:
        await update.message.reply_text("⚠️ Database connection error")
        return
        
    try:
        result = await DB.sudo_users.delete_one({"user_id": target_user})
        
        if result.deleted_count:
            # Clear sudo cache for this user
            if target_user in SUDO_CACHE:
                del SUDO_CACHE[target_user]
            await update.message.reply_text(f"✅ Removed user {target_user} from sudo list!")
        else:
            await update.message.reply_text("⚠️ User not found in sudo list")
    except Exception as e:
        logger.error(f"Remove sudo error: {e}")
        await update.message.reply_text("⚠️ Database error during sudo removal")

# Optimized token validation with caching
async def has_valid_token(user_id):
    if await is_sudo(user_id):
        return True
        
    # Check cache first
    cached = TOKEN_CACHE.get(user_id)
    if cached and time.time() < cached['expiry']:
        return cached['result']
        
    result = False
    # Check if DB is initialized (not None)
    if DB is not None:
        try:
            token_data = await DB.tokens.find_one({"user_id": user_id})
            result = token_data is not None
        except Exception as e:
            logger.error(f"Token check error: {e}")
    
    # Update cache
    TOKEN_CACHE[user_id] = {
        'result': result,
        'expiry': time.time() + CACHE_EXPIRY
    }
    return result

async def main_async() -> None:
    """Async main function"""
    global DB, SESSION
    
    # Initialize database
    DB = await init_db()
    
    # Only proceed if DB initialization was successful (DB is not None)
    if DB is not None:
        await asyncio.gather(
            create_ttl_index(),
            create_sudo_index(),
            create_clone_index()
        )
    
    # Start any existing cloned bots if DB is available
    if DB is not None:
        cloned_bots = []
        async for bot in DB.cloned_bots.find({}):
            cloned_bots.append(bot['token'])
        
        for token in cloned_bots:
            threading.Thread(target=run_cloned_bot, args=(token,), daemon=True).start()
    
    # Get token from environment
    TOKEN = os.getenv('TELEGRAM_TOKEN')
    if not TOKEN:
        logger.error("No TELEGRAM_TOKEN found in environment!")
        return
    
    # Create Telegram application
    application = ApplicationBuilder().token(TOKEN).pool_timeout(30).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start_wrapper))
    application.add_handler(CommandHandler("help", help_command_wrapper))
    application.add_handler(CommandHandler("createquiz", create_quiz_wrapper))
    application.add_handler(CommandHandler("stats", stats_command_wrapper))
    application.add_handler(CommandHandler("broadcast", broadcast_command_wrapper))
    application.add_handler(CommandHandler("confirm_broadcast", confirm_broadcast_wrapper))
    application.add_handler(CommandHandler("cancel", cancel_broadcast_wrapper))
    application.add_handler(CommandHandler("token", token_command))
    application.add_handler(CommandHandler("clone", clone_command))
    application.add_handler(MessageHandler(filters.Document.TEXT, handle_document_wrapper))
    
    # Add sudo management commands
    application.add_handler(CommandHandler("addsudo", add_sudo))
    application.add_handler(CommandHandler("remsudo", rem_sudo))
    
    # Start polling
    logger.info("Starting Telegram bot in polling mode...")
    try:
        await application.initialize()
        await application.start()
        await application.updater.start_polling(
            poll_interval=0.1,
            timeout=10,
            read_timeout=10
        )
        logger.info("Bot is now running")
        
        # Keep running until interrupted
        while True:
            await asyncio.sleep(3600)
            
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.critical(f"Telegram bot failed: {e}")
    finally:
        # Cleanup
        if SESSION:
            await SESSION.close()
        if MONGO_CLIENT:
            MONGO_CLIENT.close()
        await application.stop()
        logger.info("Bot stopped gracefully")

def main() -> None:
    """Run the bot and HTTP server"""
    # Start Flask server in a daemon thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"Flask server started in separate thread")
    
    # Run async main
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        # Attempt to restart after delay
        time.sleep(10)
        main()

if __name__ == '__main__':
    main()
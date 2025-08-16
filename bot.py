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
BOT_VERSION = "8.2"  # Premium plans version
temp_params = {}
DB = None  # Global async database instance
MONGO_CLIENT = None  # Global MongoDB client
SESSION = None  # Global aiohttp session

# API Configuration
AD_API = os.getenv('AD_API', '446b3a3f0039a2826f1483f22e9080963974ad3b')
WEBSITE_URL = os.getenv('WEBSITE_URL', 'upshrink.com')
YOUTUBE_TUTORIAL = "https://youtu.be/WeqpaV6VnO4?si=Y0pDondqe-nmIuht"
GITHUB_REPO = "https://github.com/yourusername/your-repo"
PREMIUM_CONTACT = "@Mr_rahul090"  # Premium contact

# Caches for performance
SUDO_CACHE = {}
TOKEN_CACHE = {}
PREMIUM_CACHE = {}
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

# Convert UTC to IST (UTC+5:30)
def to_ist(utc_time):
    return utc_time + timedelta(hours=5, minutes=30)

# Format time in IST
def format_ist(utc_time):
    ist_time = to_ist(utc_time)
    return ist_time.strftime("%Y-%m-%d %H:%M:%S")

# Format time left
def format_time_left(expiry):
    now = datetime.utcnow()
    if expiry < now:
        return "Expired"
    
    delta = expiry - now
    days = delta.days
    seconds = delta.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    
    parts = []
    if days > 0:
        parts.append(f"{days} days")
    if hours > 0:
        parts.append(f"{hours} hours")
    if minutes > 0:
        parts.append(f"{minutes} minutes")
    
    return ", ".join(parts) if parts else "Less than 1 minute"

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

# Create index for premium users
async def create_premium_index():
    try:
        if DB is not None:
            await DB.premium_users.create_index("user_id", unique=True)
            await DB.premium_users.create_index("expiry_date")
            logger.info("Created index for premium_users")
    except Exception as e:
        logger.error(f"Error creating premium index: {e}")

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

# Premium token command
async def token_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    user = update.effective_user
    user_id = user.id
    
    # Premium and sudo users don't need tokens
    if await is_sudo(user_id) or await is_premium(user_id):
        await update.message.reply_text(
            "🌟 You are a premium user! You don't need a token to use the bot.",
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
async def check_access(update: Update, context: ContextTypes.DEFAULT_TYPE, handler):
    user_id = update.effective_user.id
    if await is_sudo(user_id) or await is_premium(user_id) or await has_valid_token(user_id):
        return await handler(update, context)
    
    await update.message.reply_text(
        "🔒 Access restricted! You need premium or a valid token to use this feature.\n\n"
        "Use /token to get your access token or contact us for premium.",
        parse_mode='Markdown'
    )

# Wrapper functions for access verification
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
    await check_access(update, context, help_command)

async def create_quiz_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_access(update, context, create_quiz)

async def stats_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_access(update, context, stats_command)

async def broadcast_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_access(update, context, broadcast_command)

async def confirm_broadcast_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_access(update, context, confirm_broadcast)

async def cancel_broadcast_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_access(update, context, cancel_broadcast)

async def handle_document_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_access(update, context, handle_document)

# Original command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    welcome_msg = (
        "🌟 *Welcome to Quiz Bot!* 🌟\n\n"
        "I can turn your text files into interactive 10-second quizzes!\n\n"
        "🔹 Use /createquiz - Start quiz creation\n"
        "🔹 Use /help - Show formatting guide\n"
        "🔹 Use /token - Get your access token\n"
        "🔹 Premium users get unlimited access!\n\n"
    )
    
    # Add token status for non-premium users
    if not (await is_sudo(update.effective_user.id) or await is_premium(update.effective_user.id)):
        welcome_msg += (
            "🔒 You need premium or a token to access all features\n"
            "Get your access token with /token - Valid for 24 hours\n\n"
        )
    
    welcome_msg += "Let's make learning fun!"
    
    # Create keyboard with tutorial and premium buttons
    keyboard = [
        [
            InlineKeyboardButton("🎥 Watch Tutorial", url=YOUTUBE_TUTORIAL),
            InlineKeyboardButton("💎 Premium Plans", callback_data="premium_plans")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        welcome_msg, 
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    keyboard = [
        [
            InlineKeyboardButton("🎥 Watch Tutorial", url=YOUTUBE_TUTORIAL),
            InlineKeyboardButton("💎 Premium Plans", callback_data="premium_plans")
        ]
    ]
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
        "• Optional 7th line for explanation (any text)\n\n"
        "💡 *Premium Benefits:*\n"
        "- Unlimited quiz creation\n"
        "- No token required\n"
        "- Priority support",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def plan_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Create premium plans message
    plans_message = (
        "💠 𝗨𝗣𝗚𝗥𝗔𝗗𝗘 𝗧𝗢 𝗣𝗥𝗘𝗠𝗜𝗨𝗠 💠\n\n"
        "🚀 𝗣𝗿𝗲𝗺𝗶𝘂𝗺 𝗙𝗲𝗮𝘁𝘂𝗿𝗲𝘀:\n"
        "🧠 𝗨𝗡𝗟𝗜𝗠𝗜𝗧𝗘𝗗 𝗤𝗨𝗜𝗭 𝗖𝗥𝗘𝗔𝗧𝗜𝗢𝗡\n\n"
        
        "🔓 𝙁𝙍𝙀𝙀 𝙋𝙇𝘼𝙉 (𝘸𝘪𝘵𝘩 𝘳𝘦𝘴𝘵𝘳𝘪𝘤𝘵𝘪𝘰𝘯𝘴)\n"
        "🕰️ 𝗘𝘅𝗽𝗶𝗿𝘆: Never\n"
        "💰 𝗣𝗿𝗶𝗰𝗲: ₹𝟬\n\n"
        
        "🕐 𝟭-𝗗𝗔𝗬 𝗣𝗟𝗔𝗡\n"
        "💰 𝗣𝗿𝗶𝗰𝗲: ₹𝟭𝟬 🇮🇳\n"
        "📅 𝗗𝘂𝗿𝗮𝘁𝗶𝗼𝗻: 1 Day\n\n"
        
        "📆 𝟭-𝗪𝗘𝗘𝗞 𝗣𝗟𝗔𝗡\n"
        "💰 𝗣𝗿𝗶𝗰𝗲: ₹𝟮𝟱 🇮🇳\n"
        "📅 𝗗𝘂𝗿𝗮𝘁𝗶𝗼𝗻: 10 Days\n\n"
        
        "🗓️ 𝗠𝗢𝗡𝗧𝗛𝗟𝗬 𝗣𝗟𝗔𝗡\n"
        "💰 𝗣𝗿𝗶𝗰𝗲: ₹𝟱𝟬 🇮🇳\n"
        "📅 𝗗𝘂𝗿𝗮𝘁𝗶𝗼𝗻: 1 Month\n\n"
        
        "🪙 𝟮-𝗠𝗢𝗡𝗧𝗛 𝗣𝗟𝗔𝗡\n"
        "💰 𝗣𝗿𝗶𝗰𝗲: ₹𝟭𝟬𝟬 🇮🇳\n"
        "📅 𝗗𝘂𝗿𝗮𝘁𝗶𝗼𝗻: 2 Months\n\n"
        
        f"📞 𝗖𝗼𝗻𝘁𝗮𝗰𝘁 𝗡𝗼𝘄 𝘁𝗼 𝗨𝗽𝗴𝗿𝗮𝗱𝗲\n👉 {PREMIUM_CONTACT}"
    )
    
    keyboard = [
        [InlineKeyboardButton("💎 Get Premium", url=f"https://t.me/{PREMIUM_CONTACT.lstrip('@')}")],
        [InlineKeyboardButton("📋 My Plan", callback_data="my_plan")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        plans_message,
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
    user = update.effective_user
    user_id = user.id
    await record_user_interaction(update)
    
    # Check if user is premium
    is_prem = await is_premium(user_id)
    
    # For token users, check daily quiz limit (20 quizzes)
    if not is_prem:
        # Get today's date
        today = datetime.utcnow().date()
        
        # Check if user has exceeded daily limit
        if DB is not None:
            user_data = await DB.users.find_one({"user_id": user_id})
            if user_data:
                last_quiz_date = user_data.get("last_quiz_date")
                quiz_count = user_data.get("quiz_count", 0)
                
                # Reset count if it's a new day
                if last_quiz_date != today:
                    quiz_count = 0
                
                # Check if user has exceeded limit
                if quiz_count >= 20:
                    await update.message.reply_text(
                        "⚠️ You've reached your daily quiz limit (20 quizzes).\n\n"
                        "Token users are limited to 20 quizzes per day.\n"
                        "Upgrade to premium for unlimited access!",
                        parse_mode='Markdown'
                    )
                    return
    
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
            
            # Update quiz count for token users
            if not is_prem and DB is not None:
                today = datetime.utcnow().date()
                await DB.users.update_one(
                    {"user_id": user_id},
                    {
                        "$set": {"last_quiz_date": today},
                        "$inc": {"quiz_count": 1}
                    },
                    upsert=True
                )
            
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
            DB.premium_users.count_documents({})
        ]
        total_users, active_tokens, sudo_count, premium_count = await asyncio.gather(*tasks)
        
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
            f"• Premium Users: `{premium_count}`\n"
            f"• Current Ping: `{ping_time:.2f} ms`\n"
            f"• Uptime: `{uptime}`\n"
            f"• Version: `{BOT_VERSION}`\n\n"
            f"_Updated at {format_ist(datetime.utcnow())} IST_"
        )
        
        # Edit the ping message with full stats
        await ping_msg.edit_text(stats_message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Stats command error: {e}")
        await update.message.reply_text("⚠️ Error retrieving statistics. Please try again later.")

# Premium management commands
async def add_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Verify owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return
        
    # Check arguments
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "ℹ️ Usage:\n"
            "/add <username/userid/reply> <duration>\n"
            "Durations: 1hr, 1day, 1month, 1year\n\n"
            "Example: /add @username 1month\n"
            "          /add 123456789 1year\n"
            "          Reply to a user and use /add 1day"
        )
        return
        
    # Get target user
    target_user = None
    target_user_id = None
    target_fullname = "Unknown"
    
    # Check if reply
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        target_user_id = target_user.id
        target_fullname = target_user.full_name
    else:
        # Check if first argument is username or user ID
        user_ref = context.args[0]
        
        # Try to parse as user ID
        try:
            target_user_id = int(user_ref)
            # Try to get user from database
            if DB is not None:
                user_data = await DB.users.find_one({"user_id": target_user_id})
                if user_data:
                    target_fullname = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
        except ValueError:
            # Not an integer, treat as username
            username = user_ref.lstrip('@')
            if DB is not None:
                user_data = await DB.users.find_one({"username": username})
                if user_data:
                    target_user_id = user_data["user_id"]
                    target_fullname = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
    
    # Get duration
    duration_str = context.args[-1].lower()
    duration_map = {
        "1hr": timedelta(hours=1),
        "1day": timedelta(days=1),
        "1month": timedelta(days=30),
        "1year": timedelta(days=365)
    }
    
    if duration_str not in duration_map:
        await update.message.reply_text("❌ Invalid duration. Use: 1hr, 1day, 1month, 1year")
        return
    
    duration = duration_map[duration_str]
    
    if target_user_id is None:
        await update.message.reply_text("❌ User not found. Please make sure the user has interacted with the bot.")
        return
    
    # Calculate dates
    now = datetime.utcnow()
    expiry_date = now + duration
    
    # Format dates for IST display
    join_date_ist = format_ist(now)
    expiry_date_ist = format_ist(expiry_date)
    
    # Add to premium collection
    if DB is not None:
        await DB.premium_users.update_one(
            {"user_id": target_user_id},
            {"$set": {
                "full_name": target_fullname,
                "start_date": now,
                "expiry_date": expiry_date,
                "added_by": update.effective_user.id,
                "plan": duration_str
            }},
            upsert=True
        )
        
        # Clear premium cache
        if target_user_id in PREMIUM_CACHE:
            del PREMIUM_CACHE[target_user_id]
        
        # Send message to premium user
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=(
                    f"👋 ʜᴇʏ {target_fullname},\n"
                    "ᴛʜᴀɴᴋ ʏᴏᴜ ꜰᴏʀ ᴘᴜʀᴄʜᴀꜱɪɴɢ ᴘʀᴇᴍɪᴜᴍ.\n"
                    "ᴇɴᴊᴏʏ !! ✨🎉\n\n"
                    f"⏰ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇꜱꜱ : {duration_str}\n"
                    f"⏳ ᴊᴏɪɴɪɴɢ ᴅᴀᴛᴇ : {join_date_ist} IST\n"
                    f"⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_date_ist} IST"
                )
            )
        except Exception as e:
            logger.error(f"Could not send premium message to user: {e}")
        
        # Send confirmation to admin
        await update.message.reply_text(
            "ᴘʀᴇᴍɪᴜᴍ ᴀᴅᴅᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ✅\n\n"
            f"👤 ᴜꜱᴇʀ : {target_fullname}\n"
            f"⚡ ᴜꜱᴇʀ ɪᴅ : `{target_user_id}`\n"
            f"⏰ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇꜱꜱ : {duration_str}\n\n"
            f"⏳ ᴊᴏɪɴɪɴɢ ᴅᴀᴛᴇ : {join_date_ist} IST\n"
            f"⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_date_ist} IST",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text("⚠️ Database error. Premium not added.")

async def remove_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Verify owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return
        
    # Get target user
    target_user_id = None
    
    # Check if reply
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        target_user_id = target_user.id
    elif context.args:
        # Try to parse as user ID
        try:
            target_user_id = int(context.args[0])
        except ValueError:
            # Treat as username
            username = context.args[0].lstrip('@')
            if DB is not None:
                user_data = await DB.users.find_one({"username": username})
                if user_data:
                    target_user_id = user_data["user_id"]
    
    if target_user_id is None:
        await update.message.reply_text("❌ Please specify a user by replying or providing user ID/username")
        return
    
    # Remove from premium collection
    if DB is not None:
        result = await DB.premium_users.delete_one({"user_id": target_user_id})
        
        if result.deleted_count > 0:
            # Clear premium cache
            if target_user_id in PREMIUM_CACHE:
                del PREMIUM_CACHE[target_user_id]
            
            await update.message.reply_text(
                f"✅ Premium access removed for user ID: `{target_user_id}`",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text("ℹ️ User not found in premium list")
    else:
        await update.message.reply_text("⚠️ Database error. Premium not removed.")

async def list_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Verify owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return
        
    if DB is None:
        await update.message.reply_text("⚠️ Database connection error.")
        return
    
    try:
        # Get all premium users
        premium_users = []
        async for user in DB.premium_users.find({}):
            premium_users.append(user)
        
        if not premium_users:
            await update.message.reply_text("ℹ️ No premium users found.")
            return
            
        response = "🌟 *Premium Users List* 🌟\n\n"
        
        for user in premium_users:
            user_id = user["user_id"]
            full_name = user.get("full_name", "Unknown")
            plan = user.get("plan", "Unknown")
            start_date = format_ist(user["start_date"])
            expiry_date = format_ist(user["expiry_date"])
            
            response += (
                f"👤 *User*: {full_name}\n"
                f"🆔 *ID*: `{user_id}`\n"
                f"📦 *Plan*: {plan}\n"
                f"⏱️ *Start*: {start_date} IST\n"
                f"⏳ *Expiry*: {expiry_date} IST\n"
                f"────────────────────\n"
            )
        
        await update.message.reply_text(
            response,
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Premium list error: {e}")
        await update.message.reply_text("⚠️ Error retrieving premium users.")

async def my_plan_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    user = update.effective_user
    user_id = user.id
    
    # Check if user is premium
    if not await is_premium(user_id):
        # Suggest premium plans
        keyboard = [
            [InlineKeyboardButton("💎 Premium Plans", callback_data="premium_plans")],
            [InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{PREMIUM_CONTACT.lstrip('@')}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "🔒 You don't have an active premium plan.\n\n"
            "Upgrade to premium for unlimited quiz creation and other benefits!",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return
    
    # Get premium details
    if DB is not None:
        premium_data = await DB.premium_users.find_one({"user_id": user_id})
        if premium_data:
            # Format dates in IST
            start_date = format_ist(premium_data["start_date"])
            expiry_date = format_ist(premium_data["expiry_date"])
            time_left = format_time_left(premium_data["expiry_date"])
            plan_name = premium_data.get("plan", "Premium")
            
            response = (
                "⚜️ ᴘʀᴇᴍɪᴜᴍ ᴜꜱᴇʀ ᴅᴀᴛᴀ :\n\n"
                f"👤 ᴜꜱᴇʀ : {premium_data.get('full_name', user.full_name)}\n"
                f"⚡ ᴜꜱᴇʀ ɪᴅ : `{user_id}`\n"
                f"⏰ ᴘʀᴇᴍɪᴜᴍ ᴘʟᴀɴ : {plan_name}\n\n"
                f"⏱️ ᴊᴏɪɴɪɴɢ ᴅᴀᴛᴇ : {start_date} IST\n"
                f"⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_date} IST\n"
                f"⏳ ᴛɪᴍᴇ ʟᴇꜰᴛ : {time_left}"
            )
            
            await update.message.reply_text(
                response,
                parse_mode='Markdown'
            )
            return
    
    # Fallback if data not found
    await update.message.reply_text(
        "⚠️ Could not retrieve your premium information. Please contact support.",
        parse_mode='Markdown'
    )

# Button handler
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    if query.data == "premium_plans":
        await plan_command(update, context)
    elif query.data == "my_plan":
        await my_plan_command(update, context)

# Optimized token validation with caching
async def has_valid_token(user_id):
    if await is_sudo(user_id) or await is_premium(user_id):
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

# Premium check with caching
async def is_premium(user_id):
    # Check cache first
    cached = PREMIUM_CACHE.get(user_id)
    if cached and time.time() < cached['expiry']:
        return cached['result']
        
    result = False
    # Check if DB is initialized (not None)
    if DB is not None:
        try:
            premium_data = await DB.premium_users.find_one({"user_id": user_id})
            if premium_data:
                # Check if premium has expired
                if premium_data["expiry_date"] > datetime.utcnow():
                    result = True
                else:
                    # Remove expired premium
                    await DB.premium_users.delete_one({"_id": premium_data["_id"]})
        except Exception as e:
            logger.error(f"Premium check error: {e}")
    
    # Update cache
    PREMIUM_CACHE[user_id] = {
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
            create_premium_index()
        )
    
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
    application.add_handler(CommandHandler("plan", plan_command))
    application.add_handler(CommandHandler("myplan", my_plan_command))
    application.add_handler(MessageHandler(filters.Document.TEXT, handle_document_wrapper))
    
    # Add premium management commands
    application.add_handler(CommandHandler("add", add_premium))
    application.add_handler(CommandHandler("rem", remove_premium))
    application.add_handler(CommandHandler("premium", list_premium))
    
    # Add button handler
    application.add_handler(CallbackQueryHandler(button_handler))
    
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
import os
import logging
import threading
import time
import socket
import traceback
import asyncio
import html
import secrets
import string
import random
import aiohttp
from flask import Flask
from http.server import BaseHTTPRequestHandler, HTTPServer
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes
)
from telegram.error import RetryAfter, BadRequest
from pymongo import MongoClient
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Global variables
bot_start_time = time.time()
BOT_VERSION = "7.3"  # Added sudo management and tutorial
temp_params = {}  # Temporary storage for verification params

# API Configuration
AD_API = os.getenv('AD_API', '446b3a3f0039a2826f1483f22e9080963974ad3b')
WEBSITE_URL = os.getenv('WEBSITE_URL', 'upshrink.com')
YOUTUBE_TUTORIAL = "https://youtu.be/WeqpaV6VnO4?si=Y0pDondqe-nmIuht"  # Added tutorial link

# Flask app for health checks
app = Flask(__name__)

@app.route('/')
@app.route('/health')
@app.route('/status')
def health_check():
    return "Bot is running", 200

def run_flask():
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port)

# MongoDB connection function
def get_db():
    try:
        mongo_uri = os.getenv('MONGO_URI')
        if not mongo_uri:
            logger.error("MONGO_URI environment variable not set")
            return None
            
        client = MongoClient(mongo_uri)
        client.admin.command('ping')  # Test connection
        logger.info("MongoDB connection successful")
        return client.telegram_bot
    except Exception as e:
        logger.error(f"MongoDB connection error: {e}")
        return None

# Create TTL index for token expiration
def create_ttl_index():
    try:
        db = get_db()
        if db is not None:
            tokens = db.tokens
            tokens.create_index("expires_at", expireAfterSeconds=0)
            logger.info("Created TTL index for token expiration")
    except Exception as e:
        logger.error(f"Error creating TTL index: {e}")

# Create index for sudo users
def create_sudo_index():
    try:
        db = get_db()
        if db is not None:
            sudo_users = db.sudo_users
            sudo_users.create_index("user_id", unique=True)
            logger.info("Created index for sudo_users")
    except Exception as e:
        logger.error(f"Error creating sudo index: {e}")

# Record user interaction
async def record_user_interaction(update: Update):
    try:
        db = get_db()
        if db is None:
            return
            
        user = update.effective_user
        if not user:
            return
            
        users = db.users
        user_data = {
            "user_id": user.id,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "username": user.username,
            "last_interaction": datetime.utcnow()
        }
        
        # Update or insert user record
        users.update_one(
            {"user_id": user.id},
            {"$set": user_data},
            upsert=True
        )
        logger.info(f"Recorded interaction for user {user.id}")
    except Exception as e:
        logger.error(f"Error saving user data: {e}")

# Generate a random parameter
def generate_random_param(length=8):
    """Generate a random parameter for verification"""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# Get shortened URL using ad_api service
async def get_shortened_url(deep_link):
    """Shorten URL using ad_api service"""
    try:
        api_url = f"https://{WEBSITE_URL}/api?api={AD_API}&url={deep_link}"
        
        # Use timeout to prevent hanging
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(api_url) as response:
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

# Check if user is sudo
def is_sudo(user_id):
    """Check if user is sudo (owner or in sudo list)"""
    owner_id = os.getenv('OWNER_ID')
    if owner_id and str(user_id) == owner_id:
        return True
        
    db = get_db()
    if db is None:
        return False
        
    sudo_users = db.sudo_users
    return sudo_users.find_one({"user_id": user_id}) is not None

# Token command
async def token_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    user = update.effective_user
    user_id = user.id
    
    # Sudo users don't need tokens
    if is_sudo(user_id):
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
    """Check if user is sudo or has valid token"""
    user_id = update.effective_user.id
    if is_sudo(user_id) or await has_valid_token(user_id):
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
            # Store token in database
            db = get_db()
            if db is not None:
                tokens = db.tokens
                tokens.update_one(
                    {"user_id": user_id},
                    {"$set": {
                        "token": token,
                        "created_at": datetime.utcnow(),
                        "expires_at": datetime.utcnow() + timedelta(hours=24)  # Changed to 24 hours
                    }},
                    upsert=True
                )
            
            # Remove temp param and notify user
            del temp_params[user_id]
            await update.message.reply_text(
                "✅ Token activated successfully! Enjoy your 24-hour access.",  # Updated message
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
    """Send welcome message and instructions"""
    welcome_msg = (
        "🌟 *Welcome to Quiz Bot!* 🌟\n\n"
        "I can turn your text files into interactive 10-second quizzes!\n\n"
        "🔹 Use /createquiz - Start quiz creation\n"
        "🔹 Use /help - Show formatting guide\n"
        "🔹 Use /token - Get your access token\n\n"
    )
    
    # Add token status for non-sudo users
    if not is_sudo(update.effective_user.id):
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
    """Show detailed formatting instructions"""
    # Create keyboard with tutorial button
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
    """Initiate quiz creation process"""
    await update.message.reply_text(
        "📤 *Ready to create your quiz!*\n\n"
        "Please send me a .txt file containing your questions.\n\n"
        "Need format help? Use /help",
        parse_mode='Markdown'
    )

def parse_quiz_file(content: str) -> tuple:
    """Parse and validate quiz content"""
    blocks = [b.strip() for b in content.split('\n\n') if b.strip()]
    valid_questions = []
    errors = []
    
    for i, block in enumerate(blocks, 1):
        lines = [line.strip() for line in block.split('\n') if line.strip()]
        
        if len(lines) not in (6, 7):
            errors.append(f"❌ Question {i}: Invalid line count ({len(lines)}), expected 6 or 7")
            continue
            
        question = lines[0]
        options = lines[1:5]
        answer_line = lines[5]
        explanation = lines[6] if len(lines) == 7 else None
        
        # Validate answer format
        answer_error = None
        if not answer_line.lower().startswith('answer:'):
            answer_error = "Missing 'Answer:' prefix"
        else:
            try:
                answer_num = int(answer_line.split(':')[1].strip())
                if not 1 <= answer_num <= 4:
                    answer_error = f"Invalid answer number {answer_num}"
            except (ValueError, IndexError):
                answer_error = "Malformed answer line"
        
        if answer_error:
            errors.append(f"❌ Q{i}: {answer_error}")
        else:
            option_texts = options
            correct_id = int(answer_line.split(':')[1].strip()) - 1
            valid_questions.append((question, option_texts, correct_id, explanation))
    
    return valid_questions, errors

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    """Process uploaded quiz file"""
    if not update.message.document.file_name.endswith('.txt'):
        await update.message.reply_text("❌ Please send a .txt file")
        return
    
    try:
        # Download file
        file = await context.bot.get_file(update.message.document.file_id)
        await file.download_to_drive('quiz.txt')
        
        with open('quiz.txt', 'r', encoding='utf-8') as f:
            content = f.read()
        
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
        
        # Send quizzes
        if valid_questions:
            await update.message.reply_text(
                f"✅ Sending {len(valid_questions)} quiz question(s)..."
            )
            
            # Send all quizzes asynchronously
            send_tasks = []
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
                    
                    # Create task but don't await immediately
                    task = context.bot.send_poll(**poll_params)
                    send_tasks.append(task)
                except Exception as e:
                    logger.error(f"Poll creation error: {str(e)}")
            
            # Send all quizzes concurrently
            await asyncio.gather(*send_tasks, return_exceptions=True)
            
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

    db = get_db()
    if db is None:
        await update.message.reply_text("⚠️ Database connection error. Stats unavailable.")
        return
        
    try:
        # Calculate stats
        users = db.users
        total_users = users.count_documents({})
        
        # Get token usage stats
        tokens = db.tokens
        active_tokens = tokens.count_documents({})
        
        # Get sudo users count
        sudo_users = db.sudo_users
        sudo_count = sudo_users.count_documents({})
        
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
        
    db = get_db()
    if db is None:
        await update.message.reply_text("⚠️ Database connection error. Broadcast unavailable.")
        return
        
    try:
        users = db.users
        user_ids = [user["user_id"] for user in users.find({}, {"user_id": 1})]
        total_users = len(user_ids)
        
        if not user_ids:
            await update.message.reply_text("⚠️ No users found in database.")
            return
            
        # Create preview message with HTML formatting
        preview_html = "📢 <b>Broadcast Preview</b>\n\n"
        preview_html += f"• Recipients: {total_users} users\n\n"
        
        if replied_msg.text:
            # Escape and truncate text
            safe_content = html.escape(replied_msg.text)
            display_text = safe_content[:300] + ("..." if len(safe_content) > 300 else "")
            preview_html += f"Content:\n<pre>{display_text}</pre>"
        elif replied_msg.caption:
            # Escape and truncate caption
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
            elif replied_msg.voice: media_type = "voice"
            preview_html += f"✅ Ready to send {html.escape(media_type)} message"
            
        preview_html += "\n\nType /confirm_broadcast to send or /cancel to abort."
        
        # Send preview with HTML parsing
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

async def send_broadcast_message(context, user_id, message):
    """Send broadcast message to a specific user with error handling"""
    try:
        # Copy message to user
        await message.copy(chat_id=user_id)
        return True, None
    except RetryAfter as e:
        # Wait for the specified time plus a small buffer
        wait_time = e.retry_after + 0.5
        logger.warning(f"Rate limited for {user_id}: Waiting {wait_time} seconds")
        await asyncio.sleep(wait_time)
        # Retry after waiting
        return await send_broadcast_message(context, user_id, message)
    except (BadRequest, Exception) as e:
        error_type = type(e).__name__
        error_details = str(e)
        logger.warning(f"Failed to send to {user_id}: {error_type} - {error_details}")
        return False, f"{user_id}: {error_type} - {error_details}"

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
        
        # Send messages with rate limiting
        for i, user_id in enumerate(user_ids):
            result, error = await send_broadcast_message(context, user_id, message_to_broadcast)
            
            if result:
                success += 1
            else:
                failed += 1
                if error and len(failed_details) < 20:
                    failed_details.append(error)
            
            # Update progress every 20 users or last user
            if (i + 1) % 20 == 0 or (i + 1) == total_users:
                percent = (i + 1) * 100 // total_users
                await status_msg.edit_text(
                    f"📤 Broadcasting to {total_users} users...\n\n"
                    f"{i+1}/{total_users} ({percent}%)\n"
                    f"✅ Success: {success} | ❌ Failed: {failed}"
                )
                # Conservative rate limiting
                await asyncio.sleep(0.1)
        
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
    """Add user to sudo list (owner only)"""
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
        
    # Add to sudo list
    db = get_db()
    if db is None:
        await update.message.reply_text("⚠️ Database connection error")
        return
        
    sudo_users = db.sudo_users
    result = sudo_users.update_one(
        {"user_id": target_user},
        {"$set": {"user_id": target_user, "added_at": datetime.utcnow()}},
        upsert=True
    )
    
    if result.upserted_id or result.modified_count:
        await update.message.reply_text(f"✅ Added user {target_user} to sudo list!")
    else:
        await update.message.reply_text("⚠️ Failed to add user to sudo list")

async def rem_sudo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove user from sudo list (owner only)"""
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
        
    # Remove from sudo list
    db = get_db()
    if db is None:
        await update.message.reply_text("⚠️ Database connection error")
        return
        
    sudo_users = db.sudo_users
    result = sudo_users.delete_one({"user_id": target_user})
    
    if result.deleted_count:
        await update.message.reply_text(f"✅ Removed user {target_user} from sudo list!")
    else:
        await update.message.reply_text("⚠️ User not found in sudo list")

# Check if user has valid token
async def has_valid_token(user_id):
    if is_sudo(user_id):
        return True
        
    db = get_db()
    if db is None:
        return False
        
    tokens = db.tokens
    token_data = tokens.find_one({"user_id": user_id})
    
    return token_data is not None  # TTL index handles expiration

def main() -> None:
    """Run the bot and HTTP server"""
    # Create database indexes
    create_ttl_index()
    create_sudo_index()
    
    # Start Flask server in a daemon thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"Flask server started in separate thread")
    
    # Get token from environment
    TOKEN = os.getenv('TELEGRAM_TOKEN')
    if not TOKEN:
        logger.error("No TELEGRAM_TOKEN found in environment!")
        return
    
    # Create Telegram application
    application = Application.builder().token(TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start_wrapper))
    application.add_handler(CommandHandler("help", help_command_wrapper))
    application.add_handler(CommandHandler("createquiz", create_quiz_wrapper))
    application.add_handler(CommandHandler("stats", stats_command_wrapper))
    application.add_handler(CommandHandler("broadcast", broadcast_command_wrapper))
    application.add_handler(CommandHandler("confirm_broadcast", confirm_broadcast_wrapper))
    application.add_handler(CommandHandler("cancel", cancel_broadcast_wrapper))
    application.add_handler(CommandHandler("token", token_command))
    application.add_handler(MessageHandler(filters.Document.TEXT, handle_document_wrapper))
    
    # Add sudo management commands
    application.add_handler(CommandHandler("addsudo", add_sudo))
    application.add_handler(CommandHandler("remsudo", rem_sudo))
    
    # Start polling
    logger.info("Starting Telegram bot in polling mode...")
    try:
        application.run_polling()
    except Exception as e:
        logger.critical(f"Telegram bot failed: {e}")
        # Attempt to restart after delay
        time.sleep(10)
        main()

if __name__ == '__main__':
    main()
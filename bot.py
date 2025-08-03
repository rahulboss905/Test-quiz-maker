import os
import logging
import threading
import time
import socket
import traceback
import asyncio
import html  # Added for HTML escaping
from http.server import BaseHTTPRequestHandler, HTTPServer
from telegram import Update
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
BOT_VERSION = "5.5"  # HTML broadcast previews

class HealthCheckHandler(BaseHTTPRequestHandler):
    """Enhanced HTTP handler for health checks and monitoring"""
    
    server_version = f"TelegramQuizBot/{BOT_VERSION}"
    
    def do_GET(self):
        try:
            start_time = time.time()
            client_ip = self.client_address[0]
            user_agent = self.headers.get('User-Agent', 'Unknown')
            
            logger.info(f"Health check request: {self.path} from {client_ip} ({user_agent})")
            
            # Handle all valid endpoints
            if self.path in ['/', '/health', '/status']:
                # Simple plain text response for monitoring services
                response_text = "OK"
                content_type = "text/plain"
                
                # Detailed HTML response for browser requests
                if "Mozilla" in user_agent:  # Browser detection
                    status = "🟢 Bot is running"
                    uptime = time.time() - self.server.start_time
                    hostname = socket.gethostname()
                    
                    response_text = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Quiz Bot Status</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .container {{ max-width: 800px; margin: 0 auto; }}
        .status {{ font-size: 1.5em; font-weight: bold; color: #2ecc71; }}
        .info {{ margin-top: 20px; padding: 15px; background-color: #f8f9fa; border-radius: 5px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Telegram Quiz Bot Status</h1>
        <div class="status">{status}</div>
        
        <div class="info">
            <p><strong>Hostname:</strong> {hostname}</p>
            <p><strong>Uptime:</strong> {uptime:.2f} seconds</p>
            <p><strong>Version:</strong> {BOT_VERSION}</p>
            <p><strong>Last Check:</strong> {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}</p>
            <p><strong>Client IP:</strong> {client_ip}</p>
            <p><strong>User Agent:</strong> {user_agent}</p>
        </div>
        
        <p style="margin-top: 30px;">
            <a href="https://t.me/{os.getenv('BOT_USERNAME', 'your_bot')}" target="_blank">
                Contact the bot on Telegram
            </a>
        </p>
    </div>
</body>
</html>
                    """
                    content_type = "text/html"
                
                # Send response
                response = response_text.encode('utf-8')
                self.send_response(200)
                self.send_header('Content-type', content_type)
                self.send_header('Content-Length', str(len(response)))
                self.end_headers()
                self.wfile.write(response)
                
                # Log successful request
                duration = (time.time() - start_time) * 1000
                logger.info(f"Health check passed - 200 OK - {duration:.2f}ms")
            else:
                self.send_response(404)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'404 Not Found')
                logger.warning(f"Invalid path requested: {self.path}")
                
        except Exception as e:
            logger.error(f"Health check error: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'500 Internal Server Error')

    def log_message(self, format, *args):
        """Override to prevent default logging"""
        pass

def run_http_server(port=8080):
    """Run HTTP server in a separate thread"""
    try:
        server_address = ('0.0.0.0', port)
        httpd = HTTPServer(server_address, HealthCheckHandler)
        
        # Add start time to server instance
        httpd.start_time = time.time()
        
        logger.info(f"HTTP server running on port {port}")
        logger.info(f"Access URLs:")
        logger.info(f"  http://localhost:{port}/")
        logger.info(f"  http://localhost:{port}/health")
        logger.info(f"  http://localhost:{port}/status")
        
        httpd.serve_forever()
    except Exception as e:
        logger.critical(f"Failed to start HTTP server: {e}")
        time.sleep(5)
        run_http_server(port)

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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    """Send welcome message and instructions"""
    await update.message.reply_text(
        "🌟 *Welcome to Quiz Bot!* 🌟\n\n"
        "I can turn your text files into interactive 10-second quizzes!\n\n"
        "🔹 Use /createquiz - Start quiz creation\n"
        "🔹 Use /help - Show formatting guide\n\n"
        "Let's make learning fun!",
        parse_mode='Markdown'
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    """Show detailed formatting instructions"""
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
        parse_mode='Markdown'
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
                except Exception as e:
                    logger.error(f"Poll send error: {str(e)}")
                    await update.message.reply_text("⚠️ Failed to send one quiz. Continuing...")
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
                if error and len(failed_details) < 20:  # Keep reasonable number of errors
                    failed_details.append(error)
            
            # Update progress every 10 users or last user
            if (i + 1) % 10 == 0 or (i + 1) == total_users:
                percent = (i + 1) * 100 // total_users
                await status_msg.edit_text(
                    f"📤 Broadcasting to {total_users} users...\n\n"
                    f"{i+1}/{total_users} ({percent}%)\n"
                    f"✅ Success: {success} | ❌ Failed: {failed}"
                )
                # Conservative rate limiting
                await asyncio.sleep(0.2)
        
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
            report_text += "\n".join(failed_details[:5])  # Show first 5 errors
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

def main() -> None:
    """Run the bot and HTTP server"""
    # Get port from environment (Render provides this)
    PORT = int(os.environ.get('PORT', 10000))
    logger.info(f"Starting HTTP server on port {PORT}")
    
    # Start HTTP server in a daemon thread
    http_thread = threading.Thread(target=run_http_server, args=(PORT,), daemon=True)
    http_thread.start()
    logger.info(f"HTTP server thread started")
    
    # Get token from environment
    TOKEN = os.getenv('TELEGRAM_TOKEN')
    if not TOKEN:
        logger.error("No TELEGRAM_TOKEN found in environment!")
        return
    
    # Create Telegram application
    application = Application.builder().token(TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("createquiz", create_quiz))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("broadcast", broadcast_command))
    application.add_handler(CommandHandler("confirm_broadcast", confirm_broadcast))
    application.add_handler(CommandHandler("cancel", cancel_broadcast))
    application.add_handler(MessageHandler(filters.Document.TEXT, handle_document))
    
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
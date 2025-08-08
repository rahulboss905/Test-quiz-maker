import os
import json
import logging
import random
import re
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ConversationHandler,
    CallbackQueryHandler
)

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = 7456681709  # Replace with your admin ID
SUDO_FILE = "sudo_users.json"  # File to store sudo users
TOKENS_FILE = "tokens.json"
QUIZZES_FILE = "quizzes.json"
USERS_FILE = "users.json"
TOKEN_PRICE = 50  # Points needed to get a token

# Conversation states
QUIZ_QUESTION, QUIZ_OPTIONS, QUIZ_CORRECT, QUIZ_CONFIRM = range(4)

# Load sudo users from file
def load_sudo_users():
    try:
        with open(SUDO_FILE, "r") as f:
            data = json.load(f)
            return set(data.get("sudo_users", [ADMIN_ID]))
    except (FileNotFoundError, json.JSONDecodeError):
        return {ADMIN_ID}

# Save sudo users to file
def save_sudo_users(sudo_users):
    with open(SUDO_FILE, "w") as f:
        json.dump({"sudo_users": list(sudo_users)}, f)

# Initialize sudo users
SUDO_USERS = load_sudo_users()

# Load data functions
def load_data(file):
    try:
        with open(file, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        if "token" in file:
            return {"tokens": {}, "last_request": {}}
        return {}

def save_data(data, file):
    with open(file, "w") as f:
        json.dump(data, f, indent=4)

# Load initial data
tokens_data = load_data(TOKENS_FILE)
quizzes = load_data(QUIZZES_FILE)
users = load_data(USERS_FILE)

# Initialize data structures
tokens = tokens_data.get("tokens", {})
last_token_request = tokens_data.get("last_request", {})
quizzes = quizzes if quizzes else {}
users = users if users else {}

# Helper functions
def has_tokens(user_id):
    user_id = str(user_id)
    return tokens.get(user_id, 0) > 0

def use_token(user_id):
    user_id = str(user_id)
    if tokens.get(user_id, 0) > 0:
        tokens[user_id] -= 1
        save_data({"tokens": tokens, "last_request": last_token_request}, TOKENS_FILE)
        return True
    return False

def add_user(user_id):
    user_id = str(user_id)
    if user_id not in users:
        users[user_id] = {"points": 0, "quizzes_taken": 0, "quizzes_created": 0}
        save_data(users, USERS_FILE)

def add_points(user_id, points):
    user_id = str(user_id)
    users[user_id]["points"] += points
    save_data(users, USERS_FILE)

def add_quiz_created(user_id):
    user_id = str(user_id)
    users[user_id]["quizzes_created"] += 1
    save_data(users, USERS_FILE)

def add_quiz_taken(user_id):
    user_id = str(user_id)
    users[user_id]["quizzes_taken"] += 1
    save_data(users, USERS_FILE)

# Command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    add_user(user_id)
    
    keyboard = [
        [InlineKeyboardButton("📺 Watch Tutorial Video", url="https://youtu.be/WeqpaV6VnO4?si=Y0pDondqe-nmIuht")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🌟 Welcome to QuizBot! 🌟\n\n"
        "Create your own quizzes with /create\n"
        "Take quizzes with /quiz\n\n"
        "Watch our tutorial video to learn how to create amazing quizzes:",
        reply_markup=reply_markup
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🎥 Watch Tutorial", url="https://youtu.be/WeqpaV6VnO4?si=Y0pDondqe-nmIuht")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    help_text = (
        "📚 QuizBot Help:\n\n"
        "/start - Start the bot\n"
        "/help - Show this help message\n"
        "/create - Create a new quiz (requires token)\n"
        "/quiz - Take a random quiz\n"
        "/tokens - Show your available tokens\n"
        "/get_token - Request a token\n"
        "/stats - Show bot statistics\n"
        "/leaderboard - Show top users\n\n"
        "🔧 Admin Commands:\n"
        "/add_tokens [user_id] [amount] - Add tokens to a user\n"
        "/addsudo [user_id/@username] - Add user to sudo list\n"
        "/removesudo [user_id] - Remove user from sudo list\n"
        "/broadcast [message] - Send message to all users\n\n"
        "Check out our tutorial video for a complete guide:"
    )
    
    await update.message.reply_text(help_text, reply_markup=reply_markup)

async def create_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # Sudo users bypass token requirement
    if user_id not in SUDO_USERS:
        if not has_tokens(user_id):
            await update.message.reply_text(
                "❌ You need a token to create quizzes!\n"
                "Use /get_token to request one."
            )
            return False
    
    await update.message.reply_text(
        "📝 Let's create a new quiz!\n\n"
        "Please send your question:"
    )
    return QUIZ_QUESTION

async def quiz_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["quiz"] = {"question": update.message.text}
    await update.message.reply_text(
        "📋 Great! Now send the options separated by commas.\n"
        "Example: Option A, Option B, Option C, Option D"
    )
    return QUIZ_OPTIONS

async def quiz_options(update: Update, context: ContextTypes.DEFAULT_TYPE):
    options = [opt.strip() for opt in update.message.text.split(",") if opt.strip()]
    
    if len(options) < 2:
        await update.message.reply_text("❌ Please provide at least 2 options.")
        return QUIZ_OPTIONS
    
    context.user_data["quiz"]["options"] = options
    
    options_text = "\n".join([f"{i+1}. {opt}" for i, opt in enumerate(options)])
    await update.message.reply_text(
        f"📋 Options received:\n{options_text}\n\n"
        "Now send the number of the correct option (1, 2, 3, ...):"
    )
    return QUIZ_CORRECT

async def quiz_correct(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        correct_index = int(update.message.text.strip()) - 1
        options = context.user_data["quiz"]["options"]
        
        if correct_index < 0 or correct_index >= len(options):
            raise ValueError
        
        context.user_data["quiz"]["correct"] = correct_index
    except (ValueError, IndexError):
        await update.message.reply_text("❌ Invalid option number. Please enter a valid number:")
        return QUIZ_CORRECT
    
    quiz = context.user_data["quiz"]
    confirmation_text = (
        f"✅ Quiz Preview:\n\n"
        f"Question: {quiz['question']}\n\n"
        f"Options:\n" + "\n".join(
            f"{i+1}. {opt} {'(Correct)' if i == quiz['correct'] else ''}" 
            for i, opt in enumerate(quiz["options"])
        ) + "\n\nDoes this look correct? (yes/no)"
    )
    
    await update.message.reply_text(confirmation_text)
    return QUIZ_CONFIRM

async def quiz_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    response = update.message.text.lower()
    if response == "yes":
        # Generate unique quiz ID
        quiz_id = "qz_" + str(int(datetime.now().timestamp()))[-8:]
        quizzes[quiz_id] = {
            "question": context.user_data["quiz"]["question"],
            "options": context.user_data["quiz"]["options"],
            "correct": context.user_data["quiz"]["correct"],
            "creator": update.effective_user.id
        }
        save_data(quizzes, QUIZZES_FILE)
        
        # Deduct token for non-sudo users
        user_id = update.effective_user.id
        if user_id not in SUDO_USERS:
            use_token(user_id)
        
        # Update user stats
        add_quiz_created(user_id)
        
        await update.message.reply_text(
            f"🎉 Quiz created successfully!\n\n"
            f"Share this ID for others to take it: <code>{quiz_id}</code>\n\n"
            f"You have {tokens.get(str(user_id), 0)} tokens remaining.",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text("Quiz creation canceled.")
    
    context.user_data.clear()
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Operation canceled.")
    context.user_data.clear()
    return ConversationHandler.END

async def take_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not quizzes:
        await update.message.reply_text("No quizzes available yet. Create one with /create!")
        return
    
    quiz_id, quiz = random.choice(list(quizzes.items()))
    
    options = quiz["options"]
    keyboard = []
    for i, option in enumerate(options):
        keyboard.append([InlineKeyboardButton(option, callback_data=f"quiz_{quiz_id}_{i}")])
    
    context.user_data["current_quiz"] = quiz_id
    context.user_data["correct_index"] = quiz["correct"]
    
    await update.message.reply_text(
        f"❓ Quiz: {quiz['question']}",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def quiz_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    _, quiz_id, selected_index = query.data.split("_")
    selected_index = int(selected_index)
    correct_index = context.user_data["correct_index"]
    
    quiz = quizzes.get(quiz_id)
    if not quiz:
        await query.edit_message_text("Quiz no longer exists.")
        return
    
    user_id = query.from_user.id
    add_user(user_id)
    
    if selected_index == correct_index:
        add_points(user_id, 10)
        add_quiz_taken(user_id)
        result = "✅ Correct! +10 points!"
    else:
        correct_answer = quiz["options"][correct_index]
        result = f"❌ Incorrect! The correct answer was: {correct_answer}"
    
    await query.edit_message_text(
        f"{result}\n\n"
        f"Question: {quiz['question']}\n"
        f"Your answer: {quiz['options'][selected_index]}\n"
        f"Correct answer: {quiz['options'][correct_index]}"
    )
    
    context.user_data.clear()

async def tokens_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_tokens = tokens.get(str(user_id), 0)
    
    if user_id in SUDO_USERS:
        status = "🌟 Sudo User (Unlimited Quizzes)"
    else:
        status = f"🔑 Tokens: {user_tokens}"
    
    await update.message.reply_text(
        f"🔑 Your Account Status:\n\n"
        f"{status}\n\n"
        f"Get more tokens with /get_token"
    )

async def get_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    
    # Sudo users don't need tokens
    if update.effective_user.id in SUDO_USERS:
        await update.message.reply_text("🌟 You're a sudo user! You don't need tokens to create quizzes.")
        return
    
    # Check if user has enough points
    user_points = users.get(user_id, {}).get("points", 0)
    if user_points < TOKEN_PRICE:
        await update.message.reply_text(
            f"❌ You need {TOKEN_PRICE} points to get a token!\n"
            f"You currently have {user_points} points.\n\n"
            "Take more quizzes to earn points!"
        )
        return
    
    # Deduct points and add token
    users[user_id]["points"] -= TOKEN_PRICE
    tokens[user_id] = tokens.get(user_id, 0) + 1
    save_data({"tokens": tokens, "last_request": last_token_request}, TOKENS_FILE)
    save_data(users, USERS_FILE)
    
    await update.message.reply_text(
        f"🎉 Token purchased successfully!\n\n"
        f"• {TOKEN_PRICE} points deducted\n"
        f"• New token balance: {tokens[user_id]}\n"
        f"• Current points: {users[user_id]['points']}"
    )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    total_users = len(users)
    total_quizzes = len(quizzes)
    total_tokens = sum(tokens.values())
    sudo_count = len(SUDO_USERS)
    
    stats_text = (
        f"📊 Bot Statistics:\n\n"
        f"• Total Users: {total_users}\n"
        f"• Total Quizzes: {total_quizzes}\n"
        f"• Sudo Users: {sudo_count}\n"
        f"• Available Tokens: {total_tokens}"
    )
    
    await update.message.reply_text(stats_text)

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not users:
        await update.message.reply_text("No users yet!")
        return
    
    top_users = sorted(
        [(uid, data["points"]) for uid, data in users.items()],
        key=lambda x: x[1],
        reverse=True
    )[:10]
    
    leaderboard_text = "🏆 Top Users:\n\n"
    for i, (user_id, points) in enumerate(top_users):
        try:
            user = await context.bot.get_chat(int(user_id))
            name = user.username or user.first_name
        except:
            name = f"User {user_id}"
        
        leaderboard_text += f"{i+1}. {name}: {points} points\n"
    
    await update.message.reply_text(leaderboard_text)

# Admin commands
async def add_tokens(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can use this command!")
        return
    
    if len(context.args) != 2:
        await update.message.reply_text("Usage: /add_tokens <user_id> <amount>")
        return
    
    try:
        user_id = str(context.args[0])
        amount = int(context.args[1])
        
        tokens[user_id] = tokens.get(user_id, 0) + amount
        save_data({"tokens": tokens, "last_request": last_token_request}, TOKENS_FILE)
        
        await update.message.reply_text(
            f"✅ Added {amount} tokens to user {user_id}!\n"
            f"New balance: {tokens[user_id]}"
        )
    except ValueError:
        await update.message.reply_text("Invalid input. Usage: /add_tokens <user_id> <amount>")

async def addsudo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can use this command!")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /addsudo <user_id or @username>")
        return
    
    identifier = context.args[0].strip()
    user_id = None
    
    # Check if it's a username
    if identifier.startswith("@"):
        username = identifier[1:]
        # Search through users to find matching username
        for uid, data in users.items():
            try:
                user_chat = await context.bot.get_chat(int(uid))
                if user_chat.username and user_chat.username.lower() == username.lower():
                    user_id = int(uid)
                    break
            except:
                continue
        
        if not user_id:
            await update.message.reply_text(f"❌ User @{username} not found in bot's user list")
            return
    # Check if it's a user ID
    else:
        try:
            user_id = int(identifier)
        except ValueError:
            await update.message.reply_text("❌ Invalid input. Provide user ID or @username")
            return
    
    # Add to sudo users
    if user_id in SUDO_USERS:
        await update.message.reply_text(f"ℹ️ User {user_id} is already a sudo user")
        return
    
    SUDO_USERS.add(user_id)
    save_sudo_users(SUDO_USERS)
    
    try:
        user_chat = await context.bot.get_chat(user_id)
        user_name = user_chat.username or user_chat.first_name
        await update.message.reply_text(f"✅ Added {user_name} ({user_id}) to sudo users!")
    except:
        await update.message.reply_text(f"✅ Added user ID {user_id} to sudo users!")

async def removesudo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can use this command!")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /removesudo <user_id>")
        return
    
    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID")
        return
    
    if user_id not in SUDO_USERS:
        await update.message.reply_text(f"ℹ️ User {user_id} is not a sudo user")
        return
    
    SUDO_USERS.remove(user_id)
    save_sudo_users(SUDO_USERS)
    
    try:
        user_chat = await context.bot.get_chat(user_id)
        user_name = user_chat.username or user_chat.first_name
        await update.message.reply_text(f"✅ Removed {user_name} ({user_id}) from sudo users")
    except:
        await update.message.reply_text(f"✅ Removed user ID {user_id} from sudo users")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can use this command!")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    
    message = " ".join(context.args)
    success = 0
    failed = 0
    
    for user_id in users:
        try:
            await context.bot.send_message(chat_id=int(user_id), text=f"📢 Broadcast:\n\n{message}")
            success += 1
        except:
            failed += 1
    
    await update.message.reply_text(
        f"Broadcast completed:\n"
        f"• Sent to: {success} users\n"
        f"• Failed: {failed} users"
    )

def main():
    application = Application.builder().token(TOKEN).build()
    
    # Conversation handler for quiz creation
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("create", create_quiz)],
        states={
            QUIZ_QUESTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, quiz_question)],
            QUIZ_OPTIONS: [MessageHandler(filters.TEXT & ~filters.COMMAND, quiz_options)],
            QUIZ_CORRECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, quiz_correct)],
            QUIZ_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, quiz_confirm)]
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("quiz", take_quiz))
    application.add_handler(CommandHandler("tokens", tokens_command))
    application.add_handler(CommandHandler("get_token", get_token))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    application.add_handler(CommandHandler("add_tokens", add_tokens))
    application.add_handler(CommandHandler("addsudo", addsudo))
    application.add_handler(CommandHandler("removesudo", removesudo))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CallbackQueryHandler(quiz_answer, pattern=r"^quiz_"))
    
    # Start the bot
    application.run_polling()

if __name__ == "__main__":
    main()

import os
import logging
import time
from threading import Thread
from datetime import datetime, timedelta
from flask import Flask
from pymongo import MongoClient
from telegram import Update, ChatPermissions, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackQueryHandler
)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# MongoDB setup
mongo_client = MongoClient(os.getenv('MONGO_URI'))
db = mongo_client.telegram_bot
fsub_collection = db.fsub_channels
user_collection = db.users

# Flask app for health checks
app = Flask(__name__)

@app.route('/')
def health_check():
    return "Bot is running", 200

def run_flask():
    app.run(host='0.0.0.0', port=8000)

# Global variables for bot stats
BOT_START_TIME = time.time()

async def delete_previous_warnings(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Delete all previous warning messages for a user"""
    if 'user_warnings' not in context.chat_data:
        return
    
    msg_ids = context.chat_data['user_warnings'].get(user_id, [])
    if not isinstance(msg_ids, list):
        msg_ids = [msg_ids]
    
    for msg_id in msg_ids:
        try:
            await context.bot.delete_message(
                chat_id=chat_id,
                message_id=msg_id
            )
        except Exception as e:
            logger.warning(f"Could not delete message {msg_id}: {e}")
    
    if user_id in context.chat_data['user_warnings']:
        del context.chat_data['user_warnings'][user_id]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == 'private':
        user = update.effective_user
        user_collection.update_one(
            {'user_id': user.id},
            {'$set': {
                'first_name': user.first_name,
                'last_name': user.last_name,
                'username': user.username,
                'last_interaction': datetime.now()
            }},
            upsert=True
        )
    
    keyboard = [
        [
            InlineKeyboardButton(
                "➕ Add to Group", 
                url=f"https://t.me/{context.bot.username}?startgroup=true"
            ),
            InlineKeyboardButton(
                "➕ Add to Channel", 
                url=f"https://t.me/{context.bot.username}?startchannel=true"
            )
        ]
    ]
    
    if os.getenv('SUPPORT_CHANNEL'):
        keyboard.append([
            InlineKeyboardButton(
                "📢 Support Channel", 
                url=f"https://t.me/{os.getenv('SUPPORT_CHANNEL')}"
            )
        ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)

    welcome_text = (
        "👋 *Welcome to Force Subscription Bot!*\n\n"
        "I help group admins enforce channel subscriptions by muting users who haven't joined required channels.\n\n"
        "✨ *Features:*\n"
        "• Auto-mute non-subscribed users\n"
        "• 5-minute mute duration\n"
        "• Self-unmute after joining\n"
        "• Supports both public & private channels\n\n"
        "📌 *How to setup:*\n"
        "1. Add me to your group as admin\n"
        "2. Use `/fsub @channel` to set requirements\n"
        "3. I'll handle the rest!\n\n"
        "Click the buttons below to add me to your groups/channels:"
    )

    if update.effective_chat.type == 'private':
        await update.message.reply_text(
            welcome_text,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            "I'm a forced subscription bot. Use /fsub to set a required channel for this group.\n\n"
            "ℹ️ I need to be admin in both this group and the channel to work properly.",
            reply_markup=reply_markup
        )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = []
    if os.getenv('SUPPORT_CHANNEL'):
        keyboard.append([
            InlineKeyboardButton(
                "📢 Support Channel", 
                url=f"https://t.me/{os.getenv('SUPPORT_CHANNEL')}"
            )
        ])
    
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    
    await update.message.reply_text(
        "⚠️ Admin Requirements:\n"
        "- Make me admin in both group and channel\n"
        "- Grant me 'Restrict users' permission in group\n\n"
        "Commands:\n"
        "/start - Introduction\n"
        "/help - This message\n"
        "/fsub [@channel|ID|reply] - Set required channel\n\n"
        "I'll mute anyone who hasn't joined the required channel for 5 minutes.",
        reply_markup=reply_markup
    )

async def set_fsub_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.type == 'private':
        await update.message.reply_text("This command only works in groups.")
        return
    
    member = await chat.get_member(user.id)
    if member.status not in ['administrator', 'creator']:
        await update.message.reply_text("❌ Only admins can use this command.")
        return
    
    if update.message.reply_to_message and update.message.reply_to_message.sender_chat:
        if update.message.reply_to_message.sender_chat.type == 'channel':
            channel = update.message.reply_to_message.sender_chat.username or str(update.message.reply_to_message.sender_chat.id)
            await save_fsub_channel(chat.id, channel, update, context)
            return
    
    if context.args:
        channel_input = context.args[0]
        if channel_input.startswith('@'):
            channel = channel_input[1:]
        elif channel_input.isdigit() or (channel_input.startswith('-') and channel_input[1:].isdigit()):
            channel = channel_input
        else:
            await update.message.reply_text("❌ Invalid channel format. Use @username, channel ID, or reply to a channel message.")
            return
        
        await save_fsub_channel(chat.id, channel, update, context)
    else:
        await update.message.reply_text(
            "Usage:\n"
            "/fsub @channelusername\n"
            "/fsub channel_id\n"
            "Or reply to a channel message with /fsub"
        )

async def save_fsub_channel(chat_id: int, channel: str, update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat = await context.bot.get_chat(f"@{channel}" if not channel.startswith('-') else channel)
        if chat.type != 'channel':
            await update.message.reply_text("❌ The specified chat is not a channel.")
            return
        
        fsub_collection.update_one(
            {'chat_id': chat_id},
            {'$set': {'channel': channel, 'channel_id': chat.id}},
            upsert=True
        )
        
        try:
            bot_member = await context.bot.get_chat_member(chat.id, context.bot.id)
            if bot_member.status not in ['administrator', 'creator']:
                await update.message.reply_text(
                    "⚠️ Warning: I'm not admin in that channel.\n"
                    "I won't be able to check memberships until you make me admin."
                )
                return
            
            await update.message.reply_text(
                f"✅ Success! All members must now join {f'@{channel}' if not channel.startswith('-') else 'the channel'} to participate here."
            )
        except Exception as perm_error:
            logger.error(f"Permission check error: {perm_error}")
            await update.message.reply_text(
                "⚠️ Warning: I can't check my permissions in that channel.\n"
                "Make sure I'm added as admin to the channel."
            )
            
    except Exception as e:
        logger.error(f"Error setting channel: {e}")
        await update.message.reply_text(
            "❌ Failed to set channel. Make sure:\n"
            "1. The channel exists\n"
            "2. I'm a member of the channel\n"
            "3. You provided a valid channel identifier"
        )

async def check_membership(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.forward_from_chat and update.message.forward_from_chat.type == 'channel':
        return
    
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.type == 'private' or user.is_bot:
        return
    
    fsub_data = fsub_collection.find_one({'chat_id': chat.id})
    if not fsub_data:
        return
    
    channel = fsub_data.get('channel')
    channel_id = fsub_data.get('channel_id')
    
    try:
        member = await chat.get_member(user.id)
        if member.status in ['administrator', 'creator']:
            return
        
        target_chat = channel_id if channel_id else (f"@{channel}" if channel and not channel.startswith('-') else channel)
        
        if not target_chat:
            logger.warning(f"No valid channel identifier found for chat {chat.id}")
            return
        
        try:
            bot_member = await context.bot.get_chat_member(target_chat, context.bot.id)
            if bot_member.status not in ['administrator', 'creator']:
                last_warning = context.chat_data.get('last_channel_warning', 0)
                current_time = time.time()
                if current_time - last_warning > 3600:
                    await update.message.reply_text(
                        "⚠️ I need admin in the channel to check memberships.\n"
                        "Please make me admin or update /fsub settings."
                    )
                    context.chat_data['last_channel_warning'] = current_time
                return
        except Exception as perm_error:
            logger.error(f"Permission check error: {perm_error}")
            return
        
        chat_member = await context.bot.get_chat_member(target_chat, user.id)
        if chat_member.status in ['left', 'kicked']:
            permissions = ChatPermissions(
                can_send_messages=False,
                can_send_audios=False,
                can_send_documents=False,
                can_send_photos=False,
                can_send_videos=False,
                can_send_video_notes=False,
                can_send_voice_notes=False,
                can_send_polls=False,
                can_send_other_messages=False,
                can_add_web_page_previews=False
            )
            
            try:
                mute_duration = 5 * 60
                until_date = int(time.time()) + mute_duration
                
                await chat.restrict_member(
                    user.id, 
                    permissions,
                    until_date=until_date
                )
                
                await delete_previous_warnings(chat.id, user.id, context)
                
                keyboard = []
                
                keyboard.append([
                    InlineKeyboardButton(
                        "✅ Unmute Me", 
                        callback_data=f"unmute:{chat.id}:{user.id}"
                    )
                ])
                
                invite_link = None
                try:
                    if channel_id and (not channel or channel.startswith('-')):
                        chat_obj = await context.bot.get_chat(channel_id)
                        if chat_obj.invite_link:
                            invite_link = chat_obj.invite_link
                        else:
                            invite_link_obj = await context.bot.create_chat_invite_link(
                                chat_id=channel_id,
                                creates_join_request=False,
                                name="FSub Link"
                            )
                            invite_link = invite_link_obj.invite_link
                except Exception as e:
                    logger.warning(f"Could not get/create invite link for channel: {e}")
                
                if channel and not channel.startswith('-'):
                    keyboard.append([
                        InlineKeyboardButton(
                            "🔗 Join Channel", 
                            url=f"https://t.me/{channel}"
                        )
                    ])
                elif invite_link:
                    keyboard.append([
                        InlineKeyboardButton(
                            "🔗 Join Private Channel", 
                            url=invite_link
                        )
                    ])
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                channel_display = ""
                if channel and not channel.startswith('-'):
                    channel_display = f"@{channel}"
                elif channel_id:
                    channel_display = "the private channel"
                else:
                    channel_display = "the required channel"
                
                warning_msg = await update.message.reply_text(
                    f"⚠️ {user.mention_html()} has been muted for 5 minutes.\n"
                    f"Reason: Not joined {channel_display}\n\n"
                    "After joining, click 'Unmute Me' to verify membership.",
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                
                if 'user_warnings' not in context.chat_data:
                    context.chat_data['user_warnings'] = {}
                
                if user.id not in context.chat_data['user_warnings']:
                    context.chat_data['user_warnings'][user.id] = []
                elif not isinstance(context.chat_data['user_warnings'][user.id], list):
                    context.chat_data['user_warnings'][user.id] = [context.chat_data['user_warnings'][user.id]]
                
                context.chat_data['user_warnings'][user.id].append(warning_msg.message_id)
                
            except Exception as mute_error:
                logger.error(f"Error muting user: {mute_error}")
                last_mute_error = context.chat_data.get('last_mute_error', 0)
                current_time = time.time()
                if current_time - last_mute_error > 3600:
                    await update.message.reply_text(
                        "⚠️ Failed to mute user. Make sure I have 'Restrict users' permission in this group."
                    )
                    context.chat_data['last_mute_error'] = current_time
    
    except Exception as e:
        logger.error(f"Error in membership check: {e}")

async def unmute_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data.split(':')
    if len(data) != 3 or data[0] != 'unmute':
        await query.edit_message_text("⚠️ Invalid request. Please try again later.")
        return
    
    chat_id = int(data[1])
    user_id = int(data[2])
    
    if query.from_user.id != user_id:
        await query.answer("❌ This button is only for the muted user!", show_alert=True)
        return
    
    try:
        fsub_data = fsub_collection.find_one({'chat_id': chat_id})
        if not fsub_data:
            await query.answer("❌ Configuration error. Please contact admin.", show_alert=True)
            return
        
        channel = fsub_data.get('channel')
        channel_id = fsub_data.get('channel_id')
        
        target_chat = channel_id if channel_id else (f"@{channel}" if channel and not channel.startswith('-') else channel)
        
        if not target_chat:
            await query.answer("❌ Configuration error. Please contact admin.", show_alert=True)
            return
        
        try:
            chat_member = await context.bot.get_chat_member(target_chat, user_id)
            if chat_member.status in ['left', 'kicked']:
                await query.answer(
                    "❌ You haven't joined the channel yet! Please join first.",
                    show_alert=True
                )
                return
        except Exception as e:
            logger.error(f"Error verifying membership: {e}")
            await query.answer(
                "⚠️ Error verifying membership. Please try again later.",
                show_alert=True
            )
            return
        
        chat = await context.bot.get_chat(chat_id)
        
        permissions = ChatPermissions(
            can_send_messages=True,
            can_send_audios=True,
            can_send_documents=True,
            can_send_photos=True,
            can_send_videos=True,
            can_send_video_notes=True,
            can_send_voice_notes=True,
            can_send_polls=True,
            can_send_other_messages=True,
            can_add_web_page_previews=True
        )
        
        await chat.restrict_member(user_id, permissions)
        
        await delete_previous_warnings(chat_id, user_id, context)
        
        await query.edit_message_text(
            f"✅ {query.from_user.mention_html()} has been unmuted!",
            parse_mode='HTML'
        )
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"✅ {query.from_user.mention_html()} has been unmuted after verifying channel membership.",
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Error unmuting user: {e}")
        await query.answer(
            "⚠️ Failed to unmute. Please contact an admin.",
            show_alert=True
        )

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != os.getenv('OWNER_ID'):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return

    uptime_seconds = time.time() - BOT_START_TIME
    uptime = str(timedelta(seconds=int(uptime_seconds)))
    
    groups_count = fsub_collection.count_documents({})
    users_count = user_collection.count_documents({})
    bot_info = await context.bot.get_me()
    mongo_status = "Connected" if mongo_client.server_info() else "Disconnected"
    
    status_text = (
        f"🤖 *Bot Status Report*\n\n"
        f"• Bot Name: [{bot_info.full_name}](t.me/{bot_info.username})\n"
        f"• Uptime: `{uptime}`\n"
        f"• Groups Using: `{groups_count}`\n"
        f"• Users Tracked: `{users_count}`\n"
        f"• MongoDB: `{mongo_status}`\n\n"
        f"📊 *System Stats*\n"
        f"• Python Version: `{os.sys.version.split()[0]}`\n"
        f"• Platform: `{os.sys.platform}`"
    )
    
    await update.message.reply_text(
        status_text,
        parse_mode='Markdown',
        disable_web_page_preview=True
    )

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != os.getenv('OWNER_ID'):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text("ℹ️ Please reply to a message to broadcast it.")
        return

    context.user_data['broadcast_msg'] = {
        'chat_id': update.message.reply_to_message.chat_id,
        'message_id': update.message.reply_to_message.message_id
    }

    keyboard = [
        [InlineKeyboardButton("📢 Groups Only", callback_data="bcast_target:groups")],
        [InlineKeyboardButton("👤 Users Only", callback_data="bcast_target:users")],
        [InlineKeyboardButton("🌐 Both Groups & Users", callback_data="bcast_target:both")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🔍 Select broadcast target:",
        reply_markup=reply_markup
    )

async def broadcast_target_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    target = query.data.split(':')[1]
    context.user_data['broadcast_target'] = target
    
    keyboard = [
        [InlineKeyboardButton("📌 Yes", callback_data="bcast_pin:yes")],
        [InlineKeyboardButton("❌ No", callback_data="bcast_pin:no")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "📌 Pin message in groups?",
        reply_markup=reply_markup
    )

async def broadcast_pin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    pin_option = query.data.split(':')[1]
    context.user_data['broadcast_pin'] = pin_option
    
    msg_info = context.user_data['broadcast_msg']
    target = context.user_data['broadcast_target']
    
    del context.user_data['broadcast_msg']
    del context.user_data['broadcast_target']
    del context.user_data['broadcast_pin']
    
    recipients = []
    
    if target in ['groups', 'both']:
        groups = fsub_collection.distinct("chat_id")
        recipients.extend([('group', gid) for gid in groups])
    
    if target in ['users', 'both']:
        users = user_collection.distinct("user_id")
        recipients.extend([('user', uid) for uid in users])
    
    total = len(recipients)
    if total == 0:
        await query.edit_message_text("❌ No recipients found for broadcast.")
        return

    progress_msg = await query.edit_message_text(
        f"📢 Broadcasting to {total} recipients...\n"
        f"• Sent: 0\n"
        f"• Failed: 0"
    )

    successful = 0
    failed = 0
    failed_ids = []
    
    for idx, (recipient_type, recipient_id) in enumerate(recipients):
        try:
            sent_msg = await context.bot.copy_message(
                chat_id=recipient_id,
                from_chat_id=msg_info['chat_id'],
                message_id=msg_info['message_id']
            )
            
            if recipient_type == 'group' and pin_option == 'yes':
                try:
                    await context.bot.pin_chat_message(
                        chat_id=recipient_id,
                        message_id=sent_msg.message_id
                    )
                except Exception as pin_error:
                    logger.error(f"Pin failed in {recipient_id}: {pin_error}")
            
            successful += 1
        except Exception as e:
            logger.error(f"Broadcast failed to {recipient_type} {recipient_id}: {e}")
            failed += 1
            failed_ids.append(recipient_id)
        
        if (idx + 1) % 10 == 0 or (idx + 1) == total:
            try:
                await progress_msg.edit_text(
                    f"📢 Broadcasting to {total} recipients...\n"
                    f"• Sent: {successful}\n"
                    f"• Failed: {failed}\n"
                    f"• Progress: {idx+1}/{total} ({((idx+1)/total)*100:.1f}%)"
                )
            except Exception as e:
                logger.error(f"Progress update failed: {e}")
    
    report_text = (
        f"✅ Broadcast completed!\n\n"
        f"• Total recipients: {total}\n"
        f"• Successful: {successful}\n"
        f"• Failed: {failed}"
    )
    
    if failed > 0:
        report_text += f"\n\n❌ Failed IDs:\n{', '.join(map(str, failed_ids[:10]))}"
        if failed > 10:
            report_text += f"\n... and {failed-10} more"
    
    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=report_text
    )

def main():
    Thread(target=run_flask, daemon=True).start()
    
    application = ApplicationBuilder().token(os.getenv('BOT_TOKEN')).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("fsub", set_fsub_channel))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("broadcast", broadcast_command))
    application.add_handler(
        MessageHandler(filters.ChatType.GROUPS & ~filters.StatusUpdate.ALL, check_membership)
    )
    application.add_handler(CallbackQueryHandler(unmute_button, pattern=r"^unmute:"))
    application.add_handler(CallbackQueryHandler(broadcast_target_callback, pattern=r"^bcast_target:"))
    application.add_handler(CallbackQueryHandler(broadcast_pin_callback, pattern=r"^bcast_pin:"))
    
    application.run_polling()

if __name__ == '__main__':
    main()
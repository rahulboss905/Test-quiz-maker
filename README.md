# Telegram Quiz Bot 🤖

A feature-rich bot that converts text files into interactive Telegram quizzes with admin controls and user analytics.

## Features ✨
- 📝 Text-to-Quiz Conversion
- ✅ Automatic Format Validation
- 📊 User Statistics (MongoDB)
- 📢 Broadcast Messaging
- 🏥 Health Monitoring Dashboard
- 🔒 Admin-Only Commands
- ⏱️ 10-Second Interactive Polls

## Setup Guide ⚙️

### Prerequisites
- Python 3.9+
- MongoDB ([Free Atlas Cluster](https://www.mongodb.com/atlas/database))
- Telegram Bot Token ([@BotFather](https://t.me/BotFather))
- Your Telegram User ID ([@userinfobot](https://t.me/userinfobot))

### Environment Variables
Create `.env` file:
```env
TELEGRAM_TOKEN=your_bot_token_here
OWNER_ID=your_telegram_user_id
MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/telegram_bot?retryWrites=true&w=majority

# Clone repository
git clone https://github.com/your-username/telegram-quiz-bot.git
cd telegram-quiz-bot

# Install dependencies
pip install -r requirements.txt

# Start the bot
python bot.py

# Clone repository
git clone https://github.com/your-username/telegram-quiz-bot.git
cd telegram-quiz-bot

# Install dependencies
pip install -r requirements.txt

# Start the bot
python bot.py

services:
  - type: web
    name: telegram-quiz-bot
    runtime: python
    env:
      - key: PORT
        value: 10000
      - key: TELEGRAM_TOKEN
        fromService:
          type: secret
          name: telegram_token
      - key: OWNER_ID
        fromService:
          type: secret
          name: owner_id
      - key: MONGO_URI
        fromService:
          type: secret
          name: mongo_uri.

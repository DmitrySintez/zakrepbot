# Channel Forwarder Bot

A Telegram bot that forwards messages from a source channel to multiple target groups with advanced features and robust design patterns.

## Architecture

The project uses several design patterns to ensure maintainable and extensible code:

1. **Command Pattern**
   - Located in `commands/` directory
   - Base abstract Command class for all bot commands
   - Each command implemented as a separate class
   - Consistent command execution flow

2. **Factory Pattern**
   - `KeyboardFactory` in `utils/keyboard_factory.py`
   - Centralized creation of UI elements
   - Consistent keyboard layout management

3. **Singleton Pattern**
   - `Config` class for application configuration
   - `ChatCacheService` for chat information caching
   - Ensures single instances for global state

4. **State Pattern**
   - Bot states managed through `BotState` hierarchy
   - `IdleState` and `RunningState` implementations
   - Clean state transitions via `BotContext`

5. **Repository Pattern**
   - Database operations in `Repository` class
   - Connection pooling and prepared statements
   - Centralized data access layer

6. **Observer Pattern**
   - Cache updates notification system
   - Bot as observer for cache changes
   - Loose coupling between components

## Project Structure

```
├── commands/
│   ├── base_command.py     # Base command class
│   └── commands.py         # Command implementations
├── database/
│   └── repository.py       # Database repository
├── services/
│   └── chat_cache.py       # Chat caching service
├── utils/
│   ├── bot_state.py        # Bot state management
│   ├── config.py           # Configuration singleton
│   └── keyboard_factory.py # UI factory
├── bot.py                  # Main bot class
├── requirements.txt        # Dependencies
└── README.md              # Documentation
```

## Features

- Message forwarding from source channel to target groups
- Configurable repost intervals
- Chat member tracking
- Message validation
- Statistics tracking
- Cache management
- Connection pooling
- Error handling and logging

## Dependencies

- aiogram: Telegram Bot API framework
- python-dotenv: Environment variables management
- loguru: Enhanced logging
- aiosqlite: Async SQLite database
- psutil: Process utilities

## Setup

1. Create a `.env` file with the following variables:
   ```
   BOT_TOKEN=your_bot_token
   OWNER_ID=your_telegram_id
   SOURCE_CHANNEL=source_channel_username
   DB_PATH=forwarder.db
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the bot:
   ```bash
   python bot.py
   ```

## Usage

1. Add the bot to your source channel as administrator
2. Add the bot to target groups
3. Start the bot with `/start` command
4. Use the inline keyboard to control forwarding

## Bot Commands

- `/start` - Show main menu
- `/help` - Show help message
- `/setlast` - Set last message ID manually
- `/getlast` - Get current last message ID
- `/forwardnow` - Forward last message immediately
- `/test` - Test message existence
- `/findlast` - Find last valid message

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

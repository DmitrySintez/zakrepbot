from abc import ABC, abstractmethod
import asyncio
import os
import json
import shutil
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any, Set
import multiprocessing
from multiprocessing import Process

from loguru import logger
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

# Импортируем наши модули
from utils.config import Config
from utils.bot_state import BotContext, IdleState, RunningState
from utils.keyboard_factory import KeyboardFactory
from database.repository import Repository, DatabaseConnectionPool
from services.chat_cache import ChatCacheService, CacheObserver, ChatInfo
from commands.commands import (
    StartCommand,
    HelpCommand,
    SetLastMessageCommand,
    GetLastMessageCommand,
    ForwardNowCommand,
    TestMessageCommand,
    FindLastMessageCommand
)

class BotManager:
    """Manages multiple bot instances"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(BotManager, cls).__new__(cls)
            # Create a manager instance properly
            import multiprocessing
            cls._instance.manager = multiprocessing.Manager()
            cls._instance.bots = cls._instance.manager.dict()
            cls._instance.processes = {}
            
            from loguru import logger
            logger.info("BotManager singleton created")
        return cls._instance
    
    def add_bot(self, bot_id: str, process: Process):
        """Add a bot process to the manager"""
        self.bots[bot_id] = {
            'status': 'running',
            'pid': process.pid,
            'started_at': datetime.now().isoformat()
        }
        self.processes[bot_id] = process
        
        from loguru import logger
        logger.info(f"Added bot {bot_id} to manager. Total bots: {len(self.bots)}")
        logger.debug(f"Current bots: {list(self.bots.keys())}")
    
    def remove_bot(self, bot_id: str):
        """Remove a bot from the manager"""
        if bot_id in self.processes:
            process = self.processes[bot_id]
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            del self.processes[bot_id]
            del self.bots[bot_id]
    
    def get_bot_status(self, bot_id: str):
        """Get status of a specific bot"""
        return self.bots.get(bot_id, None)
    
    def list_bots(self):
        """List all managed bots"""
        from loguru import logger
        logger.debug(f"Listing bots. Total: {len(self.bots)}, Keys: {list(self.bots.keys())}")
        return dict(self.bots)



# Add this function to run a bot in a separate process
def run_bot_process(bot_token: str, owner_id: int, source_channels: list, bot_id: str):
    """Wrapper to run bot in a separate process"""
    # Set up logging for the subprocess
    from loguru import logger
    logger.add(f"bot_{bot_id}.log", rotation="10 MB")
    
    # Create new event loop for this process
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(run_bot_instance(bot_token, owner_id, source_channels, bot_id))
    except Exception as e:
        logger.error(f"Error in bot process {bot_id}: {e}")
    finally:
        loop.close()


# Add this function to run a bot in a separate process
async def run_bot_instance(bot_token: str, owner_id: int, source_channels: list, bot_id: str):
    """Run a bot instance with specific configuration"""
    import os
    import sys
    
    # Create a temporary config for this bot instance
    os.environ['BOT_TOKEN'] = bot_token
    os.environ['OWNER_ID'] = str(owner_id)
    
    # Create a custom config class for this instance
    from utils.config import Config
    
    # Override the singleton pattern for this process
    Config._instance = None
    config = Config()
    config.bot_token = bot_token
    config.owner_id = owner_id
    config.source_channels = source_channels
    
    # Create a new bot instance
    bot_instance = ForwarderBot()
    bot_instance.bot_id = bot_id  # Add identifier
    
    try:
        await bot_instance.start()
    except Exception as e:
        logger.error(f"Bot {bot_id} crashed: {e}")
        raise


class ForwarderBot(CacheObserver):
    """Основной класс бота для пересылки сообщений из каналов в чаты с автозакреплением"""
    
    def __init__(self):
        self.config = Config()
        self.bot = Bot(token=self.config.bot_token)
        self.dp = Dispatcher()
        self.context = BotContext(self.bot, self.config)
        self.cache_service = ChatCacheService()
        self.awaiting_channel_input = None  # Отслеживание ввода канала
        self.awaiting_interval_input = None  # Отслеживание ввода интервала
        
        # Словарь для хранения ID закрепленных сообщений в чатах
        self.pinned_messages = {}
        
        # Регистрируем себя как наблюдатель кэша
        self.cache_service.add_observer(self)
        
        # Настраиваем обработчики
        self._setup_handlers()

    async def clone_bot_prompt(self, callback: types.CallbackQuery):
        """Prompt for cloning the bot"""
        if callback.from_user.id != self.config.owner_id:
            return
        
        # Set state to wait for new token
        self.awaiting_clone_token = callback.from_user.id
        
        kb = InlineKeyboardBuilder()
        kb.button(text="Отмена", callback_data="back_to_main")
        
        await callback.message.edit_text(
            "🤖 Клонирование бота\n\n"
            "1. Создайте нового бота через @BotFather\n"
            "2. Получите новый токен бота\n"
            "3. Отправьте токен сюда\n\n"
            "После проверки токена вы сможете выбрать:\n"
            "• Запустить клон в текущем процессе\n"
            "• Создать файлы для отдельного запуска\n\n"
            "Отправьте новый токен сообщением 💬",
            reply_markup=kb.as_markup()
        )
        await callback.answer()

    # Let's also add the overwrite_clone method that was referenced earlier
    async def overwrite_clone(self, callback: types.CallbackQuery):
        """Handler for overwriting existing clone"""
        if callback.from_user.id != self.config.owner_id:
            return
        
        # Parse data: overwrite_clone_dirname_token
        parts = callback.data.split('_', 3)
        if len(parts) != 4:
            await callback.answer("Ошибка в данных")
            return
        
        clone_dir = parts[2]
        new_token = parts[3]
        
        # Delete existing clone
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        clone_path = os.path.join(os.path.dirname(current_dir), clone_dir)
        
        if os.path.exists(clone_path):
            shutil.rmtree(clone_path)
        
        # Perform clone
        await self._perform_bot_clone(new_token, clone_dir, callback.message)
        await callback.answer()
    async def _perform_bot_clone(self, new_token: str, clone_dir: str, progress_msg=None):
        """Perform the actual bot cloning"""
        try:
            # Get bot info for the new token
            test_bot = Bot(token=new_token)
            bot_info = await test_bot.get_me()
            await test_bot.session.close()
            
            # Get paths
            current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            clone_path = os.path.join(os.path.dirname(current_dir), clone_dir)
            
            # Create clone directory
            os.makedirs(clone_path, exist_ok=True)
            
            # Files and directories to copy
            items_to_copy = [
                'bot.py',
                'requirements.txt',
                'Dockerfile',
                'utils',
                'commands',
                'services',
                'database'
            ]
            
            # Copy files and directories
            for item in items_to_copy:
                src = os.path.join(current_dir, item)
                dst = os.path.join(clone_path, item)
                
                if os.path.isdir(src):
                    shutil.copytree(src, dst, dirs_exist_ok=True)
                elif os.path.isfile(src):
                    shutil.copy2(src, dst)
            
            # Create new .env file with new token
            env_content = f"""# Telegram Bot Token from @BotFather
BOT_TOKEN={new_token}

# Your Telegram user ID (get from @userinfobot)
OWNER_ID={self.config.owner_id}

# Source channel username or ID (bot must be admin)
# Can be either numeric ID (-100...) or channel username without @
SOURCE_CHANNEL={self.config.source_channels[0] if self.config.source_channels else ''}
"""
            
            with open(os.path.join(clone_path, '.env'), 'w') as f:
                f.write(env_content)
            
            # Copy bot_config.json with same channels
            if os.path.exists(os.path.join(current_dir, 'bot_config.json')):
                shutil.copy2(
                    os.path.join(current_dir, 'bot_config.json'),
                    os.path.join(clone_path, 'bot_config.json')
                )
            
            # Create a start script for Linux
            start_script = f"""#!/bin/bash
cd "{clone_path}"
python bot.py
"""
            
            start_script_path = os.path.join(clone_path, 'start_bot.sh')
            with open(start_script_path, 'w') as f:
                f.write(start_script)
            
            # Make the script executable
            os.chmod(start_script_path, 0o755)
            
            # Create Windows start script
            start_script_windows = f"""@echo off
cd /d "{clone_path}"
python bot.py
pause
"""
            
            with open(os.path.join(clone_path, 'start_bot.bat'), 'w') as f:
                f.write(start_script_windows)
            
            # Create README.md for the clone
            readme_content = f"""# Bot Clone: @{bot_info.username}

This is a clone of the main forwarding bot.

## Configuration
- Bot Token: Configured in .env
- Owner ID: {self.config.owner_id}
- Source Channels: {', '.join(self.config.source_channels)}

## Running the bot

### Linux/Mac:
```bash
./start_bot.sh
```

### Windows:
```bash
start_bot.bat
```

### Manual:
```bash
python bot.py
```

## Important Notes
- Make sure the bot is admin in all source channels
- The bot will forward messages to the same target chats as the main bot
- Database is separate from the main bot
"""
            
            with open(os.path.join(clone_path, 'README.md'), 'w') as f:
                f.write(readme_content)
            
            if progress_msg:
                kb = InlineKeyboardBuilder()
                kb.button(text="Назад", callback_data="back_to_main")
                
                success_text = (
                    f"✅ Бот успешно клонирован!\n\n"
                    f"📁 Папка: {clone_dir}\n"
                    f"🤖 Имя бота: @{bot_info.username}\n\n"
                    f"Для запуска клона:\n"
                    f"1. Перейдите в папку: {clone_path}\n"
                    f"2. Запустите: `python bot.py` или используйте скрипт start_bot.sh (Linux) / start_bot.bat (Windows)\n\n"
                    f"Клон будет работать независимо с теми же настройками каналов."
                )
                
                await progress_msg.edit_text(success_text, reply_markup=kb.as_markup())
            
            logger.info(f"Successfully cloned bot to {clone_dir}")
            
        except Exception as e:
            logger.error(f"Error during bot clone: {e}")
            if progress_msg:
                kb = InlineKeyboardBuilder()
                kb.button(text="Назад", callback_data="back_to_main")
                
                await progress_msg.edit_text(
                    f"❌ Ошибка при клонировании: {e}",
                    reply_markup=kb.as_markup()
                )
            raise


    async def create_clone_files(self, callback: types.CallbackQuery):
        """Create clone files for separate deployment"""
        if callback.from_user.id != self.config.owner_id:
            return
        
        # Parse data: clone_files_token
        parts = callback.data.split('_', 2)
        if len(parts) != 3:
            await callback.answer("Ошибка в данных")
            return
        
        new_token = parts[2]
        
        progress_msg = await callback.message.edit_text("🔄 Создание файлов клона...")
        
        try:
            # Verify the new token
            test_bot = Bot(token=new_token)
            bot_info = await test_bot.get_me()
            await test_bot.session.close()
            
            # Create clone directory name
            clone_dir = f"bot_clone_{bot_info.username}"
            
            # Check if clone already exists
            current_dir = os.path.dirname(os.path.abspath(__file__))
            clone_path = os.path.join(os.path.dirname(current_dir), clone_dir)
            
            if os.path.exists(clone_path):
                kb = InlineKeyboardBuilder()
                kb.button(text="Да, перезаписать", callback_data=f"overwrite_clone_{clone_dir}_{new_token}")
                kb.button(text="Отмена", callback_data="back_to_main")
                kb.adjust(2)
                
                await progress_msg.edit_text(
                    f"⚠️ Клон бота уже существует в папке: {clone_dir}\n\n"
                    "Перезаписать существующий клон?",
                    reply_markup=kb.as_markup()
                )
                return
            
            # Create clone files
            await self._perform_bot_clone(new_token, clone_dir, progress_msg)
            
        except Exception as e:
            kb = InlineKeyboardBuilder()
            kb.button(text="Назад", callback_data="back_to_main")
            
            await progress_msg.edit_text(
                f"❌ Ошибка при создании файлов клона: {e}",
                reply_markup=kb.as_markup()
            )
            logger.error(f"Failed to create clone files: {e}")
        
        await callback.answer()
    async def clone_bot_inline(self, callback: types.CallbackQuery):
        """Run cloned bot in the same solution"""
        if callback.from_user.id != self.config.owner_id:
            return
        
        # Parse data: clone_inline_token
        parts = callback.data.split('_', 2)
        if len(parts) != 3:
            await callback.answer("Ошибка в данных")
            return
        
        new_token = parts[2]
        
        await callback.message.edit_text("🚀 Запускаю клон бота...")
        
        try:
            # Verify the token
            test_bot = Bot(token=new_token)
            bot_info = await test_bot.get_me()
            await test_bot.session.close()
            
            bot_id = f"bot_{bot_info.username}"
            
            # Check if this bot is already running
            if bot_id in self.bot_manager.processes:
                if self.bot_manager.processes[bot_id].is_alive():
                    kb = InlineKeyboardBuilder()
                    kb.button(text="Остановить", callback_data=f"stop_clone_{bot_id}")
                    kb.button(text="Назад", callback_data="manage_clones")
                    kb.adjust(2)
                    
                    await callback.message.edit_text(
                        f"⚠️ Бот @{bot_info.username} уже запущен!",
                        reply_markup=kb.as_markup()
                    )
                    await callback.answer()
                    return
            
            # Create a new process for the bot
            process = Process(
                target=run_bot_process,
                args=(new_token, self.config.owner_id, self.config.source_channels, bot_id),
                name=bot_id
            )
            
            process.start()
            self.bot_manager.add_bot(bot_id, process)
            self.child_bots.append(bot_id)
            
            kb = InlineKeyboardBuilder()
            kb.button(text="Управление клонами", callback_data="manage_clones")
            kb.button(text="Назад", callback_data="back_to_main")
            kb.adjust(2)
            
            await callback.message.edit_text(
                f"✅ Бот @{bot_info.username} успешно запущен!\n\n"
                f"ID процесса: {process.pid}\n"
                f"Статус: Работает\n\n"
                "Бот работает в отдельном процессе и будет пересылать сообщения.",
                reply_markup=kb.as_markup()
            )
            
        except Exception as e:
            kb = InlineKeyboardBuilder()
            kb.button(text="Назад", callback_data="back_to_main")
            
            await callback.message.edit_text(
                f"❌ Ошибка при запуске клона: {e}",
                reply_markup=kb.as_markup()
            )
            logger.error(f"Failed to start clone bot: {e}")
        
        await callback.answer()

    async def manage_clones(self, callback: types.CallbackQuery):
        """Manage running bot clones"""
        if callback.from_user.id != self.config.owner_id:
            return
        
        bots = self.bot_manager.list_bots()
        
        # Count clones (excluding main bot)
        clone_count = len([b for b in bots if b != "main"])
        
        if clone_count == 0:
            kb = InlineKeyboardBuilder()
            kb.button(text="Добавить клон", callback_data="clone_bot")
            kb.button(text="Назад", callback_data="back_to_main")
            kb.adjust(2)
            
            await callback.message.edit_text(
                "📋 Нет запущенных клонов.\n\n"
                "Добавьте новый клон для управления несколькими ботами.",
                reply_markup=kb.as_markup()
            )
        else:
            text = "🤖 Запущенные боты:\n\n"
            kb = InlineKeyboardBuilder()
            
            # Show main bot info first
            main_info = bots.get("main", {})
            text += f"• Основной бот\n  Статус: 🟢 Работает\n  PID: {main_info.get('pid', 'N/A')}\n\n"
            
            # Show clones
            for bot_id, info in bots.items():
                if bot_id == "main":
                    continue
                    
                # Check if process is alive
                process = self.bot_manager.processes.get(bot_id)
                if process and process.is_alive():
                    status = "🟢 Работает"
                else:
                    status = "🔴 Остановлен"
                
                # Extract bot username from bot_id
                bot_username = bot_id.replace("bot_", "@")
                text += f"• {bot_username}\n  Статус: {status}\n  PID: {info.get('pid', 'N/A')}\n  Запущен: {info.get('started_at', 'Неизвестно')}\n\n"
                
                if status == "🟢 Работает":
                    kb.button(text=f"Остановить {bot_username}", callback_data=f"stop_clone_{bot_id}")
                else:
                    kb.button(text=f"Запустить {bot_username}", callback_data=f"start_clone_{bot_id}")
            
            kb.button(text="Добавить клон", callback_data="clone_bot")
            kb.button(text="Назад", callback_data="back_to_main")
            kb.adjust(1)
            
            await callback.message.edit_text(text, reply_markup=kb.as_markup())
        
        await callback.answer()

    async def stop_clone(self, callback: types.CallbackQuery):
        """Stop a running bot clone"""
        if callback.from_user.id != self.config.owner_id:
            return
        
        bot_id = callback.data.replace("stop_clone_", "")
        
        try:
            self.bot_manager.remove_bot(bot_id)
            await callback.answer(f"Бот {bot_id} остановлен")
        except Exception as e:
            await callback.answer(f"Ошибка при остановке: {e}")
        
        await self.manage_clones(callback)

    # Update the clone_bot_submit method to provide inline option
    async def clone_bot_submit(self, message: types.Message):
        """Handler for new bot token submission"""
        if message.from_user.id != self.config.owner_id:
            return
        
        if not hasattr(self, 'awaiting_clone_token') or self.awaiting_clone_token != message.from_user.id:
            return
        
        new_token = message.text.strip()
        
        if not new_token or ':' not in new_token:
            await message.reply("⚠️ Неверный формат токена.")
            return
        
        self.awaiting_clone_token = None
        
        # Verify the token
        try:
            test_bot = Bot(token=new_token)
            bot_info = await test_bot.get_me()
            await test_bot.session.close()
            
            kb = InlineKeyboardBuilder()
            kb.button(text="🚀 Запустить сейчас", callback_data=f"clone_inline_{new_token}")
            kb.button(text="💾 Создать файлы", callback_data=f"clone_files_{new_token}")
            kb.button(text="Отмена", callback_data="back_to_main")
            kb.adjust(2)
            
            await message.reply(
                f"✅ Токен проверен!\n"
                f"Бот: @{bot_info.username}\n\n"
                "Выберите действие:",
                reply_markup=kb.as_markup()
            )
            
        except Exception as e:
            await message.reply(f"❌ Ошибка проверки токена: {e}")

    # Add cleanup method to stop all child bots on shutdown
    async def cleanup(self):
        """Stop all child bots"""
        for bot_id in self.child_bots:
            try:
                self.bot_manager.remove_bot(bot_id)
            except Exception as e:
                logger.error(f"Error stopping bot {bot_id}: {e}")

    async def clone_bot_prompt(self, callback: types.CallbackQuery):
        """Prompt for cloning the bot"""
        if callback.from_user.id != self.config.owner_id:
            return
        
        # Set state to wait for new token
        self.awaiting_clone_token = callback.from_user.id
        
        kb = InlineKeyboardBuilder()
        kb.button(text="Отмена", callback_data="back_to_main")
        
        await callback.message.edit_text(
            "🤖 Клонирование бота\n\n"
            "1. Создайте нового бота через @BotFather\n"
            "2. Получите новый токен бота\n"
            "3. Отправьте токен сюда\n\n"
            "После проверки токена вы сможете выбрать:\n"
            "• Запустить клон в текущем процессе\n"
            "• Создать файлы для отдельного запуска\n\n"
            "Отправьте новый токен сообщением 💬",
            reply_markup=kb.as_markup()
        )
        await callback.answer()

    # Let's also add the overwrite_clone method that was referenced earlier
    async def overwrite_clone(self, callback: types.CallbackQuery):
        """Handler for overwriting existing clone"""
        if callback.from_user.id != self.config.owner_id:
            return
        
        # Parse data: overwrite_clone_dirname_token
        parts = callback.data.split('_', 3)
        if len(parts) != 4:
            await callback.answer("Ошибка в данных")
            return
        
        clone_dir = parts[2]
        new_token = parts[3]
        
        # Delete existing clone
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        clone_path = os.path.join(os.path.dirname(current_dir), clone_dir)
        
        if os.path.exists(clone_path):
            shutil.rmtree(clone_path)
        
        # Perform clone
        await self._perform_bot_clone(new_token, clone_dir, callback.message)
        await callback.answer()
    def is_admin(self, user_id: int) -> bool:
        """Проверка, является ли пользователь администратором"""
        return self.config.is_admin(user_id)
        
    def _setup_handlers(self):
        """Инициализация обработчиков сообщений с паттерном Command"""
        # Обработчики команд администратора
        commands = {
            "start": StartCommand(
                isinstance(self.context.state, RunningState)
            ),
            "help": HelpCommand(),
            "setlast": SetLastMessageCommand(self.bot),
            "getlast": GetLastMessageCommand(),
            "forwardnow": ForwardNowCommand(self.context),
            "test": TestMessageCommand(self.bot),
            "findlast": FindLastMessageCommand(self.bot)
        }
        
        for cmd_name, cmd_handler in commands.items():
            self.dp.message.register(cmd_handler.execute, Command(cmd_name))
    
        # Регистрация обработчика для ввода канала
        self.dp.message.register(
            self.add_channel_submit,
            lambda message: message.from_user.id == self.awaiting_channel_input
        )
        
        # Регистрация обработчика для ввода интервала вручную
        self.dp.message.register(
            self.set_interval_submit,
            lambda message: message.from_user.id == self.awaiting_interval_input
        )
        
        # Регистрация прямой команды для добавления канала
        self.dp.message.register(
            self.add_channel_prompt,
            Command("addchannel")
        )
        
        # Обработчик для постов в канале
        self.dp.channel_post.register(self.handle_channel_post)
        
        self.dp.message.register(
            self.clone_bot_submit,
            lambda message: hasattr(self, 'awaiting_clone_token') and 
                          self.awaiting_clone_token == message.from_user.id
        )
        # Обработчики callback-запросов
        callbacks = {
            "toggle_forward": self.toggle_forwarding,
            "set_interval": self.set_interval_prompt,
            "set_interval_value_": self.set_interval_value,
            "add_channel_input": self.add_channel_input,
            "remove_channel_": self.remove_channel,
            "remove_": self.remove_chat,
            "list_chats": self.list_chats,
            "back_to_main": self.main_menu,
            "channels": self.manage_channels,
            "add_channel": self.add_channel_prompt,
            "forward_now": self.forward_now_handler,
            "test_pin": self.test_pin_handler,
            "clone_bot": self.clone_bot_prompt,
            "clone_inline_": self.clone_bot_inline,
            "overwrite_clone_": self.overwrite_clone,
            "clone_files_": self.create_clone_files,
            "stop_clone_": self.stop_clone,
            "manage_clones": self.manage_clones

        }
        
        # Регистрируем обработчики с определенным порядком, чтобы избежать конфликтов
        for prefix, handler in callbacks.items():
            self.dp.callback_query.register(
                handler,
                lambda c, p=prefix: c.data.startswith(p)
            )
        
        # Обработчик для добавления бота в чаты
        self.dp.my_chat_member.register(self.handle_chat_member)

    async def find_latest_message(self, channel_id: str) -> Optional[int]:
        """Вспомогательный метод для поиска последнего действительного сообщения в канале"""
        try:
            # Начинаем с разумно высокого числа и идем назад
            max_id = 10000
            
            for test_id in range(max_id, 0, -1):
                try:
                    # Пытаемся получить информацию о сообщении
                    msg = await self.bot.get_messages(channel_id, message_ids=test_id)
                    if msg and not getattr(msg, 'empty', False):
                        return test_id
                except Exception:
                    # Пропускаем ошибки для несуществующих сообщений
                    pass
                    
                # Делаем паузу каждые 1000 проверок, чтобы не превысить лимиты API
                if test_id % 1000 == 0:
                    await asyncio.sleep(0.5)
                    
                # Не проверяем слишком много ID
                if max_id - test_id > 5000:
                    break
                    
            return None
        except Exception as e:
            logger.error(f"Ошибка при поиске последнего сообщения в канале {channel_id}: {e}")
            return None

    async def forward_now_handler(self, callback: types.CallbackQuery):
        """Обработчик немедленной пересылки последнего сообщения"""
        if not self.is_admin(callback.from_user.id):
            return
            
        await callback.message.edit_text(
            "🔄 Выполняю немедленную пересылку последнего сообщения из всех каналов...",
            reply_markup=None
        )
        
        success = await self.context.forward_latest_messages()
        
        if success:
            await callback.message.edit_text(
                "✅ Сообщения успешно пересланы и закреплены",
                reply_markup=KeyboardFactory.create_main_keyboard(
                    isinstance(self.context.state, RunningState),
                )
            )
        else:
            await callback.message.edit_text(
                "⚠️ Произошла ошибка при пересылке сообщений. Проверьте логи.",
                reply_markup=KeyboardFactory.create_main_keyboard(
                    isinstance(self.context.state, RunningState),
                )
            )
            
        await callback.answer()

    async def test_pin_handler(self, callback: types.CallbackQuery):
        """Обработчик для тестирования функции закрепления сообщений"""
        if not self.is_admin(callback.from_user.id):
            return
            
        # Получаем ID чата из callback_data (формат test_pin_CHAT_ID)
        parts = callback.data.split('_')
        if len(parts) != 3:
            await callback.answer("Неверный формат данных")
            return
            
        chat_id = int(parts[2])
        
        try:
            # Отправляем тестовое сообщение в чат
            test_message = await self.bot.send_message(
                chat_id,
                "🔄 Это тестовое сообщение для проверки функции закрепления. Бот попытается закрепить его."
            )
            
            # Пробуем закрепить сообщение
            await self.bot.pin_chat_message(
                chat_id=chat_id,
                message_id=test_message.message_id,
                disable_notification=True
            )
            
            # Сохраняем ID закрепленного сообщения
            self.pinned_messages[str(chat_id)] = test_message.message_id
            await Repository.save_pinned_message(str(chat_id), test_message.message_id)
            
            await callback.message.edit_text(
                f"✅ Тестовое сообщение успешно отправлено и закреплено в чате {chat_id}",
                reply_markup=KeyboardFactory.create_chat_list_keyboard(
                    await self._get_chat_info()
                )
            )
            
            # Через 5 секунд открепляем сообщение для завершения теста
            await asyncio.sleep(5)
            
            await self.bot.unpin_chat_message(
                chat_id=chat_id,
                message_id=test_message.message_id
            )
            
        except Exception as e:
            await callback.message.edit_text(
                f"❌ Ошибка при тестировании закрепления: {e}\n\n"
                f"Проверьте, что бот имеет права администратора в чате {chat_id} с возможностью закреплять сообщения.",
                reply_markup=KeyboardFactory.create_chat_list_keyboard(
                    await self._get_chat_info()
                )
            )
            
        await callback.answer()

    async def toggle_forwarding(self, callback: types.CallbackQuery):
        """Обработчик для кнопки включения/выключения пересылки"""
        if not self.is_admin(callback.from_user.id):
            return

        if isinstance(self.context.state, IdleState):
            interval = await Repository.get_config("rotation_interval", "7200")  # 2 часа по умолчанию
            await callback.message.edit_text(
                f"🔄 Запуск ротации с интервалом {int(interval)//60} минут..."
            )
            await self.context.start()
        else:
            await self.context.stop()

        await callback.message.edit_text(
            f"Пересылка {'запущена' if isinstance(self.context.state, RunningState) else 'остановлена'}!",
            reply_markup=KeyboardFactory.create_main_keyboard(
                isinstance(self.context.state, RunningState),
            )
        )
        await callback.answer()

    async def set_interval_prompt(self, callback: types.CallbackQuery):
        """Окно установки интервала ротации каналов"""
        if not self.is_admin(callback.from_user.id):
            return
            
        current_interval = await Repository.get_config("rotation_interval", "7200")  # 2 часа по умолчанию
        current_minutes = int(current_interval) // 60
        
        try:
            await callback.message.edit_text(
                f"⏱️ Интервал ротации каналов\n\n"
                f"Текущий интервал: {current_minutes} мин.\n\n"
                f"Выберите новый интервал ротации между каналами.\n"
                f"Это время, через которое бот будет переключаться между каналами.",
                reply_markup=KeyboardFactory.create_rotation_interval_keyboard()
            )
        except Exception as e:
            # Обрабатываем ошибку при обновлении сообщения
            # Если это ошибка "message is not modified", просто игнорируем её
            if "message is not modified" not in str(e):
                logger.error(f"Ошибка при обновлении сообщения: {e}")
        
        await callback.answer()

    async def set_interval_value(self, callback: types.CallbackQuery):
        """Установка конкретного значения интервала"""
        if not self.is_admin(callback.from_user.id):
            return
            
        parts = callback.data.split('_')
        if len(parts) != 3:
            # Если это запрос на ручной ввод интервала
            if callback.data == "set_interval_value_custom":
                self.awaiting_interval_input = callback.from_user.id
                
                kb = InlineKeyboardBuilder()
                kb.button(text="Отмена", callback_data="set_interval")
                
                await callback.message.edit_text(
                    "⏱️ Введите интервал ротации в минутах (от 5 до 1440):",
                    reply_markup=kb.as_markup()
                )
                await callback.answer()
                return
                
            await callback.answer("Неверный формат данных")
            return
            
        try:
            # Значение в секундах
            interval = int(parts[2])
            
            await Repository.set_config("rotation_interval", str(interval))
            
            # Если бот работает, обновляем интервал
            if isinstance(self.context.state, RunningState):
                self.context.state.update_interval(interval)
                
            # Форматируем интервал для отображения
            if interval >= 3600:
                hours = interval // 3600
                minutes = (interval % 3600) // 60
                display = f"{hours}ч"
                if minutes > 0:
                    display += f" {minutes}м"
            else:
                display = f"{interval // 60}м"
                
            await callback.message.edit_text(
                f"✅ Интервал ротации установлен на {display}",
                reply_markup=KeyboardFactory.create_main_keyboard(
                    isinstance(self.context.state, RunningState),
                )
            )
            
            logger.info(f"Установлен интервал ротации {interval} секунд ({interval//60} минут)")
            
        except ValueError:
            await callback.answer("Ошибка при установке интервала")
        
        await callback.answer()

    async def set_interval_submit(self, message: types.Message):
        """Обработчик ручного ввода интервала"""
        if not self.is_admin(message.from_user.id) or message.from_user.id != self.awaiting_interval_input:
            return
            
        # Сбрасываем состояние ожидания
        self.awaiting_interval_input = None
        
        try:
            # Конвертируем ввод в минуты
            minutes = int(message.text.strip())
            
            if minutes < 5:
                await message.reply("⚠️ Интервал должен быть не менее 5 минут")
                return
                
            if minutes > 1440:  # 24 часа
                await message.reply("⚠️ Интервал не должен превышать 24 часа (1440 минут)")
                return
                
            # Конвертируем в секунды для сохранения
            interval = minutes * 60
            
            await Repository.set_config("rotation_interval", str(interval))
            
            # Если бот работает, обновляем интервал
            if isinstance(self.context.state, RunningState):
                self.context.state.update_interval(interval)
                
            # Форматируем интервал для отображения
            if interval >= 3600:
                hours = interval // 3600
                remaining_minutes = (interval % 3600) // 60
                display = f"{hours}ч"
                if remaining_minutes > 0:
                    display += f" {remaining_minutes}м"
            else:
                display = f"{minutes}м"
                
            kb = InlineKeyboardBuilder()
            kb.button(text="Главное меню", callback_data="back_to_main")
                
            await message.reply(
                f"✅ Интервал ротации установлен на {display}",
                reply_markup=kb.as_markup()
            )
            
            logger.info(f"Установлен интервал ротации {interval} секунд ({minutes} минут)")
            
        except ValueError:
            await message.reply(
                "❌ Ошибка: введите целое число минут\n"
                "Например: 60 для интервала в 1 час"
            )

    async def _get_chat_info(self) -> Dict[int, str]:
        """Получение информации о чатах для отображения в меню"""
        chats = await Repository.get_target_chats()
        chat_info = {}
        
        for chat_id in chats:
            info = await self.cache_service.get_chat_info(self.bot, chat_id)
            if info:
                chat_info[chat_id] = info.title
                
        return chat_info

    async def add_channel_prompt(self, callback: types.CallbackQuery):
        """Улучшенное приглашение для добавления канала"""
        if not self.is_admin(callback.from_user.id):
            return
        
        # Создаем клавиатуру с кнопками для распространенных типов каналов
        kb = InlineKeyboardBuilder()
        kb.button(text="🔄 Ввести ID или username канала", callback_data="add_channel_input")
        kb.button(text="Назад", callback_data="channels")
        kb.adjust(1)
        
        await callback.message.edit_text(
            "Выберите способ добавления канала:\n\n"
            "• Вы можете ввести ID канала (начинается с -100...)\n"
            "• Или username канала (без @)\n\n"
            "Бот должен быть администратором в канале.",
            reply_markup=kb.as_markup()
        )
        await callback.answer()

    async def add_channel_input(self, callback: types.CallbackQuery):
        """Обработчик ввода ID/username канала"""
        if not self.is_admin(callback.from_user.id):
            return
        
        self.awaiting_channel_input = callback.from_user.id
        
        kb = InlineKeyboardBuilder()
        kb.button(text="Отмена", callback_data="channels")
        
        await callback.message.edit_text(
            "Пожалуйста, введите ID канала или username для добавления:\n\n"
            "• Для публичных каналов: введите username без @\n"
            "• Для приватных каналов: введите ID канала (начинается с -100...)\n\n"
            "Отправьте ID/username сообщением 💬",
            reply_markup=kb.as_markup()
        )
        await callback.answer()

    async def add_channel_submit(self, message: types.Message):
        """Обработчик сообщения с прямым вводом канала"""
        if not self.is_admin(message.from_user.id):
            return
        
        channel = message.text.strip()
        
        if not channel:
            await message.reply("⚠️ ID/username канала не может быть пустым")
            return
        
        self.awaiting_channel_input = None
        
        progress_msg = await message.reply("🔄 Проверяю доступ к каналу...")
        
        try:
            chat = await self.bot.get_chat(channel)
            
            bot_id = (await self.bot.get_me()).id
            member = await self.bot.get_chat_member(chat.id, bot_id)
            
            if member.status != "administrator":
                kb = InlineKeyboardBuilder()
                kb.button(text="Назад к каналам", callback_data="channels")
                
                await progress_msg.edit_text(
                    "⚠️ Бот должен быть администратором канала.\n"
                    "Пожалуйста, добавьте бота как администратора и попробуйте снова.",
                    reply_markup=kb.as_markup()
                )
                return
            
            if self.config.add_source_channel(str(chat.id)):
                await progress_msg.edit_text(f"✅ Добавлен канал: {chat.title} ({chat.id})\n\n🔍 Теперь ищу последнее сообщение...")
                
                try:
                    latest_id = await self.find_latest_message(str(chat.id))
                    
                    if latest_id:
                        await Repository.save_last_message(str(chat.id), latest_id)
                        
                        kb = InlineKeyboardBuilder()
                        kb.button(text="Назад к каналам", callback_data="channels")
                        
                        await progress_msg.edit_text(
                            f"✅ Добавлен канал: {chat.title} ({chat.id})\n"
                            f"✅ Найдено и сохранено последнее сообщение (ID: {latest_id})",
                            reply_markup=kb.as_markup()
                        )
                    else:
                        kb = InlineKeyboardBuilder()
                        kb.button(text="Назад к каналам", callback_data="channels")
                        
                        await progress_msg.edit_text(
                            f"✅ Добавлен канал: {chat.title} ({chat.id})\n"
                            f"⚠️ Не удалось найти валидные сообщения. Будет использоваться следующее сообщение в канале.",
                            reply_markup=kb.as_markup()
                        )
                except Exception as e:
                    logger.error(f"Error finding latest message: {e}")
                    
                    kb = InlineKeyboardBuilder()
                    kb.button(text="Назад к каналам", callback_data="channels")
                    
                    await progress_msg.edit_text(
                        f"✅ Добавлен канал: {chat.title} ({chat.id})\n"
                        f"⚠️ Ошибка при поиске последнего сообщения.",
                        reply_markup=kb.as_markup()
                    )
            else:
                kb = InlineKeyboardBuilder()
                kb.button(text="Назад к каналам", callback_data="channels")
                
                await progress_msg.edit_text(
                    f"⚠️ Канал {chat.title} уже настроен.",
                    reply_markup=kb.as_markup()
                )
        except Exception as e:
            kb = InlineKeyboardBuilder()
            kb.button(text="Назад к каналам", callback_data="channels")
            
            await progress_msg.edit_text(
                f"❌ Ошибка доступа к каналу: {e}\n\n"
                "Убедитесь что:\n"
                "• ID/username канала указан правильно\n"
                "• Бот является участником канала\n"
                "• Бот является администратором канала",
                reply_markup=kb.as_markup()
            )
            logger.error(f"Failed to add channel {channel}: {e}")

    async def remove_chat(self, callback: types.CallbackQuery):
        """Обработчик удаления чата"""
        if not self.is_admin(callback.from_user.id):
            return
        
        # Проверяем, что это удаление чата, а не канала
        if not callback.data.startswith("remove_") or callback.data.startswith("remove_channel_"):
            await callback.answer("Эта команда только для удаления чатов")
            return
        
        try:
            chat_id = int(callback.data.split("_")[1])
            
            # Перед удалением пробуем открепить сообщение, если оно есть
            try:
                pinned_message_id = await Repository.get_pinned_message(str(chat_id))
                if pinned_message_id:
                    try:
                        await self.bot.unpin_chat_message(
                            chat_id=chat_id,
                            message_id=pinned_message_id
                        )
                    except Exception as e:
                        logger.warning(f"Не удалось открепить сообщение {pinned_message_id} в чате {chat_id}: {e}")
            except Exception as e:
                logger.warning(f"Ошибка при получении закрепленного сообщения для чата {chat_id}: {e}")
            
            await Repository.remove_target_chat(chat_id)
            await Repository.delete_pinned_message(str(chat_id))
            self.cache_service.remove_from_cache(chat_id)
            
            # Удаляем запись из словаря закрепленных сообщений
            if str(chat_id) in self.pinned_messages:
                del self.pinned_messages[str(chat_id)]
            
            await self.list_chats(callback)
            await callback.answer("Чат удален!")
        except ValueError:
            await callback.answer("Ошибка при удалении чата")
            logger.error(f"Invalid chat_id in callback data: {callback.data}")

    async def list_chats(self, callback: types.CallbackQuery):
        """Обработчик списка чатов"""
        if not self.is_admin(callback.from_user.id):
            return
        
        chats = await Repository.get_target_chats()
        chat_info = {}
        
        for chat_id in chats:
            info = await self.cache_service.get_chat_info(self.bot, chat_id)
            if info:
                chat_info[chat_id] = info.title
        
        if not chats:
            text = (
                "Нет настроенных целевых чатов.\n"
                "Убедитесь, что:\n"
                "1. Бот добавлен в целевые чаты\n"
                "2. Бот является администратором в исходных каналах"
            )
            markup = KeyboardFactory.create_main_keyboard(
                isinstance(self.context.state, RunningState),
            )
        else:
            text = "📡 Целевые чаты:\n\n"
            
            # Получаем информацию о закрепленных сообщениях
            pinned_messages = await Repository.get_all_pinned_messages()
            
            for chat_id, title in chat_info.items():
                has_pinned = str(chat_id) in pinned_messages
                pin_status = "📌" if has_pinned else "🔴"
                text += f"{pin_status} {title} ({chat_id})\n"
                
            text += "\n📌 - есть закрепленное сообщение\n🔴 - нет закрепленного сообщения"
            
            markup = KeyboardFactory.create_chat_list_keyboard(chat_info)
        
        await callback.message.edit_text(text, reply_markup=markup)
        await callback.answer()

    async def main_menu(self, callback: types.CallbackQuery):
        """Обработчик кнопки главного меню"""
        if not self.is_admin(callback.from_user.id):
            return
        
        await callback.message.edit_text(
            "Главное меню:",
            reply_markup=KeyboardFactory.create_main_keyboard(
                isinstance(self.context.state, RunningState),
            )
        )
        await callback.answer()

    async def manage_channels(self, callback: types.CallbackQuery):
        """Меню управления каналами"""
        if not self.is_admin(callback.from_user.id):
            return
                
        # Сбрасываем состояние ввода канала
        self.awaiting_channel_input = None
        
        source_channels = self.config.source_channels
        
        if not source_channels:
            text = (
                "Нет настроенных исходных каналов.\n"
                "Добавьте канал, нажав кнопку ниже."
            )
        else:
            text = "📡 Исходные каналы:\n\n"
            
            # Получаем последние сообщения для каждого канала
            last_messages = await Repository.get_all_last_messages()
            
            for channel in source_channels:
                # Пытаемся получить информацию о канале для лучшего отображения
                try:
                    chat = await self.bot.get_chat(channel)
                    channel_title = chat.title or channel
                    
                    # Получаем ID последнего сообщения, если оно есть
                    last_msg = "Нет данных"
                    if channel in last_messages:
                        last_msg = f"ID: {last_messages[channel]['message_id']}"
                        
                    text += f"• {channel_title} ({channel})\n  Последнее сообщение: {last_msg}\n\n"
                except Exception:
                    # Если не удалось получить информацию, просто показываем ID
                    last_msg = "Нет данных"
                    if channel in last_messages:
                        last_msg = f"ID: {last_messages[channel]['message_id']}"
                        
                    text += f"• {channel}\n  Последнее сообщение: {last_msg}\n\n"
        
        # Используем KeyboardFactory для создания клавиатуры управления
        markup = KeyboardFactory.create_channel_management_keyboard(source_channels)
        
        await callback.message.edit_text(text, reply_markup=markup)
        await callback.answer()

    async def remove_channel(self, callback: types.CallbackQuery):
        """Удаление исходного канала"""
        if not self.is_admin(callback.from_user.id):
            return
        
        # Извлекаем ID канала из данных callback
        if not callback.data.startswith("remove_channel_"):
            await callback.answer("Неверный формат данных")
            return
        
        channel = callback.data.replace("remove_channel_", "")
        
        if self.config.remove_source_channel(channel):
            await callback.answer("Канал успешно удален")
        else:
            await callback.answer("Не удалось удалить канал")
        
        await self.manage_channels(callback)

    async def handle_channel_post(self, message: types.Message | None):
        """Обработчик сообщений в каналах"""
        if message is None:
            return
            
        chat_id = str(message.chat.id)
        username = getattr(message.chat, 'username', None)
        source_channels = self.config.source_channels
            
        is_source = False
        for channel in source_channels:
            if channel == chat_id or (username and channel.lower() == username.lower()):
                is_source = True
                break
                
        if not is_source:
            logger.info(f"Сообщение не из канала-источника: {chat_id}/{username}")
            return
        
        # Сохраняем ID последнего сообщения
        await Repository.save_last_message(chat_id, message.message_id)
        
        if isinstance(self.context.state, RunningState) and self.context.state.auto_forward:
            # Перенаправляем новое сообщение и закрепляем его
            await self.context.forward_and_pin_message(chat_id, message.message_id)
            logger.info(f"Переслано и закреплено сообщение {message.message_id} из канала {chat_id}")
        else:
            logger.info(f"Сохранено сообщение {message.message_id} из канала {chat_id} (без автопересылки)")

    async def handle_chat_member(self, update: types.ChatMemberUpdated):
        """Обработчик добавления/удаления бота из чатов"""
        if update.new_chat_member.user.id != self.bot.id:
            return

        chat_id = update.chat.id
        is_member = update.new_chat_member.status in ['member', 'administrator']
        
        if is_member and update.chat.type in ['group', 'supergroup']:
            await Repository.add_target_chat(chat_id)
            self.cache_service.remove_from_cache(chat_id)
            await self._notify_admins(f"Бот добавлен в {update.chat.type}: {update.chat.title} ({chat_id})")
            logger.info(f"Бот добавлен в {update.chat.type}: {update.chat.title} ({chat_id})")
        elif not is_member:
            # Если бота удалили из чата, удаляем информацию о закрепленном сообщении
            await Repository.delete_pinned_message(str(chat_id))
            if str(chat_id) in self.pinned_messages:
                del self.pinned_messages[str(chat_id)]
                
            await Repository.remove_target_chat(chat_id)
            self.cache_service.remove_from_cache(chat_id)
            await self._notify_admins(f"Бот удален из чата {chat_id}")
            logger.info(f"Бот удален из чата {chat_id}")

    async def _notify_owner(self, message: str):
        """Отправка уведомления владельцу бота (для совместимости)"""
        try:
            await self.bot.send_message(self.config.owner_id, message)
        except Exception as e:
            logger.error(f"Не удалось уведомить владельца: {e}")
            
    async def _notify_admins(self, message: str):
        """Отправка уведомления всем администраторам бота"""
        for admin_id in self.config.admin_ids:
            try:
                await self.bot.send_message(admin_id, message)
            except Exception as e:
                logger.error(f"Не удалось уведомить администратора {admin_id}: {e}")

    async def on_cache_update(self, chat_id: int, info: ChatInfo):
        """Реализация метода из протокола CacheObserver"""
        # В этой реализации мы не делаем ничего при обновлении кэша
        pass

    async def start(self):
        """Запуск бота"""
        await Repository.init_db()
        
        # Восстанавливаем закрепленные сообщения из базы данных
        pinned_messages = await Repository.get_all_pinned_messages()
        self.pinned_messages = pinned_messages
        
        # Устанавливаем интервал по умолчанию, если не задан
        if not await Repository.get_config("rotation_interval"):
            await Repository.set_config("rotation_interval", "7200")  # 2 часа по умолчанию
        
        logger.info("Бот успешно запущен!")
        try:
            # Получаем ID последнего обновления, чтобы избежать дубликатов
            offset = 0
            try:
                updates = await self.bot.get_updates(limit=1, timeout=1)
                if updates:
                    offset = updates[-1].update_id + 1
            except Exception as e:
                logger.warning(f"Не удалось получить начальные обновления: {e}")

            await self.dp.start_polling(self.bot, offset=offset)
        finally:
            self.cache_service.remove_observer(self)
            await self.bot.session.close()

# Обновляем класс KeyboardFactory для работы с новым функционалом
class KeyboardFactory:
    """Реализация паттерна Factory для создания клавиатур"""
    
    @staticmethod
    def create_main_keyboard(running: bool = False) -> Any:
        """Создание клавиатуры главного меню"""
        kb = InlineKeyboardBuilder()
        kb.button(
            text="🔄 Запустить ротацию" if not running else "⏹ Остановить ротацию",
            callback_data="toggle_forward"
        )
        kb.button(text="⏱️ Установить интервал", callback_data="set_interval")
        kb.button(text="⚙️ Управление каналами", callback_data="channels")
        kb.button(text="💬 Список целевых чатов", callback_data="list_chats")
        kb.button(text="📌 Немедленная пересылка", callback_data="forward_now")
        kb.adjust(2)
        return kb.as_markup()

    @staticmethod
    def create_rotation_interval_keyboard() -> Any:
        """Создание клавиатуры выбора интервала ротации"""
        kb = InlineKeyboardBuilder()
        intervals = [
            ("30м", 1800), ("1ч", 3600), ("2ч", 7200), 
            ("3ч", 10800), ("6ч", 21600), ("12ч", 43200), 
            ("24ч", 86400)
        ]
        for label, seconds in intervals:
            kb.button(text=label, callback_data=f"set_interval_value_{seconds}")
        kb.button(text="Другой...", callback_data="set_interval_value_custom")
        kb.button(text="Назад", callback_data="back_to_main")
        kb.adjust(4)
        return kb.as_markup()

    @staticmethod
    def create_chat_list_keyboard(chats: Dict[int, str]) -> Any:
        """Создание клавиатуры списка чатов с кнопками удаления и тестирования закрепления"""
        kb = InlineKeyboardBuilder()
        for chat_id, title in chats.items():
            kb.button(
                text=f"📌 Тест закрепления в {title}",
                callback_data=f"test_pin_{chat_id}"
            )
            kb.button(
                text=f"❌ Удалить {title}",
                callback_data=f"remove_{chat_id}"
            )
        kb.button(text="Назад", callback_data="back_to_main")
        kb.adjust(1)
        return kb.as_markup()

    @staticmethod
    def create_channel_management_keyboard(channels: List[str]) -> Any:
        """Создание клавиатуры управления каналами"""
        kb = InlineKeyboardBuilder()
        kb.button(text="➕ Добавить канал", callback_data="add_channel")
        
        # Добавляем кнопки для каждого канала
        for channel in channels:
            # Обрезаем имя канала, если оно слишком длинное
            display_name = channel[:15] + "..." if len(channel) > 18 else channel
            
            # Кнопка только для удаления каналов
            kb.button(
                text=f"❌ Удалить ({display_name})",
                callback_data=f"remove_channel_{channel}"
            )
        
        kb.button(text="Назад", callback_data="back_to_main")
        kb.adjust(1)
        return kb.as_markup()

# Обновляем класс BotContext для работы с ротацией и закреплением
class BotContext:
    """Контекстный класс, управляющий состоянием бота"""
    
    def __init__(self, bot, config):
        self.bot = bot
        self.config = config
        self.state: BotState = IdleState(self)
    
    async def start(self) -> None:
        await self.state.start()
    
    async def stop(self) -> None:
        await self.state.stop()
    
    async def forward_and_pin_message(self, channel_id: str, message_id: int) -> bool:
        """Пересылает сообщение во все целевые чаты и закрепляет его"""
        success = False
        target_chats = await Repository.get_target_chats()
        
        if not target_chats:
            logger.warning("Нет целевых чатов для пересылки")
            return False

        for chat_id in target_chats:
            if str(chat_id) == channel_id:
                logger.info(f"Пропускаю пересылку в исходный канал {chat_id}")
                continue
                
            try:
                chat_info = await self.bot.get_chat(chat_id)
                
                # Проверяем, что это не канал (в каналах нельзя закреплять сообщения с помощью бота)
                if chat_info.type == 'channel':
                    logger.info(f"Пропускаю пересылку в канал {chat_id} (нельзя закреплять сообщения в каналах)")
                    continue
                
                # Пересылаем сообщение
                forwarded_message = await self.bot.forward_message(
                    chat_id=chat_id,
                    from_chat_id=channel_id,
                    message_id=message_id
                )
                
                # Открепляем предыдущее сообщение, если оно есть
                old_pinned_id = await Repository.get_pinned_message(str(chat_id))
                if old_pinned_id:
                    try:
                        await self.bot.unpin_chat_message(
                            chat_id=chat_id,
                            message_id=old_pinned_id
                        )
                        logger.info(f"Откреплено предыдущее сообщение {old_pinned_id} в чате {chat_id}")
                    except Exception as e:
                        logger.warning(f"Не удалось открепить предыдущее сообщение {old_pinned_id} в чате {chat_id}: {e}")
                
                # Закрепляем новое сообщение
                await self.bot.pin_chat_message(
                    chat_id=chat_id,
                    message_id=forwarded_message.message_id,
                    disable_notification=True
                )
                
                # Сохраняем ID закрепленного сообщения
                await Repository.save_pinned_message(str(chat_id), forwarded_message.message_id)
                
                await Repository.log_forward(message_id)
                success = True
                logger.info(f"Сообщение {message_id} переслано и закреплено в чате {chat_id}")
            except Exception as e:
                logger.error(f"Ошибка при пересылке/закреплении в {chat_id}: {e}")

        return success

    async def forward_latest_messages(self) -> bool:
        """Пересылает последние сообщения из всех каналов"""
        success = False
        source_channels = self.config.source_channels
        
        if not source_channels:
            logger.warning("Нет настроенных исходных каналов")
            return False
        
        logger.info(f"Найдено {len(source_channels)} исходных каналов")
        
        # Проверяем наличие целевых чатов
        target_chats = await Repository.get_target_chats()
        if not target_chats:
            logger.warning("Нет целевых чатов для пересылки. Бот должен быть добавлен в группы/супергруппы.")
            return False
        
        logger.info(f"Найдено {len(target_chats)} целевых чатов")
        
        for channel_id in source_channels:
            # Получаем ID последнего сообщения
            message_id = await Repository.get_last_message(channel_id)
            
            if not message_id:
                logger.warning(f"Не найдено последнее сообщение для канала {channel_id}")
                continue
            
            logger.info(f"Найдено сообщение {message_id} для канала {channel_id}")
            
            # Пересылаем и закрепляем сообщение
            result = await self.forward_and_pin_message(channel_id, message_id)
            success = success or result
            
            if result:
                logger.info(f"Успешно переслано сообщение {message_id} из канала {channel_id}")
            else:
                logger.warning(f"Не удалось переслать сообщение {message_id} из канала {channel_id}")
        
        return success
            
    async def _notify_admins(self, message: str):
        """Отправка уведомления всем администраторам бота"""
        for admin_id in self.config.admin_ids:
            try:
                await self.bot.send_message(admin_id, message)
            except Exception as e:
                logger.error(f"Не удалось уведомить администратора {admin_id}: {e}")

# Обновляем класс BotState для управления ротацией каналов
class BotState(ABC):
    """Абстрактный базовый класс для состояний бота"""
    
    @abstractmethod
    async def start(self) -> None:
        """Обработка действия запуска"""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Обработка действия остановки"""
        pass

class IdleState(BotState):
    """Состояние, когда бот не пересылает сообщения"""
    
    def __init__(self, bot_context):
        self.context = bot_context
    
    async def start(self) -> None:
        interval = int(await Repository.get_config("rotation_interval", "7200"))
        self.context.state = RunningState(self.context, interval)
        await self.context._notify_admins("Бот начал ротацию каналов")
    
    async def stop(self) -> None:
        # Уже остановлен
        pass

class RunningState(BotState):
    """Состояние, когда бот активно пересылает сообщения"""
    
    def __init__(self, bot_context, interval: int):
        self.context = bot_context
        self.interval = interval  # Интервал ротации в секундах
        self._rotation_task: Optional[asyncio.Task] = None
        self.auto_forward = True  # Всегда включено в этой реализации
        
        # Для отслеживания последнего обработанного канала
        self._last_processed_channel_index = -1
        
        # Запускаем задачу ротации
        self._start_rotation_task()
        
    def _start_rotation_task(self):
        """Запуск задачи ротации каналов"""
        if not self._rotation_task or self._rotation_task.done():
            self._rotation_task = asyncio.create_task(self._channel_rotation())
            
    def update_interval(self, new_interval: int):
        """Обновление интервала ротации"""
        logger.info(f"Обновление интервала ротации с {self.interval} на {new_interval} секунд")
        self.interval = new_interval
        
        # Перезапускаем задачу с новым интервалом
        if self._rotation_task and not self._rotation_task.done():
            logger.info("Отмена существующей задачи ротации")
            self._rotation_task.cancel()
        else:
            logger.info("Предыдущая задача ротации не найдена или уже завершена")
        
        logger.info("Запуск новой задачи ротации")
        self._start_rotation_task()
    
    async def start(self) -> None:
        # Уже запущен
        pass
    
    async def stop(self) -> None:
        if self._rotation_task and not self._rotation_task.done():
            self._rotation_task.cancel()
        
        self.context.state = IdleState(self.context)
        await self.context._notify_admins("Бот остановил ротацию каналов")
    
    async def _channel_rotation(self):
        """Задача ротации каналов по расписанию"""
        try:
            logger.info("Запущена задача ротации каналов")
            
            # Начинаем с пересылки последних сообщений из всех каналов
            success = await self.context.forward_latest_messages()
            logger.info(f"Начальная пересылка сообщений - успешно: {success}")
            
            while True:
                # Выводим сообщение о начале ожидания
                logger.info(f"Ожидание {self.interval} секунд до следующей ротации")
                
                # Ждем указанный интервал
                await asyncio.sleep(self.interval)
                
                # Получаем список каналов
                source_channels = self.context.config.source_channels
                
                if not source_channels:
                    logger.warning("Нет настроенных исходных каналов для ротации")
                    continue
                    
                logger.info(f"Доступные каналы: {source_channels}")
                
                # Выбираем следующий канал по кругу
                self._last_processed_channel_index = (self._last_processed_channel_index + 1) % len(source_channels)
                channel_id = source_channels[self._last_processed_channel_index]
                
                logger.info(f"Выбран канал для ротации: {channel_id} (индекс: {self._last_processed_channel_index})")
                
                # Получаем ID последнего сообщения в этом канале
                message_id = await Repository.get_last_message(channel_id)
                
                if not message_id:
                    logger.warning(f"Не найдено последнее сообщение для канала {channel_id}")
                    continue
                    
                logger.info(f"Найдено сообщение для пересылки: {message_id}")
                
                # Пересылаем и закрепляем сообщение
                success = await self.context.forward_and_pin_message(channel_id, message_id)
                
                if success:
                    logger.info(f"Успешно переслано сообщение из канала {channel_id}")
                    logger.info(f"Следующая ротация через {self.interval // 60} минут")
                else:
                    logger.error(f"Не удалось переслать сообщение из канала {channel_id}")
                    
        except asyncio.CancelledError:
            logger.info("Задача ротации каналов отменена")
        except Exception as e:
            logger.error(f"Ошибка в задаче ротации каналов: {e}")
            # Добавляем полный traceback для отладки
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Перезапускаем задачу в случае неожиданной ошибки
            await asyncio.sleep(10)
            self._start_rotation_task()

# Обновляем класс Repository для работы с закрепленными сообщениями
class Repository:
    """Реализация паттерна Repository для работы с базой данных"""
    
    @staticmethod
    async def close_db() -> None:
        """Закрываем все соединения с базой данных"""
        await DatabaseConnectionPool.close_all()
    
    @staticmethod
    async def init_db() -> None:
        """Инициализация схемы базы данных"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
                CREATE TABLE IF NOT EXISTS target_chats (
                    chat_id INTEGER PRIMARY KEY,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS forward_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS last_messages (
                    channel_id TEXT PRIMARY KEY,
                    message_id INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS pinned_messages (
                    chat_id TEXT PRIMARY KEY,
                    message_id INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_forward_stats_timestamp ON forward_stats(timestamp);
                CREATE INDEX IF NOT EXISTS idx_target_chats_added_at ON target_chats(added_at);
            """)
            await db.commit()

    @staticmethod
    async def get_target_chats() -> List[int]:
        """Получение списка ID целевых чатов"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute("SELECT chat_id FROM target_chats") as cursor:
                return [row[0] for row in await cursor.fetchall()]

    @staticmethod
    async def add_target_chat(chat_id: int) -> None:
        """Добавление нового целевого чата"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "INSERT OR IGNORE INTO target_chats (chat_id) VALUES (?)",
                (chat_id,)
            )
            await db.commit()

    @staticmethod
    async def remove_target_chat(chat_id: int) -> None:
        """Удаление целевого чата"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "DELETE FROM target_chats WHERE chat_id = ?",
                (chat_id,)
            )
            await db.commit()

    @staticmethod
    async def get_config(key: str, default: Optional[str] = None) -> Optional[str]:
        """Получение значения конфигурации"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT value FROM config WHERE key = ?",
                (key,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else default

    @staticmethod
    async def set_config(key: str, value: str) -> None:
        """Установка значения конфигурации"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                (key, str(value))
            )
            await db.commit()

    @staticmethod
    async def log_forward(message_id: int) -> None:
        """Логирование пересланного сообщения"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "INSERT INTO forward_stats (message_id) VALUES (?)",
                (message_id,)
            )
            await db.commit()

    @staticmethod
    async def save_last_message(channel_id: str, message_id: int) -> None:
        """Сохранение ID последнего сообщения для канала"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO last_messages 
                (channel_id, message_id, timestamp) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
                """,
                (channel_id, message_id)
            )
            await db.commit()

    @staticmethod
    async def get_last_message(channel_id: str) -> Optional[int]:
        """Получение ID последнего сообщения для канала"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT message_id FROM last_messages WHERE channel_id = ?",
                (channel_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    @staticmethod
    async def get_all_last_messages() -> Dict[str, Dict[str, Any]]:
        """Получение ID последних сообщений для всех каналов"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT channel_id, message_id, timestamp FROM last_messages"
            ) as cursor:
                results = await cursor.fetchall()
                return {row[0]: {"message_id": row[1], "timestamp": row[2]} for row in results}

    @staticmethod
    async def get_latest_message() -> tuple:
        """Получение самого последнего сообщения из всех каналов"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT channel_id, message_id, timestamp FROM last_messages ORDER BY timestamp DESC LIMIT 1"
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return (row[0], row[1])  # (channel_id, message_id)
                return (None, None)

    @staticmethod
    async def save_pinned_message(chat_id: str, message_id: int) -> None:
        """Сохранение ID закрепленного сообщения для чата"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO pinned_messages 
                (chat_id, message_id, timestamp) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
                """,
                (chat_id, message_id)
            )
            await db.commit()

    @staticmethod
    async def get_pinned_message(chat_id: str) -> Optional[int]:
        """Получение ID закрепленного сообщения для чата"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT message_id FROM pinned_messages WHERE chat_id = ?",
                (chat_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    @staticmethod
    async def get_all_pinned_messages() -> Dict[str, int]:
        """Получение всех закрепленных сообщений"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT chat_id, message_id FROM pinned_messages"
            ) as cursor:
                results = await cursor.fetchall()
                return {row[0]: row[1] for row in results}

    @staticmethod
    async def delete_pinned_message(chat_id: str) -> None:
        """Удаление записи о закрепленном сообщении"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "DELETE FROM pinned_messages WHERE chat_id = ?",
                (chat_id,)
            )
            await db.commit()

# Update the main function to handle cleanup
async def main():
    """Main entry point with improved error handling"""
    lock_file = "bot.lock"
    bot = None
    
    if os.path.exists(lock_file):
        try:
            with open(lock_file, 'r') as f:
                pid = int(f.read().strip())
            
            import psutil
            if psutil.pid_exists(pid):
                logger.error(f"Another instance is running (PID: {pid})")
                return
            os.remove(lock_file)
            logger.info("Cleaned up stale lock file")
        except Exception as e:
            logger.warning(f"Error handling lock file: {e}")
            if os.path.exists(lock_file):
                os.remove(lock_file)

    try:
        with open(lock_file, 'w') as f:
            f.write(str(os.getpid()))

        bot = ForwarderBot()
        await bot.start()
    finally:
        try:
            if bot:
                await bot.cleanup()  # Stop all child bots
            await Repository.close_db()
            if os.path.exists(lock_file):
                os.remove(lock_file)
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Main entry point with proper Windows multiprocessing support
if __name__ == "__main__":
    # Set multiprocessing start method for Windows compatibility
    if sys.platform.startswith('win'):
        multiprocessing.set_start_method('spawn', force=True)
    else:
        multiprocessing.set_start_method('spawn', force=True)  # Use spawn for all platforms for consistency
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Бот остановлен из-за ошибки: {e}")
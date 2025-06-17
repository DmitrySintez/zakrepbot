from abc import ABC, abstractmethod
import asyncio
import os
import json
import shutil
import sys
from datetime import datetime, timedelta
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
    FindLastMessageCommand,
)
from utils.message_utils import find_latest_message as find_msg

import multiprocessing
from multiprocessing import Process

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
        logger.debug(f"Listing bots. Total: {len(self.bots)}, Keys: {list(self.bots.keys())}")
        return dict(self.bots)
def run_bot_process(bot_token: str, owner_id: int, source_channels: list, bot_id: str):
    """Wrapper to run bot in a separate process"""
    # Set up logging for the subprocess
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

# Function to run a bot instance 
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
    from bot import ForwarderBot  # Adjust import as needed for zakrepbot
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
        self.bot_manager = BotManager()
        self.bot_id = "main"  # Identifier for the main bot
        self.child_bots = []  # Track spawned bots
        self.awaiting_clone_token = None  # Track if waiting for clone token
        # Словарь для хранения ID закрепленных сообщений в чатах
        self.pinned_messages = {}
        self.awaiting_custom_start_time = None
        self.awaiting_custom_end_time = None
        self.temp_schedule_data = {}
        # Add this line to create the keyboard factory
        self.keyboard_factory = KeyboardFactory()
        
        # Регистрируем себя как наблюдатель кэша
        self.cache_service.add_observer(self)
        
        # Настраиваем обработчики
        self._setup_handlers()
    # Let's also add the overwrite_clone method that was referenced earlier
    async def add_schedule_prompt(self, callback: types.CallbackQuery):
        if not self.is_admin(callback.from_user.id):
            return

        self.awaiting_channel_for_schedule = callback.from_user.id
        kb = InlineKeyboardBuilder()
        kb.button(text="Отмена", callback_data="back_to_schedule")

        await callback.message.edit_text(
            "Введите ID канала для нового слота (например, -100123456789):",
            reply_markup=kb.as_markup()
        )
        await callback.answer()

    async def add_schedule_channel_submit(self, message: types.Message):
        if not self.is_admin(message.from_user.id) or message.from_user.id != self.awaiting_channel_for_schedule:
            return

        channel_id = message.text.strip()
        # Простая валидация канала (можно улучшить)
        if not channel_id.startswith("-100"):
            await message.reply("⚠️ Неверный формат ID канала. Ожидается -100...")
            return

        self.awaiting_start_time = message.from_user.id
        self.temp_schedule = {"channel_id": channel_id}

        kb = InlineKeyboardBuilder()
        kb.button(text="Отмена", callback_data="back_to_schedule")

        await message.reply(
            "Введите время начала (HH:MM):",
            reply_markup=kb.as_markup()
        )

    async def add_schedule_start_time_submit(self, message: types.Message):
        if not self.is_admin(message.from_user.id) or message.from_user.id != self.awaiting_start_time:
            return

        start_time = message.text.strip()
        if not self._validate_time(start_time):
            await message.reply("⚠️ Неверный формат времени. Ожидается HH:MM.")
            return

        self.awaiting_end_time = message.from_user.id
        self.temp_schedule["start_time"] = start_time

        kb = InlineKeyboardBuilder()
        kb.button(text="Отмена", callback_data="back_to_schedule")

        await message.reply(
            "Введите время окончания (HH:MM):",
            reply_markup=kb.as_markup()
        )
    async def safe_edit_message(self, callback: types.CallbackQuery, new_text: str, new_markup=None):
        """Безопасное обновление сообщения с проверкой изменений"""
        try:
            current_text = callback.message.text or ""
            
            # Если текст отличается, обновляем сообщение
            if current_text.strip() != new_text.strip():
                if new_markup:
                    await callback.message.edit_text(new_text, reply_markup=new_markup)
                else:
                    await callback.message.edit_text(new_text)
            else:
                # Если текст тот же, просто отвечаем на callback
                await callback.answer()
                
        except Exception as e:
            error_text = str(e).lower()
            if "message is not modified" in error_text:
                # Это нормальная ситуация, просто отвечаем на callback
                await callback.answer()
            else:
                # Другая ошибка, логируем и отвечаем на callback
                logger.warning(f"Не удалось обновить сообщение: {e}")
                await callback.answer()
            
    async def add_schedule_end_time_submit(self, message: types.Message):
        if not self.is_admin(message.from_user.id) or message.from_user.id != self.awaiting_end_time:
            return

        end_time = message.text.strip()
        if not self._validate_time(end_time):
            await message.reply("⚠️ Неверный формат времени. Ожидается HH:MM.")
            return

        self.temp_schedule["end_time"] = end_time
        await Repository.add_schedule(
            self.temp_schedule["channel_id"],
            self.temp_schedule["start_time"],
            self.temp_schedule["end_time"]
        )

        kb = InlineKeyboardBuilder()
        kb.button(text="Назад к расписанию", callback_data="manage_schedule")

        await message.reply(
            f"✅ Слот добавлен: канал {self.temp_schedule['channel_id']}, {self.temp_schedule['start_time']} - {self.temp_schedule['end_time']}",
            reply_markup=kb.as_markup()
        )

        self.awaiting_end_time = None
        self.temp_schedule = None
    async def remove_schedule_prompt(self, callback: types.CallbackQuery):
        """Выбор слота для удаления с обработкой дублирования"""
        if not self.is_admin(callback.from_user.id):
            return

        try:
            schedules = await Repository.get_schedules()
            if not schedules:
                kb = InlineKeyboardBuilder()
                kb.button(text="◀️ Назад", callback_data="manage_schedule")
                
                text = "❌ Нет слотов для удаления."
                
                await self.safe_edit_message(callback, text, kb.as_markup())
                return

            text = "❌ Выберите слот для удаления:\n\n"
            kb = InlineKeyboardBuilder()
            
            for i, schedule in enumerate(schedules):
                channel_name = await self._get_channel_name(schedule['channel_id'])
                kb.button(
                    text=f"🗑️ {channel_name} ({schedule['start_time']} - {schedule['end_time']})",
                    callback_data=f"remove_slot_{i}"
                )
            
            kb.button(text="◀️ Назад", callback_data="manage_schedule")
            kb.adjust(1)

            await self.safe_edit_message(callback, text, kb.as_markup())
                
        except Exception as e:
            logger.error(f"Ошибка в remove_schedule_prompt: {e}")
            await callback.answer(f"Ошибка: {e}")


    async def remove_schedule_confirm(self, callback: types.CallbackQuery):
        """Подтверждение удаления слота"""
        if not self.is_admin(callback.from_user.id):
            return

        try:
            slot_index = int(callback.data.split("_")[2])
            schedules = await Repository.get_schedules()
            
            if slot_index < 0 or slot_index >= len(schedules):
                await callback.answer("❌ Неверный индекс слота")
                return

            slot = schedules[slot_index]
            await Repository.remove_schedule(slot["channel_id"], slot["start_time"], slot["end_time"])

            channel_name = await self._get_channel_name(slot["channel_id"])
            
            kb = InlineKeyboardBuilder()
            kb.button(text="✅ К расписанию", callback_data="manage_schedule")

            await callback.message.edit_text(
                f"✅ Временной слот удалён!\n\n"
                f"📺 Канал: {channel_name}\n"
                f"⏰ Время: {slot['start_time']} - {slot['end_time']}",
                reply_markup=kb.as_markup()
            )
            
        except Exception as e:
            logger.error(f"Ошибка при удалении слота: {e}")
            await callback.answer(f"❌ Ошибка: {e}")
        
        await callback.answer()
        
    def _validate_time(self, time_str: str) -> bool:
        try:
            hours, minutes = map(int, time_str.split(":"))
            return 0 <= hours < 24 and 0 <= minutes < 60
        except ValueError:
            return False
    async def _perform_bot_clone(self, new_token: str, clone_dir: str, progress_msg=None):
        """Perform the actual bot cloning"""
        try:
            # Get bot info for the new token
            test_bot = Bot(token=new_token)
            bot_info = await test_bot.get_me()
            await test_bot.session.close()
            
            # Get paths
            current_dir = os.path.dirname(os.path.abspath(__file__))
            parent_dir = os.path.dirname(current_dir)
            clone_path = os.path.join(parent_dir, clone_dir)
            
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
            
            # Create new .env file with new token - ИСПРАВЛЕНО
            env_content = f"""# Telegram Bot Token from @BotFather
    BOT_TOKEN={new_token}

    # Your Telegram user ID (get from @userinfobot)
    OWNER_ID={self.config.owner_id}

    # Admin IDs (comma separated for multiple admins)
    ADMIN_IDS={','.join(map(str, self.config.admin_ids))}

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
            
            # Create README.md for the clone - ИСПРАВЛЕНО
            readme_content = f"""# Bot Clone: @{bot_info.username}

    This is a clone of the main forwarding bot.

    ## Configuration
    - Bot Token: Configured in .env
    - Owner ID: {self.config.owner_id}
    - Admin IDs: {', '.join(map(str, self.config.admin_ids))}
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
    - All configured admins can manage this bot clone
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
                    f"Клон будет работать независимо с теми же настройками каналов и администраторами."
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
        # БЫЛО: if callback.from_user.id != self.config.owner_id:
        if not self.is_admin(callback.from_user.id):  # ИСПРАВЛЕНО
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
            parent_dir = os.path.dirname(current_dir)
            clone_path = os.path.join(parent_dir, clone_dir)
            
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
        # БЫЛО: if callback.from_user.id != self.config.owner_id:
        if not self.is_admin(callback.from_user.id):  # ИСПРАВЛЕНО
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
            if hasattr(self, 'bot_manager') and bot_id in self.bot_manager.processes:
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
            
            # Ensure bot_manager exists
            if not hasattr(self, 'bot_manager'):
                self.bot_manager = BotManager()
                
            # Ensure child_bots list exists
            if not hasattr(self, 'child_bots'):
                self.child_bots = []
            
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
        # БЫЛО: if callback.from_user.id != self.config.owner_id:
        if not self.is_admin(callback.from_user.id):  # ИСПРАВЛЕНО
            return
        
        # Ensure bot_manager exists
        if not hasattr(self, 'bot_manager'):
            self.bot_manager = BotManager()
            
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
        # БЫЛО: if callback.from_user.id != self.config.owner_id:
        if not self.is_admin(callback.from_user.id):  # ИСПРАВЛЕНО
            return
        
        bot_id = callback.data.replace("stop_clone_", "")
        
        try:
            # Ensure bot_manager exists
            if not hasattr(self, 'bot_manager'):
                self.bot_manager = BotManager()
                
            self.bot_manager.remove_bot(bot_id)
            await callback.answer(f"Бот {bot_id} остановлен")
        except Exception as e:
            await callback.answer(f"Ошибка при остановке: {e}")
        
        await self.manage_clones(callback)

    # Update the clone_bot_submit method to provide inline option
    async def clone_bot_submit(self, message: types.Message):
        """Handler for new bot token submission"""
        # БЫЛО: if message.from_user.id != self.config.owner_id:
        if not self.is_admin(message.from_user.id):  # ИСПРАВЛЕНО
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
        if hasattr(self, 'child_bots') and hasattr(self, 'bot_manager'):
            for bot_id in self.child_bots:
                try:
                    self.bot_manager.remove_bot(bot_id)
                except Exception as e:
                    logger.error(f"Error stopping bot {bot_id}: {e}")

    async def clone_bot_prompt(self, callback: types.CallbackQuery):
        """Prompt for cloning the bot"""
        if not self.is_admin(callback.from_user.id):
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
        if not self.is_admin(callback.from_user.id):  # ИСПРАВЛЕНО
            return
        
        # Parse data: overwrite_clone_dirname_token
        parts = callback.data.split('_', 3)
        if len(parts) != 4:
            await callback.answer("Ошибка в данных")
            return
        
        clone_dir = parts[2]
        new_token = parts[3]
        
        # Delete existing clone
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        clone_path = os.path.join(parent_dir, clone_dir)
        
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
            self.handle_custom_start_time,
            lambda message: hasattr(self, 'awaiting_custom_start_time') and 
            self.awaiting_custom_start_time == message.from_user.id
        )

        self.dp.message.register(
            self.handle_custom_end_time,
            lambda message: hasattr(self, 'awaiting_custom_end_time') and 
            self.awaiting_custom_end_time == message.from_user.id
        )
        # Регистрация прямой команды для добавления канала
        self.dp.message.register(
            self.add_channel_prompt,
            Command("addchannel")
        )
        self.dp.my_chat_member.register(self.handle_chat_member)

        # Обработчик для постов в канале
        self.dp.channel_post.register(self.handle_channel_post)
        # Обработчики callback-запросов
        callbacks = {
            "toggle_forward": self.toggle_forwarding,
            "add_channel_input": self.add_channel_input,
            "remove_channel_": self.remove_channel,
            "remove_": self.remove_chat,
            "list_chats": self.list_chats,
            "back_to_main": self.main_menu,
            "channels": self.manage_channels,
            "add_channel": self.add_channel_prompt,
            "test_pin": self.test_pin_handler,
            "clone_bot": self.clone_bot_prompt,
            "clone_inline_": self.clone_bot_inline,
            "overwrite_clone_": self.overwrite_clone,
            "clone_files_": self.create_clone_files,
            "stop_clone_": self.stop_clone,
            "manage_clones": self.manage_clones,
            "manage_schedule": self.manage_schedule,
            "add_schedule_start": self.add_schedule_start,
            "select_channel_": self.select_channel_for_schedule,
            "set_time_": self.set_predefined_time,
            "custom_time": self.custom_time_start,
            "remove_schedule": self.remove_schedule_prompt,
            "remove_slot_": self.remove_schedule_confirm,
        }
        self.dp.message.register(
            self.clone_bot_submit,
                lambda message: hasattr(self, 'awaiting_clone_token') and 
                self.awaiting_clone_token == message.from_user.id
            )
        
        self.dp.callback_query.register(self.manage_schedule, lambda c: c.data == "manage_schedule")
        self.dp.callback_query.register(self.add_schedule_prompt, lambda c: c.data == "add_schedule")
        self.dp.callback_query.register(self.remove_schedule_prompt, lambda c: c.data == "remove_schedule")
        self.dp.callback_query.register(self.remove_schedule_confirm, lambda c: c.data.startswith("remove_slot_"))

        # Обработчики для ввода данных при добавлении слота
        self.dp.message.register(
            self.add_schedule_channel_submit,
            lambda message: hasattr(self, 'awaiting_channel_for_schedule') and 
            self.awaiting_channel_for_schedule == message.from_user.id
        )
        self.dp.message.register(
            self.add_schedule_start_time_submit,
            lambda message: hasattr(self, 'awaiting_start_time') and 
            self.awaiting_start_time == message.from_user.id
        )
        self.dp.message.register(
            self.add_schedule_end_time_submit,
            lambda message: hasattr(self, 'awaiting_end_time') and 
            self.awaiting_end_time == message.from_user.id
        )
        # Регистрируем обработчики с определенным порядком, чтобы избежать конфликтов
        for prefix, handler in callbacks.items():
            self.dp.callback_query.register(
                handler,
                lambda c, p=prefix: c.data.startswith(p)
            )
    async def add_schedule_start(self, callback: types.CallbackQuery):
        """Начало добавления расписания - выбор канала с проверкой дублирования"""
        if not self.is_admin(callback.from_user.id):
            return

        try:
            source_channels = self.config.source_channels
            if not source_channels:
                kb = InlineKeyboardBuilder()
                kb.button(text="◀️ Назад", callback_data="manage_schedule")
                
                text = ("⚠️ Нет настроенных исходных каналов.\n"
                    "Сначала добавьте каналы в разделе 'Управление каналами'.")
                
                await self.safe_edit_message(callback, text, kb.as_markup())
                return

            text = "📡 Выберите канал для добавления в расписание:\n\n"
            kb = InlineKeyboardBuilder()
            
            for channel_id in source_channels:
                channel_name = await self._get_channel_name(channel_id)
                kb.button(
                    text=f"📺 {channel_name}",
                    callback_data=f"select_channel_{channel_id}"
                )
            
            kb.button(text="◀️ Назад", callback_data="manage_schedule")
            kb.adjust(1)

            await self.safe_edit_message(callback, text, kb.as_markup())
                
        except Exception as e:
            logger.error(f"Ошибка в add_schedule_start: {e}")
            await callback.answer(f"Ошибка: {e}")
    
    async def select_channel_for_schedule(self, callback: types.CallbackQuery):
        """Выбор канала и переход к выбору времени с обработкой дублирования"""
        if not self.is_admin(callback.from_user.id):
            return

        try:
            channel_id = callback.data.replace("select_channel_", "")
            self.temp_schedule_data = {"channel_id": channel_id}
            
            channel_name = await self._get_channel_name(channel_id)
            
            text = f"📺 Выбран канал: {channel_name}\n\n"
            text += "⏰ Выберите временной интервал:\n\n"
            
            kb = InlineKeyboardBuilder()
            
            # Предустановленные интервалы по 2 часа
            time_slots = [
                ("🌅 06:00 - 08:00", "06:00", "08:00"),
                ("🌄 08:00 - 10:00", "08:00", "10:00"),
                ("🌞 10:00 - 12:00", "10:00", "12:00"),
                ("☀️ 12:00 - 14:00", "12:00", "14:00"),
                ("🌤️ 14:00 - 16:00", "14:00", "16:00"),
                ("🌇 16:00 - 18:00", "16:00", "18:00"),
                ("🌆 18:00 - 20:00", "18:00", "20:00"),
                ("🌃 20:00 - 22:00", "20:00", "22:00"),
                ("🌙 22:00 - 00:00", "22:00", "00:00"),
                ("🦉 00:00 - 02:00", "00:00", "02:00"),
            ]
            
            for display, start, end in time_slots:
                kb.button(
                    text=display,
                    callback_data=f"set_time_{start}_{end}"
                )
            
            kb.button(text="⚙️ Другое (ввести вручную)", callback_data="custom_time")
            kb.button(text="◀️ Назад", callback_data="add_schedule_start")
            kb.adjust(2)

            await callback.message.edit_text(text, reply_markup=kb.as_markup())
            await callback.answer()
            
        except Exception as e:
            logger.error(f"Ошибка в select_channel_for_schedule: {e}")
            await callback.answer(f"Ошибка: {e}")


    async def set_predefined_time(self, callback: types.CallbackQuery):
        """Установка предустановленного времени"""
        if not self.is_admin(callback.from_user.id):
            return

        # Правильно парсим время из callback_data
        callback_parts = callback.data.replace("set_time_", "").split("_")
        if len(callback_parts) == 2:
            start_time = callback_parts[0]
            end_time = callback_parts[1]
            await self._save_schedule(start_time, end_time, callback)
        else:
            await callback.answer("Ошибка в данных времени")

    async def custom_time_start(self, callback: types.CallbackQuery):
        """Начало ввода пользовательского времени"""
        if not self.is_admin(callback.from_user.id):
            return

        self.awaiting_custom_start_time = callback.from_user.id
        
        kb = InlineKeyboardBuilder()
        kb.button(text="❌ Отмена", callback_data="add_schedule_start")
        
        await callback.message.edit_text(
            "⏰ Введите время начала в формате ЧЧ:ММ\n"
            "Например: 14:00\n\n"
            "Отправьте время сообщением 💬",
            reply_markup=kb.as_markup()
        )
        await callback.answer()

    async def handle_custom_start_time(self, message: types.Message):
        """Обработка ввода времени начала"""
        if not self.is_admin(message.from_user.id) or self.awaiting_custom_start_time != message.from_user.id:
            return

        start_time = message.text.strip()
        if not self._validate_time(start_time):
            await message.reply("⚠️ Неверный формат времени. Используйте формат ЧЧ:ММ (например, 14:00)")
            return

        self.temp_schedule_data["start_time"] = start_time
        self.awaiting_custom_start_time = None
        self.awaiting_custom_end_time = message.from_user.id

        kb = InlineKeyboardBuilder()
        kb.button(text="❌ Отмена", callback_data="add_schedule_start")
        
        await message.reply(
            f"✅ Время начала: {start_time}\n\n"
            "⏰ Теперь введите время окончания в формате ЧЧ:ММ\n"
            "Например: 18:00\n\n"
            "Отправьте время сообщением 💬",
            reply_markup=kb.as_markup()
        )

    async def handle_custom_end_time(self, message: types.Message):
        """Обработка ввода времени окончания"""
        if not self.is_admin(message.from_user.id) or self.awaiting_custom_end_time != message.from_user.id:
            return

        end_time = message.text.strip()
        if not self._validate_time(end_time):
            await message.reply("⚠️ Неверный формат времени. Используйте формат ЧЧ:ММ (например, 18:00)")
            return

        self.awaiting_custom_end_time = None
        await self._save_schedule(self.temp_schedule_data["start_time"], end_time, message)

    async def _save_schedule(self, start_time: str, end_time: str, context):
        """Сохранение расписания"""
        try:
            channel_id = self.temp_schedule_data["channel_id"]
            
            # Проверяем, не пересекается ли время с существующими слотами
            existing_schedules = await Repository.get_schedules()
            for schedule in existing_schedules:
                if self._times_overlap(start_time, end_time, schedule['start_time'], schedule['end_time']):
                    kb = InlineKeyboardBuilder()
                    kb.button(text="◀️ Назад", callback_data="manage_schedule")
                    
                    error_text = (
                        f"⚠️ Временной слот {start_time} - {end_time} пересекается с существующим:\n"
                        f"{schedule['start_time']} - {schedule['end_time']}\n\n"
                        "Выберите другое время."
                    )
                    
                    if hasattr(context, 'message'):
                        await context.message.edit_text(error_text, reply_markup=kb.as_markup())
                    else:
                        await context.reply(error_text, reply_markup=kb.as_markup())
                    return

            await Repository.add_schedule(channel_id, start_time, end_time)
            
            channel_name = await self._get_channel_name(channel_id)
            
            kb = InlineKeyboardBuilder()
            kb.button(text="✅ К расписанию", callback_data="manage_schedule")
            kb.button(text="➕ Добавить еще", callback_data="add_schedule_start")
            kb.adjust(2)
            
            success_text = (
                f"✅ Временной слот успешно добавлен!\n\n"
                f"📺 Канал: {channel_name}\n"
                f"⏰ Время: {start_time} - {end_time}\n\n"
                f"Теперь в это время будут закрепляться сообщения из выбранного канала."
            )
            
            if hasattr(context, 'message'):
                await context.message.edit_text(success_text, reply_markup=kb.as_markup())
            else:
                await context.reply(success_text, reply_markup=kb.as_markup())
            
            self.temp_schedule_data = {}
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении расписания: {e}")
            
            kb = InlineKeyboardBuilder()
            kb.button(text="◀️ Назад", callback_data="manage_schedule")
            
            error_text = f"❌ Ошибка при сохранении расписания: {e}"
            
            if hasattr(context, 'message'):
                await context.message.edit_text(error_text, reply_markup=kb.as_markup())
            else:
                await context.reply(error_text, reply_markup=kb.as_markup())

    async def _get_channel_name(self, channel_id: str) -> str:
        """Получение имени канала"""
        try:
            chat = await self.bot.get_chat(channel_id)
            return chat.title or f"Канал {channel_id}"
        except Exception:
            return f"Канал {channel_id}"

    def _validate_time(self, time_str: str) -> bool:
        """Валидация формата времени"""
        try:
            hours, minutes = map(int, time_str.split(":"))
            return 0 <= hours < 24 and 0 <= minutes < 60
        except (ValueError, AttributeError):
            return False

    def _times_overlap(self, start1: str, end1: str, start2: str, end2: str) -> bool:
        """Проверка пересечения временных интервалов"""
        def time_to_minutes(time_str):
            h, m = map(int, time_str.split(':'))
            return h * 60 + m
        
        s1, e1 = time_to_minutes(start1), time_to_minutes(end1)
        s2, e2 = time_to_minutes(start2), time_to_minutes(end2)
        
        # Обработка переходов через полночь
        if e1 < s1:  # первый интервал переходит через полночь
            e1 += 24 * 60
        if e2 < s2:  # второй интервал переходит через полночь
            e2 += 24 * 60
            
        return not (e1 <= s2 or e2 <= s1)


    async def manage_schedule(self, callback: types.CallbackQuery):
        """Управление расписанием с улучшенным UI и обработкой дублирования"""
        if not self.is_admin(callback.from_user.id):
            return

        try:
            schedules = await Repository.get_schedules()
            text = "📅 Управление расписанием закрепления сообщений\n\n"
            
            if not schedules:
                text += "🔴 Нет настроенных временных слотов.\n\n"
            else:
                text += "✅ Активные временные слоты:\n"
                for i, schedule in enumerate(schedules, 1):
                    channel_name = await self._get_channel_name(schedule['channel_id'])
                    text += f"{i}. {channel_name}\n   🕐 {schedule['start_time']} - {schedule['end_time']}\n\n"

            kb = InlineKeyboardBuilder()
            kb.button(text="➕ Добавить новый слот", callback_data="add_schedule_start")
            if schedules:
                kb.button(text="❌ Удалить слот", callback_data="remove_schedule")
            kb.button(text="◀️ Назад", callback_data="back_to_main")
            kb.adjust(1)

            await self.safe_edit_message(callback, text, kb.as_markup())
                
        except Exception as e:
            logger.error(f"Ошибка в manage_schedule: {e}")
            await callback.answer(f"Ошибка: {e}")

    async def _start_rotation_task(self, interval: int = 7200) -> None:
            """Запускает ротацию закрепленных сообщений с указанным интервалом"""
            self.state = RunningState(self, interval)
            await self._notify_admins(f"Бот начал ротацию закрепленных сообщений с интервалом {interval//60} минут")
        
    async def rotate_now(self) -> bool:
        """Немедленно выполняет ротацию на следующий канал"""
        if not isinstance(self.state, RunningState):
            logger.warning("Нельзя выполнить немедленную ротацию: бот не запущен")
            return False
        
        return await self.state._rotate_to_next_channel()

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
            # Проверяем права бота в чате
            bot_id = (await self.bot.get_me()).id
            chat_member = await self.bot.get_chat_member(chat_id, bot_id)
            
            if chat_member.status != "administrator":
                await callback.message.edit_text(
                    f"⚠️ Бот не является администратором в чате {chat_id}.\n"
                    "Добавьте бота как администратора с правами на закрепление сообщений.",
                    reply_markup=KeyboardFactory.create_chat_list_keyboard(
                        await self._fetch_chat_info()  # Использование вспомогательного метода
                    )
                )
                await callback.answer()
                return
                
            if chat_member.status == "administrator" and not getattr(chat_member, "can_pin_messages", False):
                await callback.message.edit_text(
                    f"⚠️ У бота нет прав на закрепление сообщений в чате {chat_id}.\n"
                    "Измените права бота, предоставив возможность закреплять сообщения.",
                    reply_markup=KeyboardFactory.create_chat_list_keyboard(
                        await self._fetch_chat_info()  # Использование вспомогательного метода
                    )
                )
                await callback.answer()
                return
            
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
                    await self._fetch_chat_info()  # Использование вспомогательного метода
                )
            )
            
            # Через 5 секунд открепляем сообщение для завершения теста
            await asyncio.sleep(5)
            
            try:
                await self.bot.unpin_chat_message(
                    chat_id=chat_id,
                    message_id=test_message.message_id
                )
            except Exception as unpin_error:
                logger.warning(f"Не удалось открепить сообщение в чате {chat_id}: {unpin_error}")
                
        except Exception as e:
            await callback.message.edit_text(
                f"❌ Ошибка при тестировании закрепления: {e}\n\n"
                f"Проверьте, что бот имеет права администратора в чате {chat_id} с возможностью закреплять сообщения.",
                reply_markup=KeyboardFactory.create_chat_list_keyboard(
                    await self._fetch_chat_info()  # Использование вспомогательного метода
                )
            )
            
        await callback.answer()

    # Вспомогательный метод для получения информации о чатах
    async def _fetch_chat_info(self) -> Dict[int, str]:
        """Вспомогательный метод для получения информации о чатах"""
        chats = await Repository.get_target_chats()
        chat_info = {}
        
        for chat_id in chats:
            try:
                info = await self.cache_service.get_chat_info(self.bot, chat_id)
                if info:
                    chat_info[chat_id] = info.title
                else:
                    # Если не удалось получить из кэша, пробуем напрямую
                    try:
                        chat = await self.bot.get_chat(chat_id)
                        chat_info[chat_id] = chat.title or f"Чат {chat_id}"
                    except Exception:
                        chat_info[chat_id] = f"Чат {chat_id}"
            except Exception:
                chat_info[chat_id] = f"Чат {chat_id}"
        
        return chat_info

    async def toggle_forwarding(self, callback: types.CallbackQuery):
        """Обработчик для кнопки запуска/остановки ротации закрепленных сообщений"""
        if not self.is_admin(callback.from_user.id):
            return

        if isinstance(self.context.state, IdleState):
            await callback.message.edit_text("🔄 Запуск ротации закрепленных сообщений...")
            await self.context.state.start()
        else:
            await self.context.state.stop()

        await callback.message.edit_text(
            f"Ротация закрепленных сообщений {'запущена' if isinstance(self.context.state, RunningState) else 'остановлена'}!",
            reply_markup=KeyboardFactory.create_main_keyboard(
                isinstance(self.context.state, RunningState)
            )
        )
        await callback.answer()


    async def get_chat_info(self, bot: Bot, chat_id: int) -> Optional[ChatInfo]:
        """Get chat info from cache or fetch from API"""
        now = datetime.now().timestamp()
        
        # Check cache first
        if chat_id in self._cache:
            chat_info = self._cache[chat_id]
            if now - chat_info.last_updated < self._config.cache_ttl:
                return chat_info

        try:
            # Fetch fresh data
            chat = await bot.get_chat(chat_id)
            
            # Попытка получить количество участников
            member_count = None
            try:
                member_count = await bot.get_chat_member_count(chat_id)
            except Exception as e:
                from loguru import logger
                logger.warning(f"Не удалось получить количество участников для чата {chat_id}: {e}")
            
            info = ChatInfo(
                id=chat_id,
                title=chat.title or f"Чат {chat_id}",  # Fallback если название не удалось получить
                type=chat.type,
                member_count=member_count,
                last_updated=now
            )
            
            # Update cache
            self._cache[chat_id] = info
            
            # Notify observers
            await self._notify_observers(chat_id, info)
            
            # Cleanup old entries if cache is too large
            if len(self._cache) > self._config.max_cache_size:
                oldest = min(self._cache.items(), key=lambda x: x[1].last_updated)
                del self._cache[oldest[0]]
            
            return info
        except Exception as e:
            from loguru import logger
            logger.error(f"Error fetching chat info for {chat_id}: {e}")
            return None

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
    async def find_latest_message(self, channel_id: str) -> Optional[int]:
        """Метод-обертка для поиска последнего доступного сообщения в канале"""
        last_id = await Repository.get_last_message(channel_id)
        return await find_msg(self.bot, channel_id, self.config.owner_id, last_id)

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
        
        await callback.message.edit_text("🔄 Загрузка списка чатов...")
        
        try:
            chats = await Repository.get_target_chats()
            logger.info(f"Получено {len(chats)} чатов из базы данных: {chats}")
            
            chat_info = {}
            failed_chats = []
            
            for chat_id in chats:
                try:
                    # Сначала пробуем получить информацию через кэш
                    info = await self.cache_service.get_chat_info(self.bot, chat_id)
                    if info:
                        chat_info[chat_id] = info.title
                        logger.info(f"Успешно получена информация о чате {chat_id}: {info.title}")
                    else:
                        # Если кэш не помог, пробуем получить напрямую
                        try:
                            chat = await self.bot.get_chat(chat_id)
                            chat_info[chat_id] = chat.title or f"Чат {chat_id}"
                            logger.info(f"Напрямую получена информация о чате {chat_id}: {chat.title}")
                        except Exception as direct_e:
                            logger.error(f"Ошибка при прямом получении информации о чате {chat_id}: {direct_e}")
                            # Если чат недоступен, добавляем его с общим названием
                            chat_info[chat_id] = f"Недоступный чат {chat_id}"
                            failed_chats.append(chat_id)
                except Exception as e:
                    logger.error(f"Ошибка при получении информации о чате {chat_id}: {e}")
                    # При ошибке также добавляем чат с общим названием
                    chat_info[chat_id] = f"Чат {chat_id}"
            
            if not chat_info:
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
                    
                    # Добавляем индикатор для недоступных чатов
                    is_failed = chat_id in failed_chats
                    status_indicator = "⚠️ " if is_failed else ""
                    
                    text += f"{pin_status} {status_indicator}{title} ({chat_id})\n"
                    
                text += "\n📌 - есть закрепленное сообщение\n"
                text += "🔴 - нет закрепленного сообщения\n"
                
                if failed_chats:
                    text += "⚠️ - бот не имеет доступа к чату\n"
                
                markup = KeyboardFactory.create_chat_list_keyboard(chat_info)
            
            await callback.message.edit_text(text, reply_markup=markup)
        except Exception as e:
            logger.error(f"Общая ошибка при загрузке списка чатов: {e}")
            await callback.message.edit_text(
                f"❌ Ошибка при загрузке списка чатов: {e}",
                reply_markup=KeyboardFactory.create_main_keyboard(
                    isinstance(self.context.state, RunningState),
                )
            )
        
        await callback.answer()

    async def main_menu(self, callback: types.CallbackQuery):
        """Обработчик кнопки главного меню с защитой от дублирования"""
        if not self.is_admin(callback.from_user.id):
            return
        
        try:
            text = "Главное меню:"
            markup = KeyboardFactory.create_main_keyboard(
                isinstance(self.context.state, RunningState),
            )
            
            await self.safe_edit_message(callback, text, markup)
        except Exception as e:
            logger.error(f"Ошибка в main_menu: {e}")
            await callback.answer(f"Ошибка: {e}")
    async def manage_channels(self, callback: types.CallbackQuery):
        """Меню управления каналами с защитой от дублирования"""
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
        
        await self.safe_edit_message(callback, text, markup)

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
        """Обработчик сообщений в каналах - ТОЛЬКО сохранение, БЕЗ автопересылки"""
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
        
        # ТОЛЬКО сохраняем ID последнего сообщения
        await Repository.save_last_message(chat_id, message.message_id)
        logger.info(f"💾 Сохранено сообщение {message.message_id} из канала {chat_id}")
        
        # Делегируем обработку сообщения контексту
        await self.context.handle_message(chat_id, message.message_id)
        
        logger.info(f"ℹ️ Сообщение {message.message_id} из канала {chat_id} будет обработано только по расписанию")

    async def handle_chat_member(self, update: types.ChatMemberUpdated):
        """Обработчик добавления/удаления бота из чатов"""
        # Проверяем, что это событие касается бота
        if update.new_chat_member.user.id != self.bot.id:
            return

        chat_id = update.chat.id
        old_status = update.old_chat_member.status
        new_status = update.new_chat_member.status
        
        logger.info(f"Изменение статуса бота в чате {chat_id}: {old_status} → {new_status}")
        
        # Бот был добавлен или получил права администратора
        if new_status in ['member', 'administrator'] and update.chat.type in ['group', 'supergroup']:
            added = await Repository.add_target_chat(chat_id)
            self.cache_service.remove_from_cache(chat_id)
            
            if new_status == 'administrator':
                # Проверяем права бота
                pin_rights = getattr(update.new_chat_member, 'can_pin_messages', False)
                
                await self._notify_admins(
                    f"🤖 Бот добавлен как администратор в {update.chat.type}: {update.chat.title} ({chat_id})\n"
                    f"📌 Права на закрепление сообщений: {'✅' if pin_rights else '❌'}"
                )
                
                if not pin_rights:
                    await self.bot.send_message(
                        chat_id,
                        "⚠️ Внимание! Бот не имеет права на закрепление сообщений. "
                        "Для полноценной работы пожалуйста предоставьте боту необходимые права администратора."
                    )
            else:
                await self._notify_admins(f"🤖 Бот добавлен как участник в {update.chat.type}: {update.chat.title} ({chat_id})")
                
                try:
                    await self.bot.send_message(
                        chat_id,
                        "⚠️ Внимание! Для корректной работы бота, пожалуйста, добавьте его как администратора "
                        "с правами на закрепление сообщений."
                    )
                except Exception as e:
                    logger.error(f"Не удалось отправить сообщение в чат {chat_id}: {e}")
            
            logger.info(
                f"Бот {'добавлен' if added else 'уже был добавлен'} в {update.chat.type}: "
                f"{update.chat.title} ({chat_id}) со статусом {new_status}"
            )
        
        # Бот был удален или понижен в правах
        elif old_status in ['member', 'administrator'] and new_status not in ['member', 'administrator']:
            # Удаляем информацию о закрепленном сообщении
            await Repository.delete_pinned_message(str(chat_id))
            if str(chat_id) in self.pinned_messages:
                del self.pinned_messages[str(chat_id)]
                
            await Repository.remove_target_chat(chat_id)
            self.cache_service.remove_from_cache(chat_id)
            await self._notify_admins(f"⚠️ Бот удален из чата {update.chat.title} ({chat_id})")
            logger.info(f"Бот удален из чата {update.chat.title} ({chat_id})")
    
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
        try:
            # Инициализация базы данных
            await Repository.init_db()
            logger.info("База данных инициализирована")
            
            # Восстанавливаем закрепленные сообщения из базы данных
            pinned_messages = await Repository.get_all_pinned_messages()
            self.pinned_messages = pinned_messages
            self.context.pinned_messages = pinned_messages  # Передаем в контекст
            logger.info(f"Загружено {len(pinned_messages)} закрепленных сообщений")
            
            # Устанавливаем интервал по умолчанию, если не задан
            if not await Repository.get_config("rotation_interval"):
                await Repository.set_config("rotation_interval", "7200")  # 2 часа по умолчанию
                logger.info("Установлен интервал ротации по умолчанию: 7200 секунд (2 часа)")
            
            # Проверяем целевые чаты
            target_chats = await Repository.get_target_chats()
            logger.info(f"Загружено {len(target_chats)} целевых чатов из базы данных: {target_chats}")
            
            # Проверяем исходные каналы
            logger.info(f"Загружено {len(self.config.source_channels)} исходных каналов: {self.config.source_channels}")
            
            # Получаем информацию о боте
            bot_info = await self.bot.get_me()
            logger.info(f"Бот запущен: @{bot_info.username} (ID: {bot_info.id})")
            
            # Уведомляем администраторов о запуске
            for admin_id in self.config.admin_ids:
                try:
                    await self.bot.send_message(
                        admin_id, 
                        f"✅ Бот @{bot_info.username} успешно запущен!\n\n"
                        f"📊 Статистика:\n"
                        f"• Исходных каналов: {len(self.config.source_channels)}\n"
                        f"• Целевых чатов: {len(target_chats)}\n"
                        f"• Закрепленных сообщений: {len(pinned_messages)}"
                    )
                except Exception as e:
                    logger.error(f"Не удалось отправить уведомление администратору {admin_id}: {e}")
            
            logger.info("Бот успешно запущен!")
            
            try:
                # Получаем ID последнего обновления, чтобы избежать дубликатов
                offset = 0
                try:
                    updates = await self.bot.get_updates(limit=1, timeout=1)
                    if updates:
                        offset = updates[-1].update_id + 1
                        logger.info(f"Установлен начальный offset: {offset}")
                except Exception as e:
                    logger.warning(f"Не удалось получить начальные обновления: {e}")

                # Запускаем опрос обновлений
                await self.dp.start_polling(self.bot, offset=offset)
            finally:
                # Отключаем наблюдателя кэша и закрываем соединение с ботом
                self.cache_service.remove_observer(self)
                await self.bot.session.close()
        except Exception as e:
            logger.critical(f"Критическая ошибка при запуске бота: {e}")
            import traceback
            logger.critical(f"Traceback: {traceback.format_exc()}")
            raise

# Обновляем класс KeyboardFactory для работы с новым функционалом
class KeyboardFactory:
    """Реализация паттерна Factory для создания клавиатур"""
    
    @staticmethod
    def create_main_keyboard(running: bool = False) -> Any:
        """Create main menu keyboard"""
        kb = InlineKeyboardBuilder()
        kb.button(
            text="🔄 Запустить ротацию" if not running else "⏹ Остановить ротацию",
            callback_data="toggle_forward"
        )
        kb.button(text="⚙️ Управление каналами", callback_data="channels")
        kb.button(text="🤖 Клонировать бота", callback_data="clone_bot")  # Добавить эту кнопку
        kb.button(text="👥 Управление клонами", callback_data="manage_clones")  # Добавить эту кнопку
        kb.button(text="💬 Список целевых чатов", callback_data="list_chats")
        kb.button(text="📅 Управление расписанием", callback_data="manage_schedule")  # Новая кнопка
        kb.adjust(2)
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

            
# Update the main function to handle cleanup
async def main():
    """Main entry point with improved error handling and resource cleanup"""
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
    except asyncio.CancelledError:
        logger.info("Main task was cancelled")
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Bot stopped due to error: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    finally:
        logger.info("Starting cleanup process...")
        try:
            if bot:
                logger.info("Cleaning up bot resources...")
                await bot.cleanup()  # Stop all child bots
            
            logger.info("Closing database connections...")
            await Repository.close_db()
            
            if os.path.exists(lock_file):
                logger.info("Removing lock file...")
                os.remove(lock_file)
                
            logger.info("Cleanup completed successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            import traceback
            logger.error(f"Cleanup traceback: {traceback.format_exc()}")

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

from aiogram import types
from aiogram.utils.keyboard import InlineKeyboardBuilder
from loguru import logger
from .base_command import Command
from database.repository import Repository
from utils.keyboard_factory import KeyboardFactory
from utils.bot_state import IdleState, RunningState
from utils.config import Config


class StartCommand(Command):
    def __init__(self, running: bool = False):
        super().__init__()
        self.running = running

    async def _handle(self, message: types.Message) -> None:
        await message.answer(
            "Добро пожаловать в бот для пересылки сообщений из каналов!\n"
            "Используйте кнопки ниже для управления ботом:\n\n"
            "Введите /help для просмотра доступных команд.",
            reply_markup=KeyboardFactory.create_main_keyboard(self.running)
        )

class HelpCommand(Command):
    async def _handle(self, message: types.Message) -> None:
        help_text = (
            "📋 <b>Доступные команды:</b>\n\n"
            "/start - Показать главное меню\n"
            "/help - Показать это сообщение\n"
            "/setlast <channel_id> <message_id> - Установить ID последнего сообщения вручную\n"
            "/getlast - Получить текущие ID последних сообщений для всех каналов\n"
            "/forwardnow - Немедленно переслать последнее сообщение\n"
            "/test <channel_id> <message_id> - Проверить существование сообщения в канале\n"
            "/findlast <channel_id> - Найти последнее валидное сообщение в канале\n\n"
            "Используйте кнопки в меню для управления пересылкой и настройками."
        )
        await message.answer(help_text, parse_mode="HTML")

class SetLastMessageCommand(Command):
    def __init__(self, bot):
        super().__init__()
        self.bot = bot

    async def _handle(self, message: types.Message) -> None:
        args = message.text.split()
        
        if len(args) != 3:
            await message.answer("Использование: /setlast <channel_id> <message_id>")
            return

        try:
            channel_id = args[1]
            message_id = int(args[2])
            
            try:
                test_msg = await self.bot.forward_message(
                    chat_id=message.from_user.id,
                    from_chat_id=channel_id,
                    message_id=message_id
                )
                
                await Repository.save_last_message(channel_id, message_id)
                await message.answer(f"✅ Сообщение ID {message_id} из канала {channel_id} проверено и сохранено.")
            
            except Exception as e:
                await message.answer(f"⚠️ Не удалось проверить сообщение в канале {channel_id}: {e}")
        
        except ValueError:
            await message.answer("❌ ID сообщения должен быть числом")

class GetLastMessageCommand(Command):
    async def _handle(self, message: types.Message) -> None:
        last_messages = await Repository.get_all_last_messages()
        
        if not last_messages:
            await message.answer("❌ Не найдено сохраненных ID сообщений.")
            return
            
        response = "📝 Текущие последние сообщения по каналам:\n\n"
        
        for channel_id, data in last_messages.items():
            response += f"Канал: {channel_id}\n"
            response += f"ID сообщения: {data['message_id']}\n"
            response += f"Время: {data['timestamp']}\n\n"
        
        await message.answer(response)

class ForwardNowCommand(Command):
    def __init__(self, bot_context):
        super().__init__()
        self.context = bot_context

    async def _handle(self, message: types.Message) -> None:
        channel_id, message_id = await Repository.get_latest_message()
        
        if not channel_id or not message_id:
            await message.answer(
                "⚠️ Не найдено недавних сообщений. Сначала добавьте каналы и сообщения."
            )
            return

        progress_msg = await message.answer(f"🔄 Пересылаю сообщение {message_id} из канала {channel_id}...")
        
        await self.context.handle_message(channel_id, message_id)
        await progress_msg.edit_text("✅ Сообщение успешно переслано.")

class TestMessageCommand(Command):
    def __init__(self, bot):
        super().__init__()
        self.bot = bot

    async def _handle(self, message: types.Message) -> None:
        args = message.text.split()
        
        if len(args) != 3:
            await message.answer("Использование: /test <channel_id> <message_id>")
            return

        try:
            channel_id = args[1]
            message_id = int(args[2])
            
            progress_msg = await message.answer(f"🔍 Проверяю сообщение {message_id} в канале {channel_id}...")
            
            try:
                test_msg = await self.bot.forward_message(
                    chat_id=message.from_user.id,
                    from_chat_id=channel_id,
                    message_id=message_id
                )
                await progress_msg.edit_text(f"✅ Сообщение {message_id} в канале {channel_id} существует и может быть переслано.")
            except Exception as e:
                await progress_msg.edit_text(f"❌ Ошибка: {e}")
        except ValueError:
            await message.answer("❌ ID сообщения должен быть числом")

class FindLastMessageCommand(Command):
    def __init__(self, bot):
        super().__init__()
        self.bot = bot

    async def _handle(self, message: types.Message) -> None:
        args = message.text.split()
        
        if len(args) != 2:
            await message.answer("Использование: /findlast <channel_id>")
            return
            
        channel_id = args[1]
        progress_msg = await message.answer(f"🔍 Ищу последнее валидное сообщение в канале {channel_id}...")
        
        last_messages = await Repository.get_all_last_messages()
        current_id = None
        
        for chan, data in last_messages.items():
            if chan == channel_id:
                current_id = data["message_id"]
                break
        
        if not current_id:
            current_id = 1000
        
        valid_id = None
        checked_count = 0
        max_check = 100

        for msg_id in range(current_id + 10, current_id - max_check, -1):
            if msg_id <= 0:
                break

            checked_count += 1
            if checked_count % 10 == 0:
                try:
                    await progress_msg.edit_text(f"⏳ Проверено {checked_count} сообщений...")
                except Exception:
                    pass

            try:
                msg = await self.bot.forward_message(
                    chat_id=message.from_user.id,
                    from_chat_id=channel_id,
                    message_id=msg_id
                )
                valid_id = msg_id
                break
            except Exception as e:
                if "message not found" in str(e).lower():
                    continue
                logger.warning(f"Unexpected error checking message {msg_id} in channel {channel_id}: {e}")

        try:
            await progress_msg.delete()
        except Exception:
            pass

        if valid_id:
            await Repository.save_last_message(channel_id, valid_id)
            await message.answer(
                f"✅ Найдено валидное сообщение (ID: {valid_id}) в канале {channel_id} после проверки {checked_count} сообщений."
            )
        else:
            await message.answer(
                f"❌ Не найдено валидных сообщений в канале {channel_id} после проверки {checked_count} сообщений."
            )
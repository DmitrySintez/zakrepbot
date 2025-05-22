from abc import ABC, abstractmethod
from typing import Optional
import asyncio
from loguru import logger
from database.repository import Repository
from datetime import datetime, timedelta
from aiogram import types
from utils.message_utils import find_latest_message as find_msg

class BotState(ABC):
    """Abstract base class for bot states"""
    
    @abstractmethod
    async def start(self) -> None:
        """Handle start action"""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Handle stop action"""
        pass
    
    @abstractmethod
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        """Handle message forwarding"""
        pass

class IdleState:
    """Состояние, когда бот не пересылает сообщения"""
    
    def __init__(self, bot_context):
        self.context = bot_context
    
    async def start(self) -> None:
        self.context.state = RunningState(self.context)
        await self.context._notify_admins("Бот начал работу по расписанию")
    
    async def stop(self) -> None:
        pass
    
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        await Repository.save_last_message(channel_id, message_id)
        logger.info(f"Сохранено сообщение {message_id} из канала {channel_id} (бот остановлен)")

class RunningState(BotState):
    """Состояние, когда бот активно закрепляет сообщения по расписанию"""
    
    def __init__(self, bot_context, auto_forward: bool = True):
        self.context = bot_context
        self._schedule_task = None
        self._current_channel = None
        self.auto_forward = auto_forward
        self._start_schedule_task()
    
    def _start_schedule_task(self):
        """Запуск задачи проверки расписания"""
        if not self._schedule_task or self._schedule_task.done():
            self._schedule_task = asyncio.create_task(self._schedule_check())
    
    async def _schedule_check(self):
        """Периодическая проверка расписания"""
        try:
            logger.info("Запущена задача проверки расписания")
            while True:
                active_channel = await self._get_active_channel()
                if active_channel:
                    if active_channel != self._current_channel:
                        self._current_channel = active_channel
                        message_id = await Repository.get_last_message(active_channel)
                        if message_id:
                            await self.context.forward_and_pin_message(active_channel, message_id)
                            logger.info(f"Активирован канал {active_channel}, закреплено сообщение {message_id}")
                        else:
                            logger.warning(f"Не найдено последнее сообщение для канала {active_channel}")
                else:
                    if self._current_channel:
                        await self._unpin_current_message()
                        self._current_channel = None
                        logger.info("Нет активных каналов, откреплено сообщение")
                await asyncio.sleep(60)  # Проверка каждую минуту
        except asyncio.CancelledError:
            logger.info("Задача проверки расписания отменена")
        except Exception as e:
            logger.error(f"Ошибка в задаче проверки расписания: {e}")
            await asyncio.sleep(10)
            self._start_schedule_task()
    
    async def _get_active_channel(self) -> Optional[str]:
        """Определение активного канала по расписанию"""
        schedules = await Repository.get_schedules()
        current_time = datetime.now().strftime("%H:%M")
        for schedule in schedules:
            start_time = schedule["start_time"]
            end_time = schedule["end_time"]
            if start_time <= current_time < end_time:
                return schedule["channel_id"]
        return None
    
    async def _unpin_current_message(self):
        """Открепление текущего сообщения во всех чатах"""
        target_chats = await Repository.get_target_chats()
        for chat_id in target_chats:
            pinned_message_id = await Repository.get_pinned_message(str(chat_id))
            if pinned_message_id:
                try:
                    await self.context.bot.unpin_chat_message(
                        chat_id=chat_id,
                        message_id=pinned_message_id
                    )
                    await Repository.delete_pinned_message(str(chat_id))
                    logger.info(f"Откреплено сообщение {pinned_message_id} в чате {chat_id}")
                except Exception as e:
                    logger.error(f"Не удалось открепить сообщение в чате {chat_id}: {e}")
    
    async def start(self) -> None:
        pass
    
    async def stop(self) -> None:
        if self._schedule_task and not self._schedule_task.done():
            self._schedule_task.cancel()
        await self.context._notify_admins("Бот остановил работу по расписанию")
        self.context.state = IdleState(self.context)
    
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        """Обработка нового сообщения из канала"""
        await Repository.save_last_message(channel_id, message_id)
        if await self._is_channel_active(channel_id):
            await self.context.forward_and_pin_message(channel_id, message_id)
            logger.info(f"Закреплено новое сообщение {message_id} из активного канала {channel_id}")
    
    async def _is_channel_active(self, channel_id: str) -> bool:
        """Проверка, активен ли канал в данный момент"""
        schedules = await Repository.get_schedules()
        current_time = datetime.now().strftime("%H:%M")
        for schedule in schedules:
            if schedule["channel_id"] == channel_id:
                start_time = schedule["start_time"]
                end_time = schedule["end_time"]
                if start_time <= current_time < end_time:
                    return True
        return False
    
    async def find_latest_message(self, channel_id: str) -> Optional[int]:
        """Поиск последнего доступного сообщения в канале"""
        last_id = await Repository.get_last_message(channel_id)
        return await find_msg(self.context.bot, channel_id, self.context.config.owner_id, last_id)

    async def _rotate_to_next_channel(self) -> bool:
        """Заглушка для совместимости, теперь не используется"""
        logger.warning("Метод _rotate_to_next_channel не используется в режиме расписания")
        return False

                
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
    
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        await self.state.handle_message(channel_id, message_id)
    
    async def rotate_now(self) -> bool:
        """Немедленно выполняет ротацию на следующий канал"""
        if not isinstance(self.state, RunningState):
            logger.warning("Нельзя выполнить немедленную ротацию: бот не запущен")
            return False
        
        return await self.state._rotate_to_next_channel()
    
    async def forward_and_pin_message(self, channel_id: str, message_id: int) -> bool:
        target_chats = await Repository.get_target_chats()
        if not target_chats:
            logger.warning("Нет целевых чатов для пересылки")
            return False
        
        for chat_id in target_chats:
            try:
                # Получаем предыдущее закрепленное сообщение для этого чата
                prev_pinned = await Repository.get_pinned_message(str(chat_id))
                
                # Пересылаем новое сообщение
                fwd = await self.context.bot.forward_message(
                    chat_id=chat_id,
                    from_chat_id=channel_id,
                    message_id=message_id
                )
                
                # Если успешно переслали, пробуем открепить предыдущее 
                if prev_pinned:
                    try:
                        await self.context.bot.unpin_chat_message(
                            chat_id=chat_id,
                            message_id=prev_pinned
                        )
                    except Exception as e:
                        logger.warning(f"Не удалось открепить предыдущее сообщение в чате {chat_id}: {e}")
                
                # Закрепляем новое сообщение
                await self.context.bot.pin_chat_message(
                    chat_id=chat_id,
                    message_id=fwd.message_id,
                    disable_notification=True
                )
                
                # Сохраняем ID нового закрепленного сообщения
                await Repository.save_pinned_message(str(chat_id), fwd.message_id)
                
                logger.info(f"Сообщение {message_id} из канала {channel_id} переслано и закреплено в чат {chat_id}")
                success = True
            except Exception as e:
                logger.error(f"Ошибка при обработке чата {chat_id}: {e}")
    async def _notify_admins(self, message: str):
        """Отправка уведомления всем администраторам бота"""
        for admin_id in self.config.admin_ids:
            try:
                await self.bot.send_message(admin_id, message)
            except Exception as e:
                logger.error(f"Не удалось уведомить администратора {admin_id}: {e}")
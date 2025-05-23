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
        """Запуск работы по расписанию"""
        self.context.state = RunningState(self.context)
        await self.context._notify_admins("🚀 Бот запущен! Работа по расписанию активирована.")
    
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
        self._current_message_id = None
        self.auto_forward = auto_forward
        self._last_check_date = None
        self._start_schedule_task()
    
    def _start_schedule_task(self):
        """Запуск задачи проверки расписания"""
        if not self._schedule_task or self._schedule_task.done():
            self._schedule_task = asyncio.create_task(self._schedule_check())
    
    async def _schedule_check(self):
        """Периодическая проверка расписания с логикой смены дня"""
        try:
            logger.info("🕐 Запущена задача проверки расписания")
            while True:
                current_date = datetime.now().date()
                current_time = datetime.now().strftime("%H:%M")
                
                # Проверяем, сменился ли день
                if self._last_check_date != current_date:
                    logger.info(f"📅 Новый день: {current_date}. Сброс состояния.")
                    self._current_channel = None
                    self._current_message_id = None
                    self._last_check_date = current_date
                
                active_channel = await self._get_active_channel()
                
                if active_channel:
                    logger.debug(f"📺 Активный канал по расписанию: {active_channel}")
                    
                    # Получаем последнее сообщение из канала
                    latest_message_id = await Repository.get_last_message(active_channel)
                    
                    if latest_message_id:
                        # Проверяем, изменился ли канал или сообщение
                        channel_changed = active_channel != self._current_channel
                        message_changed = latest_message_id != self._current_message_id
                        
                        if channel_changed or message_changed:
                            logger.info(
                                f"🔄 Обновление: канал {'изменился' if channel_changed else 'тот же'}, "
                                f"сообщение {'новое' if message_changed else 'то же'}"
                            )
                            
                            # Пытаемся переслать и закрепить сообщение
                            success = await self.context.forward_and_pin_message(active_channel, latest_message_id)
                            
                            if success:
                                self._current_channel = None
                        self._current_message_id = None
                    
                    logger.debug(f"🕐 Нет активных каналов в {current_time}")
                
                # Проверяем каждые 30 секунд для быстрой реакции на изменения
                await asyncio.sleep(30)
                
        except asyncio.CancelledError:
            logger.info("⏹️ Задача проверки расписания отменена")
        except Exception as e:
            logger.error(f"❌ Ошибка в задаче проверки расписания: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Перезапускаем задачу через 10 секунд
            await asyncio.sleep(10)
            if not self._schedule_task.cancelled():
                self._start_schedule_task()
    
    async def _get_active_channel(self) -> Optional[str]:
        """Определение активного канала по расписанию"""
        try:
            schedules = await Repository.get_schedules()
            current_time = datetime.now().strftime("%H:%M")
            
            for schedule in schedules:
                start_time = schedule["start_time"]
                end_time = schedule["end_time"]
                
                if self._is_time_in_range(current_time, start_time, end_time):
                    logger.debug(f"📍 Найден активный канал {schedule['channel_id']} для времени {current_time}")
                    return schedule["channel_id"]
            
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка при определении активного канала: {e}")
            return None
    
    def _is_time_in_range(self, current_time: str, start_time: str, end_time: str) -> bool:
        """Проверка, попадает ли текущее время в диапазон"""
        try:
            def time_to_minutes(time_str):
                h, m = map(int, time_str.split(':'))
                return h * 60 + m
            
            current_minutes = time_to_minutes(current_time)
            start_minutes = time_to_minutes(start_time)
            end_minutes = time_to_minutes(end_time)
            
            # Обработка перехода через полночь
            if end_minutes < start_minutes:
                # Диапазон переходит через полночь (например, 22:00 - 02:00)
                return current_minutes >= start_minutes or current_minutes < end_minutes
            else:
                # Обычный диапазон в рамках одного дня
                return start_minutes <= current_minutes < end_minutes
                
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке времени: {e}")
            return False
    
    async def _unpin_current_messages(self):
        """Открепление текущих сообщений во всех чатах"""
        try:
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
                        logger.info(f"📌 Откреплено сообщение {pinned_message_id} в чате {chat_id}")
                    except Exception as e:
                        logger.error(f"❌ Не удалось открепить сообщение в чате {chat_id}: {e}")
        except Exception as e:
            logger.error(f"❌ Ошибка при откреплении сообщений: {e}")
    
    async def start(self) -> None:
        """Состояние уже запущено"""
        pass
    
    async def stop(self) -> None:
        """Остановка состояния"""
        if self._schedule_task and not self._schedule_task.done():
            self._schedule_task.cancel()
            try:
                await self._schedule_task
            except asyncio.CancelledError:
                pass
        
        # Открепляем все сообщения при остановке
        await self._unpin_current_messages()
        
        await self.context._notify_admins("⏹️ Бот остановлен. Работа по расписанию деактивирована.")
        self.context.state = IdleState(self.context)
    
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        """Обработка нового сообщения из канала"""
        await Repository.save_last_message(channel_id, message_id)
        logger.info(f"💾 Сохранено новое сообщение {message_id} из канала {channel_id}")
        
        # Проверяем, активен ли этот канал сейчас
        if await self._is_channel_active(channel_id):
            success = await self.context.forward_and_pin_message(channel_id, message_id)
            if success:
                self._current_message_id = message_id
                logger.info(f"📌 Новое сообщение {message_id} из активного канала {channel_id} переслано и закреплено")
            else:
                logger.warning(f"⚠️ Не удалось переслать новое сообщение {message_id} из канала {channel_id}")
        else:
            logger.info(f"ℹ️ Канал {channel_id} не активен по расписанию, сообщение только сохранено")
    
    async def _is_channel_active(self, channel_id: str) -> bool:
        """Проверка, активен ли канал в данный момент"""
        try:
            schedules = await Repository.get_schedules()
            current_time = datetime.now().strftime("%H:%M")
            
            for schedule in schedules:
                if schedule["channel_id"] == channel_id:
                    start_time = schedule["start_time"]
                    end_time = schedule["end_time"]
                    
                    if self._is_time_in_range(current_time, start_time, end_time):
                        return True
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке активности канала {channel_id}: {e}")
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
        """Пересылка и закрепление сообщений из канала во все целевые чаты с улучшенной обработкой ошибок"""
        try:
            target_chats = await Repository.get_target_chats()
            if not target_chats:
                logger.warning("⚠️ Нет целевых чатов для пересылки")
                return False
            
            # Для отслеживания общего результата операции
            success = False
            
            # Сначала проверяем, существует ли сообщение в канале
            try:
                # Пытаемся получить информацию о сообщении, пересылая себе для проверки
                await self.bot.forward_message(
                    chat_id=self.config.owner_id,
                    from_chat_id=channel_id,
                    message_id=message_id
                )
                logger.debug(f"✅ Сообщение {message_id} из канала {channel_id} доступно")
            except Exception as e:
                logger.error(f"❌ Сообщение {message_id} недоступно в канале {channel_id}: {e}")
                
                # Пытаемся найти более новое сообщение
                logger.info(f"🔍 Попытка найти более новое сообщение в канале {channel_id}")
                try:
                    from utils.message_utils import find_latest_message
                    latest_message_id = await find_latest_message(self.bot, channel_id, self.config.owner_id, message_id)
                    
                    if latest_message_id and latest_message_id != message_id:
                        logger.info(f"📨 Найдено более новое сообщение {latest_message_id} в канале {channel_id}")
                        await Repository.save_last_message(channel_id, latest_message_id)
                        message_id = latest_message_id
                    else:
                        logger.warning(f"⚠️ Не удалось найти новые сообщения в канале {channel_id}")
                        return False
                except Exception as find_error:
                    logger.error(f"❌ Ошибка при поиске новых сообщений в канале {channel_id}: {find_error}")
                    return False
            
            # Пересылаем сообщение во все целевые чаты
            for chat_id in target_chats:
                try:
                    # Получаем предыдущее закрепленное сообщение для этого чата
                    prev_pinned = await Repository.get_pinned_message(str(chat_id))
                    
                    # Пересылаем новое сообщение
                    try:
                        fwd = await self.bot.forward_message(
                            chat_id=chat_id,
                            from_chat_id=channel_id,
                            message_id=message_id
                        )
                        logger.debug(f"📤 Сообщение переслано в чат {chat_id}")
                        
                        # Если успешно переслали, пробуем открепить предыдущее 
                        if prev_pinned:
                            try:
                                await self.bot.unpin_chat_message(
                                    chat_id=chat_id,
                                    message_id=prev_pinned
                                )
                                logger.debug(f"📌 Откреплено предыдущее сообщение {prev_pinned} в чате {chat_id}")
                            except Exception as e:
                                logger.warning(f"⚠️ Не удалось открепить предыдущее сообщение в чате {chat_id}: {e}")
                        
                        # Закрепляем новое сообщение
                        try:
                            await self.bot.pin_chat_message(
                                chat_id=chat_id,
                                message_id=fwd.message_id,
                                disable_notification=True
                            )
                            
                            # Сохраняем ID нового закрепленного сообщения
                            await Repository.save_pinned_message(str(chat_id), fwd.message_id)
                            
                            # Если у нас есть словарь для отслеживания, обновляем его
                            if hasattr(self, 'pinned_messages'):
                                self.pinned_messages[str(chat_id)] = fwd.message_id
                            
                            logger.info(f"📌 Сообщение {message_id} из канала {channel_id} переслано и закреплено в чат {chat_id}")
                            success = True
                        except Exception as e:
                            logger.error(f"❌ Не удалось закрепить сообщение в чате {chat_id}: {e}")
                            # Даже если не удалось закрепить, пересылка прошла успешно
                            success = True
                    except Exception as e:
                        logger.error(f"❌ Не удалось переслать сообщение из канала {channel_id} в чат {chat_id}: {e}")
                except Exception as e:
                    logger.error(f"❌ Ошибка при обработке чата {chat_id}: {e}")
            
            # Возвращаем общий результат операции
            return success
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в forward_and_pin_message: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    async def _notify_admins(self, message: str):
        """Отправка уведомления всем администраторам бота"""
        for admin_id in self.config.admin_ids:
            try:
                await self.bot.send_message(admin_id, message)
            except Exception as e:
                logger.error(f"Не удалось уведомить администратора {admin_id}: {e}")
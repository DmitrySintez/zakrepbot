# 1. Обновленный файл utils/bot_state.py с полной реализацией BotContext

from abc import ABC, abstractmethod
from typing import Optional
import asyncio
from loguru import logger
from database.repository import Repository
from datetime import datetime, timedelta
from aiogram import types, Bot
from utils.message_utils import find_latest_message as find_msg
from utils.config import Config

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
        # Уведомляем администраторов
        for admin_id in self.context.config.admin_ids:
            try:
                await self.context.bot.send_message(admin_id, "🚀 Бот запущен! Работа по расписанию активирована.")
            except Exception as e:
                logger.error(f"Не удалось уведомить администратора {admin_id}: {e}")
    
    async def stop(self) -> None:
        pass
    
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        """В режиме ожидания только сохраняем сообщения, НЕ пересылаем"""
        await Repository.save_last_message(channel_id, message_id)
        logger.info(f"💾 Сохранено сообщение {message_id} из канала {channel_id} (бот остановлен, пересылка отключена)")

class RunningState(BotState):
    """Состояние, когда бот активно закрепляет сообщения ТОЛЬКО по расписанию"""
    
    def __init__(self, bot_context, auto_forward: bool = False):  # Изменено на False
        self.context = bot_context
        self._schedule_task = None
        self._current_active_channel = None  # Текущий активный канал
        self._current_pinned_message = None  # ID текущего закрепленного сообщения
        self._last_pin_time = None  # Время последнего закрепления
        self.auto_forward = auto_forward  # ОТКЛЮЧЕНА автопересылка
        self._last_check_date = None
        self._processed_slots = set()  # Отслеживание обработанных слотов в текущий день
        self._start_schedule_task()
    
    def _start_schedule_task(self):
        """Запуск задачи проверки расписания"""
        if not self._schedule_task or self._schedule_task.done():
            self._schedule_task = asyncio.create_task(self._schedule_check())
    
    async def _schedule_check(self):
        """Периодическая проверка расписания с логикой смены дня"""
        try:
            logger.info("🕐 Запущена задача проверки расписания (ТОЛЬКО по времени, без автопересылки)")
            while True:
                current_date = datetime.now().date()
                current_time = datetime.now().strftime("%H:%M")
                
                # Проверяем, сменился ли день
                if self._last_check_date != current_date:
                    logger.info(f"📅 Новый день: {current_date}. Сброс состояния.")
                    self._current_active_channel = None
                    self._current_pinned_message = None
                    self._last_pin_time = None
                    self._processed_slots.clear()  # Очищаем обработанные слоты
                    self._last_check_date = current_date
                
                # Получаем активный канал для текущего времени
                active_channel_info = await self._get_active_channel_info()
                
                if active_channel_info:
                    channel_id = active_channel_info["channel_id"]
                    slot_id = active_channel_info["slot_id"]
                    
                    logger.debug(f"📺 Активный канал по расписанию: {channel_id} (слот: {slot_id})")
                    
                    # Проверяем, обрабатывали ли мы уже этот слот сегодня
                    if slot_id not in self._processed_slots:
                        logger.info(f"🆕 Новый временной слот {slot_id} для канала {channel_id}")
                        
                        # Получаем последнее сообщение из канала
                        latest_message_id = await Repository.get_last_message(channel_id)
                        
                        if latest_message_id:
                            # Пытаемся переслать и закрепить сообщение ТОЛЬКО по расписанию
                            success = await self.context.forward_and_pin_message(channel_id, latest_message_id)
                            
                            if success:
                                self._current_active_channel = channel_id
                                self._current_pinned_message = latest_message_id
                                self._last_pin_time = datetime.now()
                                self._processed_slots.add(slot_id)  # Отмечаем слот как обработанный
                                
                                logger.info(f"✅ Успешно обработан слот {slot_id} для канала {channel_id} (по расписанию)")
                            else:
                                logger.error(f"❌ Не удалось обработать слот {slot_id} для канала {channel_id}")
                        else:
                            logger.warning(f"⚠️ Нет сохраненных сообщений для канала {channel_id}")
                            # Отмечаем слот как обработанный, чтобы не пытаться снова
                            self._processed_slots.add(slot_id)
                    else:
                        logger.debug(f"✅ Слот {slot_id} уже обработан сегодня")
                else:
                    # Если нет активного канала, сбрасываем текущее состояние
                    if self._current_active_channel:
                        logger.info(f"⏰ Время активности канала {self._current_active_channel} закончилось")
                        self._current_active_channel = None
                        self._current_pinned_message = None
                        self._last_pin_time = None
                
                # Проверяем каждые 60 секунд (достаточно для точности по минутам)
                await asyncio.sleep(60)
                
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
    
    async def _get_active_channel_info(self) -> Optional[dict]:
        """Определение активного канала по расписанию с уникальным ID слота"""
        try:
            schedules = await Repository.get_schedules()
            current_time = datetime.now().strftime("%H:%M")
            
            for i, schedule in enumerate(schedules):
                start_time = schedule["start_time"]
                end_time = schedule["end_time"]
                channel_id = schedule["channel_id"]
                
                if self._is_time_in_range(current_time, start_time, end_time):
                    # Создаем уникальный ID слота на основе канала и времени
                    slot_id = f"{channel_id}_{start_time}_{end_time}"
                    
                    logger.debug(f"📍 Найден активный канал {channel_id} для времени {current_time} (слот: {slot_id})")
                    return {
                        "channel_id": channel_id,
                        "slot_id": slot_id,
                        "start_time": start_time,
                        "end_time": end_time
                    }
            
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
                        error_text = str(e).lower()
                        if any(phrase in error_text for phrase in [
                            "message to unpin not found",
                            "message not found",
                            "message_id_invalid"
                        ]):
                            # Сообщение уже не существует, просто удаляем запись из БД
                            await Repository.delete_pinned_message(str(chat_id))
                            logger.info(f"📌 Запись о закрепленном сообщении {pinned_message_id} удалена (сообщение не найдено)")
                        else:
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
        
        # Уведомляем администраторов
        for admin_id in self.context.config.admin_ids:
            try:
                await self.context.bot.send_message(admin_id, "⏹️ Бот остановлен. Работа по расписанию деактивирована.")
            except Exception as e:
                logger.error(f"Не удалось уведомить администратора {admin_id}: {e}")
        
        self.context.state = IdleState(self.context)
    
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        """Обработка нового сообщения из канала - ТОЛЬКО сохранение, БЕЗ автопересылки"""
        await Repository.save_last_message(channel_id, message_id)
        logger.info(f"💾 Сохранено новое сообщение {message_id} из канала {channel_id} (автопересылка ОТКЛЮЧЕНА)")
        
        # ВАЖНО: НЕ пересылаем сообщения автоматически при их появлении
        # Пересылка происходит ТОЛЬКО по расписанию через _schedule_check()
        
        logger.info(f"ℹ️ Сообщение {message_id} из канала {channel_id} будет переслано только по расписанию")
    
    async def find_latest_message(self, channel_id: str) -> Optional[int]:
        """Поиск последнего доступного сообщения в канале"""
        last_id = await Repository.get_last_message(channel_id)
        return await find_msg(self.context.bot, channel_id, self.context.config.owner_id, last_id)

    async def _rotate_to_next_channel(self) -> bool:
        """Заглушка для совместимости, теперь не используется"""
        logger.warning("Метод _rotate_to_next_channel не используется в режиме расписания")
        return False

    async def _get_active_channel(self) -> Optional[str]:
        """Получение текущего активного канала"""
        info = await self._get_active_channel_info()
        return info["channel_id"] if info else None
                
class BotContext:
    """Контекстный класс, управляющий состоянием бота"""
    
    def __init__(self, bot, config):
        self.bot = bot
        self.config = config
        self.state: BotState = IdleState(self)
        # Для хранения закрепленных сообщений
        self.pinned_messages = {}
    
    async def start(self) -> None:
        await self.state.start()
    
    async def stop(self) -> None:
        await self.state.stop()
    
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        """Делегирование обработки сообщения текущему состоянию"""
        await self.state.handle_message(channel_id, message_id)
    
    async def rotate_now(self) -> bool:
        """Немедленно активировать текущий канал по расписанию"""
        if not isinstance(self.state, RunningState):
            logger.warning("Нельзя выполнить немедленную ротацию: бот не запущен")
            return False
        active_channel = await self.state._get_active_channel()
        if active_channel:
            message_id = await Repository.get_last_message(active_channel)
            if message_id:
                return await self.forward_and_pin_message(active_channel, message_id)
        return False
    
    async def forward_and_pin_message(self, channel_id: str, message_id: int) -> bool:
        """Пересылка и закрепление сообщений из канала во все целевые чаты без пересылки админу для проверки"""
        try:
            target_chats = await Repository.get_target_chats()
            if not target_chats:
                logger.warning("⚠️ Нет целевых чатов для пересылки")
                return False
            
            # Для отслеживания общего результата операции
            success = False
            
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
                            self.pinned_messages[str(chat_id)] = fwd.message_id
                            
                            logger.info(f"📌 Сообщение {message_id} из канала {channel_id} переслано и закреплено в чат {chat_id}")
                            success = True
                        except Exception as e:
                            logger.error(f"❌ Не удалось закрепить сообщение в чате {chat_id}: {e}")
                            # Даже если не удалось закрепить, пересылка прошла успешно
                            success = True
                    except Exception as e:
                        # Если сообщение не может быть переслано (например, не существует)
                        error_text = str(e).lower()
                        if any(phrase in error_text for phrase in [
                            "message not found", 
                            "message to forward not found",
                            "message_id_invalid"
                        ]):
                            logger.warning(f"⚠️ Сообщение {message_id} не найдено в канале {channel_id}")
                            
                            # Пытаемся найти более новое сообщение
                            logger.info(f"🔍 Попытка найти более новое сообщение в канале {channel_id}")
                            try:
                                from utils.message_utils import find_latest_message
                                latest_message_id = await find_latest_message(self.bot, channel_id, self.config.owner_id, message_id)
                                
                                if latest_message_id and latest_message_id != message_id:
                                    logger.info(f"📨 Найдено более новое сообщение {latest_message_id} в канале {channel_id}")
                                    await Repository.save_last_message(channel_id, latest_message_id)
                                    
                                    # Рекурсивно пробуем переслать новое сообщение
                                    return await self.forward_and_pin_message(channel_id, latest_message_id)
                                else:
                                    logger.warning(f"⚠️ Не удалось найти новые сообщения в канале {channel_id}")
                                    return False
                            except Exception as find_error:
                                logger.error(f"❌ Ошибка при поиске новых сообщений в канале {channel_id}: {find_error}")
                                return False
                        else:
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
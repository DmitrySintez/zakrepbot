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

class IdleState(BotState):
    """State when bot is not forwarding messages"""
    
    def __init__(self, bot_context, auto_forward: bool = False):
        self.context = bot_context
        self.auto_forward = auto_forward
    
    async def start(self) -> None:
        interval = int(await Repository.get_config("repost_interval", "3600"))
        self.context.state = RunningState(self.context, interval, self.auto_forward)
        await self.context._notify_admins("Бот начал пересылку")
    
    async def stop(self) -> None:
        # Already stopped
        pass
    
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        # Don't forward messages in idle state
        logger.info("Bot is idle, not forwarding messages")

class IdleState:
    """Состояние, когда бот не пересылает сообщения"""
    
    def __init__(self, bot_context):
        self.context = bot_context
    
    async def start(self) -> None:
        # Получаем интервал из базы (по умолчанию 2 часа = 7200 секунд)
        interval = int(await Repository.get_config("rotation_interval", "7200"))
        # Вместо вызова несуществующего метода, создаем объект RunningState
        # и присваиваем его контексту
        self.context.state = RunningState(self.context, interval)
        # Уведомляем админов о запуске
        await self.context._notify_admins(f"Бот начал ротацию закрепленных сообщений с интервалом {interval//60} минут")
    
    async def stop(self) -> None:
        # Уже остановлен
        pass
    
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        # Только сохраняем сообщение, но не делаем пересылку в состоянии Idle
        await Repository.save_last_message(channel_id, message_id)
        logger.info(f"Сохранено сообщение {message_id} из канала {channel_id} (бот остановлен)")
        
class RunningState:
    """Состояние, когда бот активно пересылает и закрепляет сообщения с ротацией между каналами"""
    
    def __init__(self, bot_context, interval: int):
        self.context = bot_context
        self.interval = interval  # Интервал ротации в секундах (например, 7200 = 2 часа)
        self._rotation_task = None
        
        # Индекс текущего канала для ротации
        self._current_channel_index = 0
        
        # Запускаем задачу ротации
        self._start_rotation_task()
        
    async def find_latest_message(self, channel_id: str) -> Optional[int]:
        """Метод-обертка для поиска последнего доступного сообщения в канале"""
        last_id = await Repository.get_last_message(channel_id)
        return await find_msg(self.context.bot, channel_id, self.context.config.owner_id, last_id)
        
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
        
        await self.context._notify_admins("Бот остановил ротацию каналов")
    
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        """Обработка нового сообщения из канала"""
        # Когда в канале появляется новое сообщение, сохраняем его ID
        await Repository.save_last_message(channel_id, message_id)
        logger.info(f"Сохранено новое сообщение {message_id} из канала {channel_id}")
    
    async def _channel_rotation(self):
        """Основная задача ротации закрепленных сообщений по расписанию"""
        try:
            logger.info("Запущена задача ротации закрепленных сообщений")
            
            # Начинаем с первого канала
            await self._rotate_to_next_channel()
            
            while True:
                # Ждем указанный интервал до следующей ротации
                logger.info(f"Ожидание {self.interval} секунд до следующей ротации закрепленных сообщений")
                await asyncio.sleep(self.interval)
                
                # Переключаемся на следующий канал
                await self._rotate_to_next_channel()
                
        except asyncio.CancelledError:
            logger.info("Задача ротации закрепленных сообщений отменена")
        except Exception as e:
            logger.error(f"Ошибка в задаче ротации закрепленных сообщений: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Перезапускаем задачу в случае неожиданной ошибки через 10 секунд
            await asyncio.sleep(10)
            self._start_rotation_task()
    
    async def _rotate_to_next_channel(self) -> bool:
        """Переключение на следующий канал в ротации и закрепление его сообщения"""
        source_channels = self.context.config.source_channels
        
        if not source_channels:
            logger.warning("Нет настроенных исходных каналов для ротации")
            return False
        
        # Убедимся, что индекс в пределах списка каналов
        if self._current_channel_index >= len(source_channels):
            self._current_channel_index = 0
        
        # Получаем текущий канал
        channel_id = source_channels[self._current_channel_index]
        logger.info(f"Ротация на канал: {channel_id} (индекс: {self._current_channel_index})")
        
        # Получаем ID последнего сообщения в этом канале
        message_id = await Repository.get_last_message(channel_id)
        
        if not message_id:
            logger.warning(f"Не найдено последнее сообщение для канала {channel_id}")
            
            # Пытаемся найти последнее сообщение в канале
            latest_id = await self.find_latest_message(channel_id)
            if latest_id:
                message_id = latest_id
                await Repository.save_last_message(channel_id, latest_id)
                logger.info(f"Найдено и сохранено новое последнее сообщение: {latest_id}")
            else:
                # Если не удалось найти сообщение, переходим к следующему каналу
                logger.error(f"Не удалось найти ни одного сообщения в канале {channel_id}")
                self._current_channel_index = (self._current_channel_index + 1) % len(source_channels)
                return False
        
        logger.info(f"Будет переслано и закреплено сообщение: {message_id} из канала: {channel_id}")
        
        # Пересылаем и закрепляем сообщение во все целевые чаты
        success = await self.context.forward_and_pin_message(channel_id, message_id)
        
        if success:
            logger.info(f"Успешно переслано и закреплено сообщение из канала {channel_id}")
            
            # Подготавливаем следующий канал
            self._current_channel_index = (self._current_channel_index + 1) % len(source_channels)
            
            # Рассчитываем время следующей ротации для логирования
            next_time = datetime.now() + timedelta(seconds=self.interval)
            next_time_str = next_time.strftime('%H:%M:%S')
            
            # Форматируем интервал для удобства чтения
            if self.interval >= 3600:
                hours = self.interval // 3600
                minutes = (self.interval % 3600) // 60
                if minutes > 0:
                    interval_str = f"{hours} ч {minutes} мин"
                else:
                    interval_str = f"{hours} ч"
            else:
                interval_str = f"{self.interval // 60} мин"
            
            # Определяем следующий канал
            next_channel = source_channels[self._current_channel_index]
            
            logger.info(f"Следующая ротация через {interval_str} (в {next_time_str}). "
                      f"Будет переслано сообщение из канала {next_channel}")
            
            return True
        else:
            # Если пересылка не удалась, переходим к следующему каналу
            logger.error(f"Не удалось переслать и закрепить сообщение из канала {channel_id}")
            self._current_channel_index = (self._current_channel_index + 1) % len(source_channels)
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
                # Получаем информацию о чате
                chat_info = await self.bot.get_chat(chat_id)
                
                # Проверяем, что это не канал (в каналах нельзя закреплять сообщения с помощью бота)
                if chat_info.type == 'channel':
                    logger.info(f"Пропускаю пересылку с закреплением в канал {chat_id} (только группы/супергруппы поддерживаются)")
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
                
                # Логируем статистику
                await Repository.log_forward(message_id)
                success = True
                logger.info(f"Сообщение {message_id} переслано и закреплено в чате {chat_id}")
            except Exception as e:
                logger.error(f"Ошибка при пересылке/закреплении в {chat_id}: {e}")

        return success
    
    async def _notify_admins(self, message: str):
        """Отправка уведомления всем администраторам бота"""
        for admin_id in self.config.admin_ids:
            try:
                await self.bot.send_message(admin_id, message)
            except Exception as e:
                logger.error(f"Не удалось уведомить администратора {admin_id}: {e}")
# В новом файле utils/message_utils.py
from typing import Optional
from loguru import logger
from aiogram import Bot

async def find_latest_message(bot: Bot, channel_id: str, owner_id: int, last_saved_id: Optional[int] = None) -> Optional[int]:
    """Универсальная функция для поиска последнего доступного сообщения в канале"""
    try:
        # Начинаем с последнего известного ID + 10 или с 1000
        start_id = (last_saved_id + 10) if last_saved_id else 1000
        max_check = 100
        checked_count = 0
        
        for msg_id in range(start_id, start_id - max_check, -1):
            if msg_id <= 0:
                break
                
            checked_count += 1
            if checked_count % 10 == 0:
                logger.info(f"Проверено {checked_count} сообщений в канале {channel_id}")
            
            try:
                # Пытаемся переслать сообщение самому себе для проверки
                msg = await bot.forward_message(
                    chat_id=owner_id,
                    from_chat_id=channel_id,
                    message_id=msg_id
                )
                
                logger.info(f"Найдено сообщение {msg_id} в канале {channel_id}")
                return msg_id
            except Exception as e:
                if "message not found" in str(e).lower() or "message to forward not found" in str(e).lower():
                    continue
                logger.warning(f"Неожиданная ошибка при проверке сообщения {msg_id} в канале {channel_id}: {e}")
        
        logger.warning(f"Не найдено валидных сообщений в канале {channel_id} после проверки {checked_count} сообщений")
        return None
    except Exception as e:
        logger.error(f"Ошибка при поиске последнего сообщения в канале {channel_id}: {e}")
        return None
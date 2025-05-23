# ПОЛНОСТЬЮ ЗАМЕНИТЬ utils/message_utils.py на:

from typing import Optional
from loguru import logger
from aiogram import Bot

async def find_latest_message(bot: Bot, channel_id: str, owner_id: int, last_saved_id: Optional[int] = None) -> Optional[int]:
    """
    Универсальная функция для поиска последнего доступного сообщения в канале
    
    Args:
        bot: Экземпляр бота
        channel_id: ID канала для поиска
        owner_id: ID владельца бота (для тестирования пересылки)
        last_saved_id: Последний сохраненный ID сообщения (опционально)
    
    Returns:
        ID последнего доступного сообщения или None
    """
    try:
        logger.info(f"🔍 Начинаю поиск последнего сообщения в канале {channel_id}")
        
        # Определяем начальную точку поиска
        if last_saved_id:
            # Начинаем поиск с последнего сохраненного ID + небольшой запас
            start_id = last_saved_id + 50
            logger.info(f"📍 Начинаю поиск с ID {start_id} (последний сохраненный: {last_saved_id})")
        else:
            # Если нет сохраненного ID, начинаем с разумного значения
            start_id = 1000
            logger.info(f"📍 Начинаю поиск с ID {start_id} (нет сохраненного ID)")
        
        max_check = 200  # Максимальное количество сообщений для проверки
        checked_count = 0
        valid_id = None
        
        # Сначала ищем вперед (более новые сообщения)
        logger.debug("🔎 Поиск более новых сообщений...")
        for msg_id in range(start_id, start_id + max_check):
            checked_count += 1
            
            if checked_count % 20 == 0:
                logger.info(f"⏳ Проверено {checked_count} сообщений (текущий ID: {msg_id})")
            
            try:
                # Пытаемся переслать сообщение владельцу для проверки существования
                msg = await bot.forward_message(
                    chat_id=owner_id,
                    from_chat_id=channel_id,
                    message_id=msg_id
                )
                
                logger.info(f"✅ Найдено новое сообщение {msg_id} в канале {channel_id}")
                valid_id = msg_id
                # Продолжаем поиск, чтобы найти самое последнее
                
            except Exception as e:
                error_text = str(e).lower()
                if any(phrase in error_text for phrase in [
                    "message not found", 
                    "message to forward not found",
                    "message_id_invalid"
                ]):
                    # Сообщение не найдено, это нормально
                    continue
                else:
                    # Другая ошибка, логируем предупреждение
                    logger.debug(f"⚠️ Неожиданная ошибка при проверке сообщения {msg_id}: {e}")
                    continue
        
        # Если не нашли более новые сообщения, ищем назад от исходной точки
        if not valid_id:
            logger.debug("🔙 Поиск более старых сообщений...")
            search_start = start_id if last_saved_id else start_id
            
            for msg_id in range(search_start, max(1, search_start - max_check), -1):
                checked_count += 1
                
                if checked_count % 20 == 0:
                    logger.info(f"⏳ Проверено {checked_count} сообщений (текущий ID: {msg_id})")
                
                try:
                    msg = await bot.forward_message(
                        chat_id=owner_id,
                        from_chat_id=channel_id,
                        message_id=msg_id
                    )
                    
                    logger.info(f"✅ Найдено сообщение {msg_id} в канале {channel_id}")
                    valid_id = msg_id
                    break  # Берем первое найденное сообщение при поиске назад
                    
                except Exception as e:
                    error_text = str(e).lower()
                    if any(phrase in error_text for phrase in [
                        "message not found", 
                        "message to forward not found",
                        "message_id_invalid"
                    ]):
                        continue
                    else:
                        logger.debug(f"⚠️ Неожиданная ошибка при проверке сообщения {msg_id}: {e}")
                        continue
        
        # Результат поиска
        if valid_id:
            logger.info(f"🎯 Найдено валидное сообщение (ID: {valid_id}) в канале {channel_id} после проверки {checked_count} сообщений")
            return valid_id
        else:
            logger.warning(f"❌ Не найдено валидных сообщений в канале {channel_id} после проверки {checked_count} сообщений")
            return None
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при поиске последнего сообщения в канале {channel_id}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None
# ПОЛНОСТЬЮ ЗАМЕНИТЬ utils/message_utils.py на:

from typing import Optional
from loguru import logger
from aiogram import Bot

async def find_latest_message(bot: Bot, channel_id: str, owner_id: int, last_saved_id: Optional[int] = None) -> Optional[int]:
    """
    Универсальная функция для поиска последнего доступного сообщения в канале
    БЕЗ пересылки сообщений админу для проверки
    
    Args:
        bot: Экземпляр бота
        channel_id: ID канала для поиска
        owner_id: ID владельца бота (НЕ используется для пересылки)
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
                # Используем copy_message вместо forward_message для проверки существования
                # copy_message не создает видимого сообщения для пользователя
                test_msg = await bot.copy_message(
                    chat_id=channel_id,  # Копируем в тот же канал
                    from_chat_id=channel_id,
                    message_id=msg_id,
                    disable_notification=True
                )
                
                # Если копирование успешно, сообщение существует
                # Сразу удаляем скопированное сообщение
                try:
                    await bot.delete_message(chat_id=channel_id, message_id=test_msg.message_id)
                except:
                    pass  # Игнорируем ошибки удаления
                
                logger.info(f"✅ Найдено новое сообщение {msg_id} в канале {channel_id}")
                valid_id = msg_id
                # Продолжаем поиск, чтобы найти самое последнее
                
            except Exception as e:
                error_text = str(e).lower()
                if any(phrase in error_text for phrase in [
                    "message not found", 
                    "message to copy not found",
                    "message_id_invalid",
                    "message to forward not found"
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
                    # Используем copy_message для проверки
                    test_msg = await bot.copy_message(
                        chat_id=channel_id,
                        from_chat_id=channel_id,
                        message_id=msg_id,
                        disable_notification=True
                    )
                    
                    # Удаляем скопированное сообщение
                    try:
                        await bot.delete_message(chat_id=channel_id, message_id=test_msg.message_id)
                    except:
                        pass
                    
                    logger.info(f"✅ Найдено сообщение {msg_id} в канале {channel_id}")
                    valid_id = msg_id
                    break  # Берем первое найденное сообщение при поиске назад
                    
                except Exception as e:
                    error_text = str(e).lower()
                    if any(phrase in error_text for phrase in [
                        "message not found", 
                        "message to copy not found",
                        "message_id_invalid",
                        "message to forward not found"
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


async def check_message_exists(bot: Bot, channel_id: str, message_id: int) -> bool:
    """
    Проверка существования сообщения БЕЗ пересылки админу
    
    Args:
        bot: Экземпляр бота
        channel_id: ID канала
        message_id: ID сообщения для проверки
    
    Returns:
        True если сообщение существует, False если нет
    """
    try:
        # Пробуем скопировать сообщение в тот же канал для проверки существования
        test_msg = await bot.copy_message(
            chat_id=channel_id,
            from_chat_id=channel_id, 
            message_id=message_id,
            disable_notification=True
        )
        
        # Если успешно скопировали, сразу удаляем копию
        try:
            await bot.delete_message(chat_id=channel_id, message_id=test_msg.message_id)
        except:
            pass  # Игнорируем ошибки удаления
            
        return True
        
    except Exception as e:
        error_text = str(e).lower()
        if any(phrase in error_text for phrase in [
            "message not found",
            "message to copy not found", 
            "message_id_invalid",
            "message to forward not found"
        ]):
            return False
        else:
            # Неожиданная ошибка
            logger.warning(f"Неожиданная ошибка при проверке сообщения {message_id} в канале {channel_id}: {e}")
            return False
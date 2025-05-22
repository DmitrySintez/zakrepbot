import asyncio
import weakref
from datetime import datetime
from typing import Optional, List, Dict, Any
import aiosqlite
from contextlib import asynccontextmanager
from loguru import logger
from utils.config import Config

class DatabaseConnectionPool:
    """Connection pool manager"""
    _pool = weakref.WeakSet()
    _active_connections = []  # Отслеживаем все активные соединения
    
    @classmethod
    async def close_all(cls):
        """Close all database connections"""
        logger.info(f"Закрытие всех соединений с базой данных. Активных соединений: {len(cls._active_connections)}")
        
        # Закрываем все активные соединения
        for conn in cls._active_connections[:]:
            try:
                logger.debug(f"Закрытие соединения {id(conn)}")
                await conn.close()
                cls._active_connections.remove(conn)
            except Exception as e:
                logger.error(f"Ошибка при закрытии соединения {id(conn)}: {e}")
        
        # Также проходим по пулу для надежности
        for conn in cls._pool:
            try:
                await conn.close()
            except Exception as e:
                logger.error(f"Ошибка при закрытии соединения из пула: {e}")
        
        cls._pool.clear()
        
        # Для надежности проверяем, что все соединения закрыты
        if cls._active_connections:
            logger.warning(f"Остались незакрытые соединения: {len(cls._active_connections)}")
            cls._active_connections.clear()
            
        logger.info("Все соединения с базой данных закрыты")
    
    @classmethod
    @asynccontextmanager
    async def get_connection(cls):
        """Get a database connection from the pool"""
        config = Config()
        
        # Получаем максимальное число соединений из конфигурации или используем значение по умолчанию
        max_connections = getattr(config, 'max_db_connections', 5)
        
        connection = None
        
        # Сначала ищем доступное соединение в пуле
        for conn in cls._pool:
            if hasattr(conn, 'in_use') and not conn.in_use:
                connection = conn
                connection.in_use = True
                break
        
        # Если не нашли свободное соединение, создаем новое, если не превышен лимит
        if connection is None:
            if len(cls._active_connections) < max_connections:
                try:
                    connection = await aiosqlite.connect(config.db_path)
                    connection.in_use = True
                    cls._pool.add(connection)
                    cls._active_connections.append(connection)
                    logger.debug(f"Создано новое соединение {id(connection)}. Всего соединений: {len(cls._active_connections)}")
                except Exception as e:
                    logger.error(f"Ошибка при создании соединения: {e}")
                    raise
            else:
                # Если достигнут лимит, ждем освобождения соединения
                waiting_start = datetime.now()
                logger.warning(f"Достигнут лимит соединений ({max_connections}). Ожидание свободного соединения...")
                while True:
                    await asyncio.sleep(0.1)
                    for conn in cls._pool:
                        if hasattr(conn, 'in_use') and not conn.in_use:
                            connection = conn
                            connection.in_use = True
                            waiting_time = (datetime.now() - waiting_start).total_seconds()
                            logger.info(f"Получено свободное соединение после {waiting_time:.2f} сек ожидания")
                            break
                    if connection:
                        break
                    # Если ждем слишком долго, выдаем ошибку
                    if (datetime.now() - waiting_start).total_seconds() > 30:
                        logger.error("Превышено время ожидания соединения с БД")
                        raise TimeoutError("Превышено время ожидания соединения с БД")
        
        try:
            yield connection
        finally:
            if connection:
                connection.in_use = False
                logger.debug(f"Соединение {id(connection)} освобождено")

class Repository:
    """Repository pattern implementation for database operations"""
    
    @staticmethod
    async def close_db() -> None:
        """Close all database connections"""
        await DatabaseConnectionPool.close_all()
    
    @staticmethod
    async def init_db() -> None:
        """Initialize database schema"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                await db.executescript("""
                    -- Существующие таблицы (оставьте свои как есть)
                    CREATE TABLE IF NOT EXISTS config (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    );
                    CREATE TABLE IF NOT EXISTS target_chats (
                        chat_id INTEGER PRIMARY KEY,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    -- ... (другие ваши таблицы)
                    
                    -- Новая таблица для расписания
                    CREATE TABLE IF NOT EXISTS schedule (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        channel_id TEXT,
                        start_time TEXT,  -- HH:MM
                        end_time TEXT,    -- HH:MM
                        UNIQUE(channel_id, start_time, end_time)
                    );
                    
                    -- Существующие индексы (если есть)
                    CREATE INDEX IF NOT EXISTS idx_forward_stats_timestamp ON forward_stats(timestamp);
                    -- ... (другие ваши индексы)
                """)
                await db.commit()
                logger.info("База данных инициализирована успешно")
        except Exception as e:
            logger.error(f"Ошибка при инициализации базы данных: {e}")
            raise
    @staticmethod
    async def add_schedule(channel_id: str, start_time: str, end_time: str) -> None:
        """Add a schedule entry for a channel"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                await db.execute(
                    "INSERT INTO schedule (channel_id, start_time, end_time) VALUES (?, ?, ?)",
                    (channel_id, start_time, end_time)
                )
                await db.commit()
                logger.info(f"Добавлен временной слот для канала {channel_id}: {start_time}-{end_time}")
        except Exception as e:
            logger.error(f"Ошибка при добавлении временного слота: {e}")

    @staticmethod
    async def get_schedules() -> List[Dict[str, Any]]:
        """Get all schedule entries"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                async with db.execute(
                    "SELECT channel_id, start_time, end_time FROM schedule"
                ) as cursor:
                    results = await cursor.fetchall()
                    return [{"channel_id": row[0], "start_time": row[1], "end_time": row[2]} for row in results]
        except Exception as e:
            logger.error(f"Ошибка при получении расписания: {e}")
            return []

    @staticmethod
    async def remove_schedule(channel_id: str, start_time: str, end_time: str) -> None:
        """Remove a schedule entry for a channel"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                await db.execute(
                    "DELETE FROM schedule WHERE channel_id = ? AND start_time = ? AND end_time = ?",
                    (channel_id, start_time, end_time)
                )
                await db.commit()
                logger.info(f"Удален временной слот для канала {channel_id}: {start_time}-{end_time}")
        except Exception as e:
            logger.error(f"Ошибка при удалении временного слота: {e}")
    @staticmethod
    async def get_target_chats() -> List[int]:
        """Get list of target chat IDs"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                async with db.execute("SELECT chat_id FROM target_chats") as cursor:
                    result = await cursor.fetchall()
                    chat_ids = [row[0] for row in result]
                    logger.debug(f"Получено {len(chat_ids)} целевых чатов из базы данных: {chat_ids}")
                    return chat_ids
        except Exception as e:
            logger.error(f"Ошибка при получении целевых чатов: {e}")
            return []
    @staticmethod
    async def add_target_chat(chat_id: int) -> bool:
        """Add new target chat and return success status"""
        try:
            # Проверяем, существует ли уже этот чат
            async with DatabaseConnectionPool.get_connection() as db:
                async with db.execute(
                    "SELECT chat_id FROM target_chats WHERE chat_id = ?", 
                    (chat_id,)
                ) as cursor:
                    existing = await cursor.fetchone()
                
                if existing:
                    logger.debug(f"Чат {chat_id} уже существует в базе данных")
                    return False
                
                await db.execute(
                    "INSERT INTO target_chats (chat_id) VALUES (?)",
                    (chat_id,)
                )
                await db.commit()
                
                # Проверяем, действительно ли добавлен чат
                async with db.execute(
                    "SELECT chat_id FROM target_chats WHERE chat_id = ?", 
                    (chat_id,)
                ) as cursor:
                    success = await cursor.fetchone() is not None
                
                if success:
                    logger.info(f"Добавлен целевой чат: {chat_id}")
                    return True
                else:
                    logger.error(f"Не удалось подтвердить добавление чата {chat_id}")
                    return False
        except Exception as e:
            logger.error(f"Ошибка при добавлении целевого чата {chat_id}: {e}")
            return False

    @staticmethod
    async def remove_target_chat(chat_id: int) -> None:
        """Remove target chat"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                await db.execute(
                    "DELETE FROM target_chats WHERE chat_id = ?",
                    (chat_id,)
                )
                await db.commit()
                logger.info(f"Удален целевой чат: {chat_id}")
        except Exception as e:
            logger.error(f"Ошибка при удалении целевого чата {chat_id}: {e}")

    @staticmethod
    async def get_config(key: str, default: Optional[str] = None) -> Optional[str]:
        """Get configuration value"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                async with db.execute(
                    "SELECT value FROM config WHERE key = ?",
                    (key,)
                ) as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else default
        except Exception as e:
            logger.error(f"Ошибка при получении конфигурации для ключа '{key}': {e}")
            return default

    @staticmethod
    async def set_config(key: str, value: str) -> None:
        """Set configuration value"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                await db.execute(
                    "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                    (key, str(value))
                )
                await db.commit()
                logger.debug(f"Установлено значение конфигурации: {key}={value}")
        except Exception as e:
            logger.error(f"Ошибка при установке конфигурации {key}={value}: {e}")

    @staticmethod
    async def log_forward(message_id: int) -> None:
        """Log forwarded message"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                await db.execute(
                    "INSERT INTO forward_stats (message_id) VALUES (?)",
                    (message_id,)
                )
                await db.commit()
                logger.debug(f"Залогирована пересылка сообщения {message_id}")
        except Exception as e:
            logger.error(f"Ошибка при логировании пересылки сообщения {message_id}: {e}")

    @staticmethod
    async def save_last_message(channel_id: str, message_id: int) -> None:
        """Save last message ID for channel"""
        try:
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
                logger.debug(f"Сохранено последнее сообщение для канала {channel_id}: {message_id}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении последнего сообщения для канала {channel_id}: {e}")

    @staticmethod
    async def get_last_message(channel_id: str) -> Optional[int]:
        """Get last message ID for channel"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                async with db.execute(
                    "SELECT message_id FROM last_messages WHERE channel_id = ?",
                    (channel_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else None
        except Exception as e:
            logger.error(f"Ошибка при получении последнего сообщения для канала {channel_id}: {e}")
            return None

    @staticmethod
    async def get_all_last_messages() -> Dict[str, Dict[str, Any]]:
        """Get last message IDs for all channels"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                async with db.execute(
                    "SELECT channel_id, message_id, timestamp FROM last_messages"
                ) as cursor:
                    results = await cursor.fetchall()
                    return {row[0]: {"message_id": row[1], "timestamp": row[2]} for row in results}
        except Exception as e:
            logger.error(f"Ошибка при получении всех последних сообщений: {e}")
            return {}

    @staticmethod
    async def get_latest_message() -> tuple:
        """Get the most recent message across all channels"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                async with db.execute(
                    "SELECT channel_id, message_id, timestamp FROM last_messages ORDER BY timestamp DESC LIMIT 1"
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        return (row[0], row[1])  # (channel_id, message_id)
                    return (None, None)
        except Exception as e:
            logger.error(f"Ошибка при получении последнего сообщения среди всех каналов: {e}")
            return (None, None)

    @staticmethod
    async def save_pinned_message(chat_id: str, message_id: int) -> None:
        """Save pinned message ID for chat"""
        try:
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
                logger.debug(f"Сохранено закрепленное сообщение для чата {chat_id}: {message_id}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении закрепленного сообщения для чата {chat_id}: {e}")

    @staticmethod
    async def get_pinned_message(chat_id: str) -> Optional[int]:
        """Get pinned message ID for chat"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                async with db.execute(
                    "SELECT message_id FROM pinned_messages WHERE chat_id = ?",
                    (chat_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else None
        except Exception as e:
            logger.error(f"Ошибка при получении закрепленного сообщения для чата {chat_id}: {e}")
            return None

    @staticmethod
    async def get_all_pinned_messages() -> Dict[str, int]:
        """Get all pinned messages"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                async with db.execute(
                    "SELECT chat_id, message_id FROM pinned_messages"
                ) as cursor:
                    results = await cursor.fetchall()
                    return {row[0]: row[1] for row in results}
        except Exception as e:
            logger.error(f"Ошибка при получении всех закрепленных сообщений: {e}")
            return {}

    @staticmethod
    async def delete_pinned_message(chat_id: str) -> None:
        """Delete pinned message record"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                await db.execute(
                    "DELETE FROM pinned_messages WHERE chat_id = ?",
                    (chat_id,)
                )
                await db.commit()
                logger.debug(f"Удалена запись о закрепленном сообщении для чата {chat_id}")
        except Exception as e:
            logger.error(f"Ошибка при удалении записи о закрепленном сообщении для чата {chat_id}: {e}")

    @staticmethod
    async def set_channel_interval(channel1: str, channel2: str, interval_seconds: int) -> None:
        """Set interval between two channels"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                await db.execute(
                    """
                    INSERT OR REPLACE INTO channel_intervals 
                    (channel_id, next_channel_id, interval_seconds) 
                    VALUES (?, ?, ?)
                    """,
                    (channel1, channel2, interval_seconds)
                )
                await db.commit()
                logger.info(f"Установлен интервал между каналами {channel1} -> {channel2}: {interval_seconds} сек.")
        except Exception as e:
            logger.error(f"Ошибка при установке интервала между каналами {channel1} и {channel2}: {e}")

    @staticmethod
    async def get_channel_intervals() -> Dict[str, Dict[str, Any]]:
        """Get all channel intervals"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                async with db.execute(
                    "SELECT channel_id, next_channel_id, interval_seconds FROM channel_intervals"
                ) as cursor:
                    results = await cursor.fetchall()
                    return {row[0]: {"next_channel": row[1], "interval": row[2]} for row in results}
        except Exception as e:
            logger.error(f"Ошибка при получении интервалов между каналами: {e}")
            return {}

    @staticmethod
    async def delete_channel_interval(channel_id: str) -> None:
        """Delete interval for a channel"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                await db.execute(
                    "DELETE FROM channel_intervals WHERE channel_id = ?",
                    (channel_id,)
                )
                await db.commit()
                logger.info(f"Удален интервал для канала {channel_id}")
        except Exception as e:
            logger.error(f"Ошибка при удалении интервала для канала {channel_id}: {e}")

    @staticmethod
    async def get_stats() -> Dict[str, Any]:
        """Get forwarding statistics"""
        try:
            async with DatabaseConnectionPool.get_connection() as db:
                # Get total forwards
                async with db.execute("SELECT COUNT(*) FROM forward_stats") as cursor:
                    total = (await cursor.fetchone())[0]

                # Get last forward timestamp
                async with db.execute(
                    "SELECT timestamp FROM forward_stats ORDER BY timestamp DESC LIMIT 1"
                ) as cursor:
                    row = await cursor.fetchone()
                    last = row[0] if row else None

                # Get last messages for each channel
                async with db.execute(
                    "SELECT channel_id, message_id, timestamp FROM last_messages"
                ) as cursor:
                    last_msgs = {
                        row[0]: {"message_id": row[1], "timestamp": row[2]}
                        for row in await cursor.fetchall()
                    }

                return {
                    "total_forwards": total,
                    "last_forward": last,
                    "last_messages": last_msgs
                }
        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}")
            return {"total_forwards": 0, "last_forward": None, "last_messages": {}}
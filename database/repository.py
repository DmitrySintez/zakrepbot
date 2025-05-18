# Дополнительные правки для database/repository.py

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
    
    @classmethod
    async def close_all(cls):
        """Close all database connections"""
        for conn in cls._pool:
            try:
                await conn.close()
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
        cls._pool.clear()
    
    @classmethod
    @asynccontextmanager
    async def get_connection(cls):
        """Get a database connection from the pool"""
        config = Config()
        
        # Try to get an available connection
        for conn in cls._pool:
            if not getattr(conn, 'in_use', True):
                setattr(conn, 'in_use', True)
                try:
                    yield conn
                finally:
                    setattr(conn, 'in_use', False)
                return

        # Create new connection if pool not full
        max_connections = 5  # Maximum number of connections
        if len(cls._pool) < max_connections:
            conn = await aiosqlite.connect(config.db_path)
            setattr(conn, 'in_use', True)
            cls._pool.add(conn)
            try:
                yield conn
            finally:
                setattr(conn, 'in_use', False)
        else:
            # Wait for available connection
            while True:
                await asyncio.sleep(0.1)
                for conn in cls._pool:
                    if not getattr(conn, 'in_use', True):
                        setattr(conn, 'in_use', True)
                        try:
                            yield conn
                        finally:
                            setattr(conn, 'in_use', False)
                        return

class Repository:
    """Repository pattern implementation for database operations"""
    
    @staticmethod
    async def close_db() -> None:
        """Close all database connections"""
        await DatabaseConnectionPool.close_all()
    
    @staticmethod
    async def init_db() -> None:
        """Initialize database schema"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
                CREATE TABLE IF NOT EXISTS target_chats (
                    chat_id INTEGER PRIMARY KEY,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS forward_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS last_messages (
                    channel_id TEXT PRIMARY KEY,
                    message_id INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS pinned_messages (
                    chat_id TEXT PRIMARY KEY,
                    message_id INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_forward_stats_timestamp ON forward_stats(timestamp);
                CREATE INDEX IF NOT EXISTS idx_target_chats_added_at ON target_chats(added_at);
            """)
            await db.commit()
            
    @staticmethod
    async def get_target_chats() -> List[int]:
        """Get list of target chat IDs"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute("SELECT chat_id FROM target_chats") as cursor:
                return [row[0] for row in await cursor.fetchall()]

    @staticmethod
    async def add_target_chat(chat_id: int) -> None:
        """Add new target chat"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "INSERT OR IGNORE INTO target_chats (chat_id) VALUES (?)",
                (chat_id,)
            )
            await db.commit()

    @staticmethod
    async def remove_target_chat(chat_id: int) -> None:
        """Remove target chat"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "DELETE FROM target_chats WHERE chat_id = ?",
                (chat_id,)
            )
            await db.commit()

    @staticmethod
    async def get_config(key: str, default: Optional[str] = None) -> Optional[str]:
        """Get configuration value"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT value FROM config WHERE key = ?",
                (key,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else default

    @staticmethod
    async def set_config(key: str, value: str) -> None:
        """Set configuration value"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                (key, str(value))
            )
            await db.commit()

    @staticmethod
    async def log_forward(message_id: int) -> None:
        """Log forwarded message"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "INSERT INTO forward_stats (message_id) VALUES (?)",
                (message_id,)
            )
            await db.commit()

    @staticmethod
    async def save_last_message(channel_id: str, message_id: int) -> None:
        """Save last message ID for channel"""
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

    @staticmethod
    async def get_last_message(channel_id: str) -> Optional[int]:
        """Get last message ID for channel"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT message_id FROM last_messages WHERE channel_id = ?",
                (channel_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    @staticmethod
    async def get_all_last_messages() -> Dict[str, Dict[str, Any]]:
        """Get last message IDs for all channels"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT channel_id, message_id, timestamp FROM last_messages"
            ) as cursor:
                results = await cursor.fetchall()
                return {row[0]: {"message_id": row[1], "timestamp": row[2]} for row in results}

    @staticmethod
    async def get_latest_message() -> tuple:
        """Get the most recent message across all channels"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT channel_id, message_id, timestamp FROM last_messages ORDER BY timestamp DESC LIMIT 1"
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return (row[0], row[1])  # (channel_id, message_id)
                return (None, None)

    @staticmethod
    async def save_pinned_message(chat_id: str, message_id: int) -> None:
        """Save pinned message ID for chat"""
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

    @staticmethod
    async def get_pinned_message(chat_id: str) -> Optional[int]:
        """Get pinned message ID for chat"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT message_id FROM pinned_messages WHERE chat_id = ?",
                (chat_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    @staticmethod
    async def get_all_pinned_messages() -> Dict[str, int]:
        """Get all pinned messages"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT chat_id, message_id FROM pinned_messages"
            ) as cursor:
                results = await cursor.fetchall()
                return {row[0]: row[1] for row in results}

    @staticmethod
    async def delete_pinned_message(chat_id: str) -> None:
        """Delete pinned message record"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "DELETE FROM pinned_messages WHERE chat_id = ?",
                (chat_id,)
            )
            await db.commit()

    @staticmethod
    async def get_stats() -> Dict[str, Any]:
        """Get forwarding statistics"""
        async with DatabaseConnectionPool.get_connection() as db:
            # Get total forwards
            async with db.execute("SELECT COUNT(*) FROM forward_stats") as cursor:
                total = (await cursor.fetchone())[0]

            # Get last forward timestamp
            async with db.execute(
                "SELECT timestamp FROM forward_stats ORDER BY timestamp DESC LIMIT 1"
            ) as cursor:
                last = (await cursor.fetchone() or [None])[0]

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
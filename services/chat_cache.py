from datetime import datetime
from typing import Optional, Dict, List, Protocol
from dataclasses import dataclass
from aiogram import Bot
from utils.config import Config

@dataclass
class ChatInfo:
    """Data class for storing chat information"""
    id: int
    title: str
    type: str
    member_count: Optional[int] = None
    last_updated: float = 0.0

class CacheObserver(Protocol):
    """Protocol for cache update observers"""
    async def on_cache_update(self, chat_id: int, info: ChatInfo) -> None:
        """Called when chat info is updated in cache"""
        pass

class ChatCacheService:
    """Chat cache service with observer pattern"""
    _instance = None
    _cache: Dict[int, ChatInfo] = {}
    _observers: List[CacheObserver] = []
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ChatCacheService, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._config = Config()
    
    def add_observer(self, observer: CacheObserver) -> None:
        """Add observer for cache updates"""
        if observer not in self._observers:
            self._observers.append(observer)
    
    def remove_observer(self, observer: CacheObserver) -> None:
        """Remove observer"""
        if observer in self._observers:
            self._observers.remove(observer)
    
    async def _notify_observers(self, chat_id: int, info: ChatInfo) -> None:
        """Notify all observers about cache update"""
        for observer in self._observers:
            try:
                await observer.on_cache_update(chat_id, info)
            except Exception as e:
                from loguru import logger
                logger.error(f"Error notifying observer: {e}")
    
    async def get_chat_info(self, bot: Bot, chat_id: int) -> Optional[ChatInfo]:
        """Get chat info from cache or fetch from API"""
        now = datetime.now().timestamp()
        
        # Check cache first
        if chat_id in self._cache:
            chat_info = self._cache[chat_id]
            if now - chat_info.last_updated < self._config.cache_ttl:
                return chat_info

        try:
            # Fetch fresh data
            chat = await bot.get_chat(chat_id)
            
            # Попытка получить количество участников
            member_count = None
            try:
                member_count = await bot.get_chat_member_count(chat_id)
            except Exception as e:
                from loguru import logger
                logger.warning(f"Не удалось получить количество участников для чата {chat_id}: {e}")
            
            # Проверяем тип чата и права бота
            bot_id = (await bot.get_me()).id
            bot_member = None
            can_pin = False
            
            try:
                bot_member = await bot.get_chat_member(chat_id, bot_id)
                if bot_member.status == 'administrator':
                    can_pin = getattr(bot_member, 'can_pin_messages', False)
                    
                    if not can_pin:
                        from loguru import logger
                        logger.warning(f"Бот не имеет прав на закрепление сообщений в чате {chat_id}")
            except Exception as e:
                from loguru import logger
                logger.warning(f"Ошибка при проверке прав бота в чате {chat_id}: {e}")
            
            # Создаем объект информации о чате
            info = ChatInfo(
                id=chat_id,
                title=chat.title or f"Чат {chat_id}",
                type=chat.type,
                member_count=member_count,
                last_updated=now
            )
            
            # Update cache
            self._cache[chat_id] = info
            
            # Notify observers
            await self._notify_observers(chat_id, info)
            
            # Cleanup old entries if cache is too large
            if len(self._cache) > self._config.max_cache_size:
                oldest = min(self._cache.items(), key=lambda x: x[1].last_updated)
                del self._cache[oldest[0]]
            
            return info
        except Exception as e:
            from loguru import logger
            logger.error(f"Error fetching chat info for {chat_id}: {e}")
            return None
    
    def clear_cache(self) -> None:
        """Clear the entire cache"""
        self._cache.clear()
    
    def remove_from_cache(self, chat_id: int) -> None:
        """Remove specific chat from cache"""
        self._cache.pop(chat_id, None)

# Дополнительные правки для utils/config.py

import asyncio
import os
import json
from typing import List, Optional
from dotenv import load_dotenv
from loguru import logger

class Config:
    """Singleton для конфигурации"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        load_dotenv()
        
        # Загружаем переменные окружения
        self.bot_token: str = os.getenv("BOT_TOKEN", "")
        
        # Загружаем ID администраторов из списка, разделенного запятыми
        admin_ids_str = os.getenv("ADMIN_IDS", os.getenv("OWNER_ID", ""))
        self.admin_ids: List[int] = []
        
        if admin_ids_str:
            try:
                # Разделяем по запятым и конвертируем в целые числа
                self.admin_ids = [int(id.strip()) for id in admin_ids_str.split(',') if id.strip()]
            except ValueError as e:
                logger.error(f"Ошибка при парсинге ID администраторов: {e}")
                # В случае ошибки используем пустой список
                self.admin_ids = []
        
        # Для обратной совместимости - сохраняем первого администратора как owner_id
        self.owner_id: int = self.admin_ids[0] if self.admin_ids else 0
        
        self.source_channels: List[str] = []
        
        # Поддержка обратной совместимости - добавляем начальный канал источника, если он указан
        initial_source = os.getenv("SOURCE_CHANNEL", "").lstrip('@')
        if initial_source:
            self.source_channels.append(initial_source)
            
        self.db_path: str = os.getenv("DB_PATH", "forwarder.db")
        
        # Пытаемся загрузить дополнительные каналы из конфигурационного файла
        self._load_channels_from_config()
        
        # Проверяем обязательные настройки
        if not all([self.bot_token, self.admin_ids]):
            raise ValueError("Отсутствуют обязательные переменные окружения")
        
        # Настройки кэша
        self.cache_ttl: int = 300  # 5 минут кэширования для информации о чатах
        self.max_cache_size: int = 100
        
        # Настройки соединений с базой данных
        self.max_db_connections: int = 5
        
        self._initialized = True
    
    def is_admin(self, user_id: int) -> bool:
        """Проверка, является ли пользователь администратором"""
        return user_id in self.admin_ids
    
    def _load_channels_from_config(self):
        """Загрузка каналов из конфигурационного файла"""
        try:
            with open('bot_config.json', 'r') as f:
                config = json.load(f)
                if 'source_channels' in config and isinstance(config['source_channels'], list):
                    # Добавляем каналы, которых еще нет в списке
                    for channel in config['source_channels']:
                        channel = str(channel).lstrip('@')
                        if channel and channel not in self.source_channels:
                            self.source_channels.append(channel)
        except (FileNotFoundError, json.JSONDecodeError):
            # Создаем конфигурацию по умолчанию, если она не существует
            self._save_channels_to_config()
    
    def _save_channels_to_config(self):
        """Сохранение каналов в конфигурационный файл"""
        try:
            config = {}
            # Сначала пытаемся загрузить существующую конфигурацию
            try:
                with open('bot_config.json', 'r') as f:
                    config = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                config = {"source_channels": [], "target_chats": [], "last_message_ids": {}}
            
            # Обновляем каналы источники
            config['source_channels'] = self.source_channels
            
            # Сохраняем обновленную конфигурацию
            with open('bot_config.json', 'w') as f:
                json.dump(config, f, indent=4)
        except Exception as e:
            logger.error(f"Не удалось сохранить каналы в конфиг: {e}")
            
    def add_source_channel(self, channel: str) -> bool:
        """Добавление нового канала источника и сохранение в конфиг"""
        channel = channel.lstrip('@')
        if channel and channel not in self.source_channels:
            self.source_channels.append(channel)
            self._save_channels_to_config()
            return True
        return False
        
    def remove_source_channel(self, channel: str) -> bool:
        """Удаление канала источника и обновление конфига"""
        channel = channel.lstrip('@')
        if channel in self.source_channels:
            self.source_channels.remove(channel)
            self._save_channels_to_config()
            return True
        return False
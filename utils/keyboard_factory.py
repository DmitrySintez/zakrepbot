# Дополнительные правки для utils/keyboard_factory.py

from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import Dict, List, Any

class KeyboardFactory:
    """Factory Pattern implementation for creating keyboards"""
    
    @staticmethod
    def create_main_keyboard(running: bool = False) -> Any:
        """Create main menu keyboard"""
        kb = InlineKeyboardBuilder()
        kb.button(
            text="🔄 Запустить ротацию" if not running else "⏹ Остановить ротацию",
            callback_data="toggle_forward"
        )
        kb.button(text="⏱️ Установить интервал", callback_data="set_interval")
        kb.button(text="⚙️ Управление каналами", callback_data="channels")
        kb.button(text="🤖 Клонировать бота", callback_data="clone_bot")
        kb.button(text="👥 Управление клонами", callback_data="manage_clones")
        kb.button(text="💬 Список целевых чатов", callback_data="list_chats")
        kb.button(text="📌 Немедленная пересылка", callback_data="forward_now")
        kb.adjust(2)
        return kb.as_markup()

    @staticmethod
    def create_rotation_interval_keyboard() -> Any:
        """Create interval selection keyboard for rotation"""
        kb = InlineKeyboardBuilder()
        intervals = [
            ("30м", 1800), ("1ч", 3600), ("2ч", 7200), 
            ("3ч", 10800), ("6ч", 21600), ("12ч", 43200), 
            ("24ч", 86400)
        ]
        for label, seconds in intervals:
            kb.button(text=label, callback_data=f"set_interval_value_{seconds}")
        kb.button(text="Другой...", callback_data="set_interval_value_custom")
        kb.button(text="Назад", callback_data="back_to_main")
        kb.adjust(4)
        return kb.as_markup()

    @staticmethod
    def create_chat_list_keyboard(chats: Dict[int, str]) -> Any:
        """Create chat list keyboard with remove buttons and pin test buttons"""
        kb = InlineKeyboardBuilder()
        for chat_id, title in chats.items():
            kb.button(
                text=f"📌 Тест закрепления в {title}",
                callback_data=f"test_pin_{chat_id}"
            )
            kb.button(
                text=f"❌ Удалить {title}",
                callback_data=f"remove_{chat_id}"
            )
        kb.button(text="Назад", callback_data="back_to_main")
        kb.adjust(1)
        return kb.as_markup()

    @staticmethod
    def create_channel_management_keyboard(channels: List[str]) -> Any:
        """Create channel management keyboard"""
        kb = InlineKeyboardBuilder()
        kb.button(text="➕ Добавить канал", callback_data="add_channel")
        
        # Add buttons for each channel
        for channel in channels:
            # Truncate channel name if too long
            display_name = channel[:15] + "..." if len(channel) > 18 else channel
            
            # Button only for removing channels
            kb.button(
                text=f"❌ Удалить ({display_name})",
                callback_data=f"remove_channel_{channel}"
            )
        
        kb.button(text="Назад", callback_data="back_to_main")
        kb.adjust(1)
        return kb.as_markup()
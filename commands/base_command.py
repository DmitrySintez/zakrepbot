from abc import ABC, abstractmethod
from aiogram import types
from utils.config import Config

class Command(ABC):
    """Base command class implementing Command Pattern"""
    
    def __init__(self):
        self.config = Config()
    
    async def execute(self, message: types.Message) -> None:
        """Execute the command if user has permission"""
        if not self.config.is_admin(message.from_user.id):
            return
        await self._handle(message)
    
    @abstractmethod
    async def _handle(self, message: types.Message) -> None:
        """Implementation of command handling"""
        pass
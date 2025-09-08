"""Простая заглушка для command_processor для обратной совместимости."""

import logging
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

class CommandProcessor:
    """Простой обработчик команд."""
    
    async def process_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Обрабатывает обновления. Возвращает False чтобы передать обработку дальше."""
        return False

# Глобальный экземпляр
command_processor = CommandProcessor()

def setup_command_processor():
    """Настройка обработчика команд."""
    logger.info("Command processor настроен")
    pass
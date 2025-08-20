# Создайте файл command_processor.py
import logging
import asyncio
import re
from typing import Dict, Any, Optional, List, Callable, Coroutine, Pattern, Tuple, Union
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

# Тип для обработчика команды
CommandHandler = Callable[[Update, ContextTypes.DEFAULT_TYPE, List[str]], Coroutine[Any, Any, None]]

class CommandProcessor:
    """
    Обработчик команд пользователя с поддержкой регулярных выражений.
    """
    
    def __init__(self):
        # Словарь команд
        self.commands: Dict[str, CommandHandler] = {}
        
        # Словарь регулярных выражений
        self.patterns: List[Tuple[Pattern, CommandHandler]] = []
        
        logger.info("Обработчик команд инициализирован")
    
    def register_command(self, command: str, handler: CommandHandler) -> None:
        """
        Регистрирует обработчик для команды.
        
        Args:
            command: Команда (без '/')
            handler: Функция-обработчик
        """
        self.commands[command] = handler
        logger.info(f"Зарегистрирована команда: {command}")
    
    def register_pattern(self, pattern: Union[str, Pattern], handler: CommandHandler) -> None:
        """
        Регистрирует обработчик для регулярного выражения.
        
        Args:
            pattern: Регулярное выражение или его строковое представление
            handler: Функция-обработчик
        """
        # Если pattern - строка, компилируем регулярное выражение
        if isinstance(pattern, str):
            compiled_pattern = re.compile(pattern)
        else:
            compiled_pattern = pattern
            
        self.patterns.append((compiled_pattern, handler))
        logger.info(f"Зарегистрирован шаблон: {pattern}")
    
    async def process_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """
        Обрабатывает обновление от Telegram.
        
        Args:
            update: Обновление
            context: Контекст
            
        Returns:
            True, если обновление обработано, иначе False
        """
        if not update.message or not update.message.text:
            return False
        
        text = update.message.text.strip()
        
        try:
            # Проверяем, является ли сообщение командой
            if text.startswith('/'):
                # Разбиваем на части
                parts = text[1:].split(None, 1)
                command = parts[0].lower()
                
                # Проверяем, есть ли обработчик для этой команды
                if command in self.commands:
                    # Получаем аргументы
                    args = parts[1].split() if len(parts) > 1 else []
                    
                    # Вызываем обработчик
                    await self.commands[command](update, context, args)
                    return True
            
            # Если это не команда или нет обработчика, проверяем регулярные выражения
            for pattern, handler in self.patterns:
                match = pattern.search(text)
                
                if match:
                    # Получаем группы
                    groups = list(match.groups())
                    
                    # Вызываем обработчик
                    await handler(update, context, groups)
                    return True
            
            # Если ничего не совпало, возвращаем False
            return False
            
        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

# Создаем глобальный экземпляр обработчика команд
command_processor = CommandProcessor()

# Регистрируем обработчики команд
async def handle_token_query(update: Update, context: ContextTypes.DEFAULT_TYPE, args: List[str]) -> None:
    """
    Обрабатывает запрос токена.
    
    Args:
        update: Обновление
        context: Контекст
        args: Аргументы команды
    """
    query = args[0] if args else update.message.text.strip()
    
    # Отправляем сообщение о поиске
    msg = await update.message.reply_text(f"Ищу информацию о токене: {query}...")
    
    # Получаем информацию о токене
    from token_service import get_token_info
    result = await get_token_info(query, update.message.chat_id, None, context)
    
    # Удаляем сообщение о поиске, если получили результат
    if result:
        try:
            await msg.delete()
        except Exception as e:
            logger.error(f"Ошибка при удалении сообщения: {e}")

# Регистрируем обработчики шаблонов
async def handle_contract_message(update: Update, context: ContextTypes.DEFAULT_TYPE, groups: List[str]) -> None:
    """
    Обрабатывает сообщение с адресом контракта.
    
    Args:
        update: Обновление
        context: Контекст
        groups: Группы из регулярного выражения
    """
    contract = groups[0]
    
    # Отправляем сообщение о поиске
    msg = await update.message.reply_text(f"Обрабатываю контракт: {contract}...")
    
    # Обрабатываем адрес контракта
    from token_service import process_token_address
    await process_token_address(contract, update.message.chat_id, context)
    
    # Удаляем сообщение о поиске
    try:
        await msg.delete()
    except Exception as e:
        logger.error(f"Ошибка при удалении сообщения: {e}")

# Регистрируем обработчики
def setup_command_processor():
    """Настраивает обработчик команд."""
    # Импортируем функции из test_bot_commands
    from test_bot_commands import (
        start,
        help_command,
        list_tokens,
        clear_tokens,
        stats_command
    )
    
    # Регистрируем команды
    command_processor.register_command("start", lambda u, c, a: start(u, c))
    command_processor.register_command("help", lambda u, c, a: help_command(u, c))
    command_processor.register_command("list", lambda u, c, a: list_tokens(u, c))
    command_processor.register_command("clear", lambda u, c, a: clear_tokens(u, c))
    command_processor.register_command("stats", lambda u, c, a: stats_command(u, c))
    command_processor.register_command("token", handle_token_query)
    
    # Регистрируем шаблоны
    # Шаблон для адреса контракта
    contract_pattern = r'\b([a-zA-Z0-9]{32,44})\b'
    command_processor.register_pattern(contract_pattern, handle_contract_message)
    
    # Шаблон для сообщения "Контракт: <адрес>"
    contract_msg_pattern = r'(?:Контракт|Contract):\s*([a-zA-Z0-9]{32,44})'
    command_processor.register_pattern(contract_msg_pattern, handle_contract_message)
    
    logger.info("Обработчик команд настроен")
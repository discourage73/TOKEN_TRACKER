# Создайте файл logging_config.py
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

def setup_logging(app_name, log_level=logging.INFO):
    """
    Настраивает систему логирования для всего приложения.
    
    Args:
        app_name: Имя приложения для логов
        log_level: Уровень логирования
    """
    # Создаем директорию для логов, если она не существует
    logs_dir = 'logs'
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Генерируем уникальное имя файла с датой
    timestamp = datetime.now().strftime("%Y%m%d")
    log_file = f"{logs_dir}/{app_name}_{timestamp}.log"
    
    # Настраиваем корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Удаляем существующие обработчики
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Создаем обработчик для файла с ротацией
    # Максимальный размер файла - 10 МБ, хранить до 10 файлов
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,
        backupCount=10,
        encoding='utf-8'
    )
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(log_level)
    
    # Создаем обработчик для консоли
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)  # Для консоли уровень может быть выше
    
    # Добавляем обработчики к корневому логгеру
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Настраиваем логгеры для часто используемых библиотек
    for module_name in ['asyncio', 'telethon', 'httpx', 'telegram', 'requests']:
        module_logger = logging.getLogger(module_name)
        module_logger.setLevel(logging.WARNING)  # Для сторонних библиотек используем WARNING
    
    # Возвращаем настроенный логгер для основного модуля
    app_logger = logging.getLogger(app_name)
    app_logger.info(f"Логирование настроено. Файл логов: {log_file}")
    
    return app_logger

# Пример функции для получения именованного логгера
def get_logger(module_name):
    """
    Возвращает логгер для конкретного модуля.
    
    Args:
        module_name: Имя модуля
        
    Returns:
        Настроенный логгер
    """
    return logging.getLogger(f"token_bot.{module_name}")
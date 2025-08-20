import os
from dotenv import load_dotenv
from typing import Final, List
import logging

# Загружаем переменные из .env
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.WARNING
)

logger = logging.getLogger(__name__)

# Получаем конфиденциальные данные из .env
TELEGRAM_TOKEN: Final = os.getenv('TELEGRAM_TOKEN')
API_ID: Final = int(os.getenv('API_ID'))
API_HASH: Final = os.getenv('API_HASH')

# Парсим список админов из строки
admin_ids_str = os.getenv('ADMIN_IDS', '')
CONTROL_ADMIN_IDS: List[int] = [int(id.strip()) for id in admin_ids_str.split(',') if id.strip()]

# Остальные настройки
DEXSCREENER_API_URL: Final = os.getenv('DEXSCREENER_API_URL')
TARGET_BOT: Final = os.getenv('TARGET_BOT')
TARGET_CHANNEL: Final = os.getenv('TARGET_CHANNEL')

# Парсим список ботов-источников
source_bots_str = os.getenv('SOURCE_BOTS', '')
SOURCE_BOTS: List[str] = [bot.strip() for bot in source_bots_str.split(',') if bot.strip()]

# Проверяем, что все критически важные переменные загружены
required_vars = ['TELEGRAM_TOKEN', 'API_ID', 'API_HASH']
missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    raise ValueError(f"Отсутствуют обязательные переменные в .env: {missing_vars}")

logger.info("Конфигурация успешно загружена из .env файла")
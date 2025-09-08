from telethon import TelegramClient, events
import asyncio
import logging
import sys
import re
import os
import time
import signal
import traceback
from datetime import datetime
from message_parsers import parser_factory


# Исправляем кодировку для Windows
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        # Для Python 3.6, который не имеет метода reconfigure
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer)

# Импортируем конфигурацию
try:
    from config import (API_ID, API_HASH, SOURCE_BOTS, TARGET_CHANNEL, logger)
    # Используем логгер из config.py
    USING_CONFIG_LOGGER = True
except ImportError:
    # Значения для независимого запуска (замените на свои!)
    API_ID = 25308063
    API_HASH = "458e1315175e0103f19d925204b690a5"
    SOURCE_BOTS = ["TheMobyBot", "ray_cyan_bot"]
    TARGET_CHANNEL = "cringemonke"
    
    USING_CONFIG_LOGGER = False
    # Создаем директорию для логов, если она не существует
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"logs/forwarder_{datetime.now().strftime('%Y%m%d-%H%M%S')}.log", encoding='utf-8'),
        ]
    )

    # Добавляем собственный обработчик для консоли
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger = logging.getLogger(__name__)
    logger.addHandler(console_handler)
    
    logger.warning("Не удалось импортировать config.py, используются встроенные значения")

# Глобальная переменная для отслеживания статуса работы
is_running = True


async def extract_and_format_ray_cyan_data(message):
    """
    Извлекает и форматирует ключевую информацию из сообщений ray_cyan_bot.
    Возвращает отформатированный текст с полным адресом кошелька.
    """
    if not hasattr(message, 'text') or not message.text:
        return None
        
    text = message.text
    
    # Проверяем, что это сообщение о покупке токена
    if "BUY" not in text:
        # Если это не сообщение о покупке, возвращаем None
        return None
    
    try:
        # Извлекаем название токена (без скобки)
        buy_match = re.search(r'BUY ([^\s\(\)]+)', text)
        token_name = buy_match.group(1) if buy_match else "UNKNOWN"
        
        # Убедимся, что в токене нет закрывающей скобки
        if token_name.endswith("]"):
            token_name = token_name[:-1]
        
        # Извлекаем полный адрес кошелька
        full_wallet = None
        
        # Ищем в ссылках solscan
        solscan_match = re.search(r'https://solscan.io/account/([a-zA-Z0-9]{32,})', text)
        
        if solscan_match:
            full_wallet = solscan_match.group(1)
            logger.info(f"Найден полный адрес кошелька из URL solscan: {full_wallet}")
        
        # Если не нашли в URL, ищем строки, которые могут быть кошельками
        if not full_wallet:
            # Ищем строки с "swapped"
            swap_lines = [line for line in text.split('\n') if "swapped" in line]
            
            if swap_lines:
                for line in swap_lines:
                    # Ищем кошелек в начале строки с "swapped"
                    swap_match = re.search(r'^(\s*[a-zA-Z0-9]+\S+)\s+swapped', line)
                    
                    if swap_match:
                        # Извлекаем полное имя перед "swapped"
                        wallet_prefix = swap_match.group(1).strip()
                        
                        # Ищем полный адрес, соответствующий этому префиксу
                        for potential_addr in re.findall(r'[a-zA-Z0-9]{40,}', text):
                            if potential_addr.startswith(wallet_prefix[:5]):
                                full_wallet = potential_addr
                                break
            
            # Если все еще не нашли, ищем любую достаточно длинную строку, которая выглядит как кошелек
            if not full_wallet:
                # Ищем в строках текста
                for line in text.split('\n'):
                    wallet_matches = re.findall(r'([a-zA-Z0-9]{40,})', line)
                    
                    for wallet in wallet_matches:
                        # Проверяем, что это не похоже на Contract (не последняя строка)
                        if wallet and line != text.split('\n')[-1]:
                            full_wallet = wallet
                            break
        
        # Если все еще не нашли, ищем длинные строки перед строками с метаданными
        if not full_wallet:
            # Ищем строки с метаданными, которые обычно в конце
            metadata_patterns = [r'#\w+ \|', r'MC:', r'Seen:']
            
            for i, line in enumerate(text.split('\n')):
                if any(re.search(pattern, line) for pattern in metadata_patterns):
                    # Ищем кошелек в предыдущих строках
                    for prev_line in text.split('\n')[:i]:
                        wallet_matches = re.findall(r'([a-zA-Z0-9]{40,})', prev_line)
                        
                        if wallet_matches:
                            full_wallet = wallet_matches[0]
                            break
        
        # Извлекаем адрес контракта 
        # Это обычно длинная строка в последней строке или рядом с метаданными
        contract_address = ""
        
        # Проверяем последнюю строку на наличие контракта
        lines = text.split('\n')
        
        if lines and lines[-1].strip():
            last_line = lines[-1].strip()
            
            if re.match(r'^[a-zA-Z0-9]{30,}$', last_line) and (not full_wallet or full_wallet != last_line):
                contract_address = last_line
                logger.info(f"Найден адрес контракта в последней строке: {contract_address}")
        
        # Если не нашли в последней строке, ищем рядом с метаданными
        if not contract_address:
            # Ищем строки с метаданными
            for i, line in enumerate(lines):
                if re.search(r'#\w+ \|', line) or re.search(r'MC:', line) or re.search(r'Seen:', line):
                    # Проверяем предыдущую строку
                    if i > 0 and re.match(r'^[a-zA-Z0-9]{30,}$', lines[i-1].strip()):
                        contract_address = lines[i-1].strip()
                        break
        
        # Если все еще не нашли, ищем в любом месте текста длинную строку, 
        # которая не совпадает с кошельком
        if not contract_address:
            for line in text.split('\n'):
                contract_matches = re.findall(r'([a-zA-Z0-9]{30,})', line)
                
                for contract in contract_matches:
                    if contract and (not full_wallet or full_wallet != contract):
                        contract_address = contract
                        break
        
        # Форматируем сообщение по требуемому шаблону с пробелами и построчным разделением
        formatted_text = f"""🟢 BUY {token_name}"""
        
        # Добавляем строку с кошельком, если нашли
        if full_wallet:
            formatted_text += f"\nSmart money : {full_wallet}"
        
        # Добавляем адрес контракта, если нашли
        if contract_address:
            formatted_text += f"\n{contract_address}"
        
        # Для отладки показываем, что мы нашли
        logger.info(f"Найдено - Токен: {token_name}, Кошелек: {full_wallet}, Contract: {contract_address}")
        
        return formatted_text
    except Exception as e:
        logger.error(f"Ошибка при извлечении данных из сообщения ray_cyan_bot: {e}")
        logger.error(traceback.format_exc())
        return None

async def extract_and_format_whale_alerts(message):
    """
    Извлекает и форматирует информацию из сообщений о китах (Whale Alerts).
    Возвращает отформатированный текст с только необходимой информацией.
    Обрабатывает только сообщения с "just bought", игнорирует "just sold".
    """
    if not hasattr(message, 'text') or not message.text:
        return None
        
    text = message.text
    
    # Проверяем, что это сообщение о покупке кита (только "just bought")
    if "New Token Whale Alert" in text and "just bought" in text and "just sold" not in text:
        try:
            # Извлекаем основную информацию о покупке кита (с учетом разных форматов из скриншотов)
            whale_info_match = re.search(r'(A .+? Whale just bought \$[\d.]+[KMB]? of .+?)(?=\(|\n|$)', text)
            whale_info = whale_info_match.group(1).strip() if whale_info_match else ""
            
            # Если не нашли информацию о ките, значит, это не интересующее нас сообщение
            if not whale_info:
                return None
            
            # Проверяем, есть ли "just sold" в тексте сообщения (дополнительная проверка)
            if "just sold" in whale_info:
                return None
                
            # Извлекаем информацию о маркет капе
            mc_match = re.search(r'\(MC:?\s*\$([\d.]+[KMB]?)\)', text)
            mc_info = f"(MC: ${mc_match.group(1)})" if mc_match else ""
            
            # Извлекаем адрес контракта - ищем длинную строку из букв и цифр
            contract_match = re.search(r'([A-Za-z0-9]{30,})', text)
            contract_address = contract_match.group(1) if contract_match else ""
            
            # Форматируем сообщение по требуемому шаблону
            formatted_text = f"""New Token Whale Alert
🟢 {whale_info} {mc_info}

{contract_address}"""
            
            return formatted_text
            
        except Exception as e:
            logger.error("Ошибка при извлечении данных из сообщения о ките: {}".format(e))
            logger.error(traceback.format_exc())
            return None
    
    # Если это не сообщение о покупке кита, возвращаем None
    return None

async def start_forwarding():
    """Основная функция для запуска пересылки сообщений."""
    logger.info("Сервис пересылки сообщений запущен!")
    
    # Подключаемся к Telegram (оптимизированный для SOURCE_BOTS только)
    client = TelegramClient('forwarder_session', API_ID, API_HASH)
    
    try:
        await client.start()
        logger.info("✅ Подключение к Telegram установлено")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Telegram: {e}")
        raise
    
    # Отправляем приветственные сообщения
    try:
        # Отправляем в основной канал для TheMobyBot
        await client.send_message(
            TARGET_CHANNEL, 
            f"🔄 Сервис пересылки сообщений запущен и отслеживает бота: @TheMobyBot"
        )
        logger.info("Приветственное сообщение отправлено в канал @{}".format(TARGET_CHANNEL))
        
        # Отправляем в канал для ray_cyan_bot
        await client.send_message(
            "cringemonke2", 
            f"🔄 Сервис пересылки сообщений запущен и отслеживает бота: @ray_cyan_bot"
        )
        logger.info("Приветственное сообщение отправлено в канал @cringemonke2")
    except Exception as e:
        logger.error("Ошибка при отправке приветственного сообщения: {}".format(e))
        
    # Небольшая пауза перед началом основной работы
    await asyncio.sleep(3)
    
    # Регистрируем обработчик ТОЛЬКО для отслеживаемых ботов (ОПТИМИЗАЦИЯ ПАМЯТИ!)
    @client.on(events.NewMessage(from_users=SOURCE_BOTS))
    async def handler(event):
        if not is_running:
            return
            
        try:
            # Получаем имя отправителя (уже отфильтровано по SOURCE_BOTS)
            sender = event.sender
            sender_username = sender.username if sender else "Unknown"
            
            logger.info(f"Получено сообщение от @{sender_username}")
            
            # Получаем текст сообщения
            if hasattr(event.message, 'text') and event.message.text:
                text = event.message.text
                
                # Получаем подходящий парсер
                parser = parser_factory.get_parser(text)
                
                if parser:
                    # Парсим сообщение
                    parsed_data = parser.parse(text)
                    
                    if parsed_data:
                        # Форматируем сообщение
                        formatted_text = parser.format(parsed_data)
                        
                        if formatted_text:
                            # Определяем целевой канал
                            target_channel = TARGET_CHANNEL
                            
                            # Для ray_cyan_bot используем другой канал
                            if sender_username == "ray_cyan_bot":
                                target_channel = "cringemonke2"
                            
                            # Отправляем сообщение
                            try:
                                await client.send_message(target_channel, formatted_text)
                                logger.info(f"Сообщение от @{sender_username} переслано в @{target_channel}")
                            except Exception as e:
                                logger.error(f"Ошибка при пересылке сообщения: {e}")
                                logger.error(traceback.format_exc())
                    else:
                        logger.info(f"Не удалось распарсить сообщение от @{sender_username}")
                else:
                    # Используем старую логику обработки если парсер не найден
                    if sender_username == "TheMobyBot":
                        logger.info("Получено сообщение от @TheMobyBot")
                        
                        # ИСПРАВЛЕНИЕ: Проверяем сначала, содержит ли сообщение "just sold"
                        # Если да, то полностью игнорируем его
                        if "just sold" in text:
                            logger.info("Сообщение содержит 'just sold', игнорируем")
                            return
                        
                        # Проверяем, является ли сообщение сообщением о ките
                        formatted_text = await extract_and_format_whale_alerts(event.message)
                        
                        if formatted_text:
                            logger.info("Обнаружено сообщение о ките, применяем форматирование")
                        else:
                            logger.info("Сообщение не распознано как сообщение о ките, пересылаем как есть")
                        
                        # Если не удалось отформатировать или это не сообщение о ките, используем оригинальный текст
                        text_to_send = formatted_text if formatted_text else text
                        
                        try:
                            await client.send_message(TARGET_CHANNEL, text_to_send)
                            logger.info("Сообщение от @TheMobyBot переслано в @{}".format(TARGET_CHANNEL))
                        except Exception as e:
                            logger.error("Ошибка при пересылке сообщения от TheMobyBot: {}".format(e))
                            logger.error(traceback.format_exc())
                    
                    elif sender_username == "ray_cyan_bot":
                        logger.info("Получено сообщение от @ray_cyan_bot")
                        
                        # Применяем фильтр и форматирование для ray_cyan_bot
                        formatted_text = await extract_and_format_ray_cyan_data(event.message)
                        
                        # Если не удалось отформатировать, используем оригинальный текст
                        text_to_send = formatted_text if formatted_text else text
                        
                        try:
                            await client.send_message("cringemonke2", text_to_send)
                            logger.info("Сообщение от @ray_cyan_bot переслано в @cringemonke2")
                        except Exception as e:
                            logger.error("Ошибка при пересылке сообщения от ray_cyan_bot: {}".format(e))
                            logger.error(traceback.format_exc())
            
        except Exception as e:
            logger.error("Ошибка при обработке сообщения: {}".format(e))
            logger.error(traceback.format_exc())
    
    # Держим соединение активным до получения сигнала остановки
    try:
        logger.info("Ожидание сообщений от ботов...")
        while is_running:
            await asyncio.sleep(60)
            logger.info("Сервис активен, ожидание сообщений...")
    except KeyboardInterrupt:
        logger.info("Сервис остановлен пользователем")
    except Exception as e:
        logger.error("Error in основном цикле: {}".format(e))
        logger.error(traceback.format_exc())
    finally:
        await client.disconnect()
        logger.info("Соединение закрыто") 

# Обработчик сигнала прерывания
def signal_handler(sig, frame):
    global is_running
    logger.info("Получен сигнал прерывания, выполняется выход...")
    print("Остановка сервиса пересылки сообщений...")
    is_running = False

if __name__ == "__main__":
    try:
        # Регистрируем обработчик прерывания
        signal.signal(signal.SIGINT, signal_handler)
        
        # Решение проблемы с event loop в новых версиях Python
        if sys.version_info >= (3, 10):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        # Запускаем сервис
        asyncio.run(start_forwarding())
    except Exception as e:
        print("Критическая ошибка при запуске: {}".format(e))
        print(traceback.format_exc())
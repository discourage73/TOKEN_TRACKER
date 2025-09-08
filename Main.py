import logging
import sys
import os
import signal
import time
import asyncio
import subprocess

from logging_config import setup_logging

# Creating директорию для логов, если она не существует
if not os.path.exists('logs'):
    os.makedirs('logs')

# Getting текущую временную метку для имен файлов
timestamp = time.strftime("%Y%m%d-%H%M%S")

# Глобальный уровень логирования: можно изменить на INFO для более подробных логов
LOG_LEVEL = "WARNING"  # Доступные варианты: DEBUG, INFO, WARNING, ERROR, CRITICAL

# Настройка логирования
# Файловый обработчик для детальных логов
file_handler = logging.FileHandler(f'logs/main_{timestamp}.log', encoding='utf-8')
file_handler.setLevel(logging.INFO)  # В file пишем все информационные messages
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Настройка логирования

# Настраиваем простой логгер без лишних сообщений
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger('main')


# Процессы и задачи для управления компонентами
bot_process = None
tracker_task = None
forwarder_task = None
stop_event = asyncio.Event()

def configure_system_logging():
    """Централизованная настройка логирования для всей системы."""
    # Настройка корневого логгера
    root_logger = logging.getLogger()
    
    # Deleting существующие обработчики
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # ОТКЛЮЧЕНО: Файловый обработчик для all_components (дублирует другие логи)
    # timestamp = time.strftime("%Y%m%d-%H%M%S")
    # all_log_file = f'logs/all_components_{timestamp}.log'
    # all_handler = logging.FileHandler(all_log_file, encoding='utf-8')
    
    # Setting минимальное логирование
    root_logger.setLevel(logging.WARNING)
    
    # Настройка популярных модулей
    for module_name in ['asyncio', 'telethon', 'httpx', 'telegram', 'config']:
        module_logger = logging.getLogger(module_name)
        module_logger.setLevel(logging.INFO)
    
    logger.info("Настроено упрощенное логирование (all_components disabled)")

async def init_system_database():
    """Инициализация базы данных системы (ОБНОВЛЕНО: автоинициализация tracker DB)."""
    try:
        # МИГРАЦИЯ: Tracker DB инициализируется автоматически при первом использовании
        # Вместо создания старой базы tokens_database.db, просто проверим tracker DB
        
        import os
        tracker_db_path = "tokens_tracker_database.db"
        
        if os.path.exists(tracker_db_path):
            logger.info(f"Tracker DB найдена: {tracker_db_path}")
        else:
            logger.info(f"Tracker DB будет создана при первом использовании: {tracker_db_path}")
        
        # ВАЖНО: НЕ создаём старую базу tokens_database.db
        logger.info("database системы готова к работе (tracker DB)")
        return True
        
    except Exception as e:
        logger.error(f"Error при проверке базы данных: {e}")
        # Позволяем системе запуститься даже при ошибках
        return True

async def shutdown_system():
    """Graceful shutdown всех системных компонентов."""
    try:
        logger.info("Начало выключения системы...")
        
        # Stopping планировщик
        from task_scheduler import scheduler
        scheduler.stop()
        logger.info("Планировщик Stopped")
        
        # HTTP client больше не используется (удален)
        logger.info("HTTP client закрыт")
        
        logger.info("system Success выключена")
        
    except Exception as e:
        logger.error(f"Error при завершении работы системы: {e}")

def run_solana_tracker_subprocess():
    """НЕ ИСПОЛЬЗУЕТСЯ: Функция для запуска solana_tracker в отдельном процессе."""
    global tracker_process
    
    try:
        logger.info("Запуск solana_tracker в отдельном процессе (КРИТИЧЕСКАЯ ОПТИМИЗАЦИЯ ПАМЯТИ)...")
        
        # Creating переменные окружения
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["LOG_LEVEL"] = LOG_LEVEL

        # Starting solana_tracker в отдельном процессе
        tracker_process = subprocess.Popen(
            [sys.executable, "-c", "from solana_contract_tracker import main; import asyncio; asyncio.run(main())"],
            env=env
        )
        
        logger.info(f"Solana tracker Started в отдельном процессе с PID: {tracker_process.pid}")
        return tracker_process
        
    except Exception as e:
        logger.error(f"Error при запуске solana_tracker: {e}")
        return None

def run_message_forwarder_subprocess():
    """НЕ ИСПОЛЬЗУЕТСЯ: Функция для запуска message_forwarder в отдельном процессе."""
    global forwarder_process
    
    try:
        logger.info("Запуск message_forwarder в отдельном процессе (ОПТИМИЗАЦИЯ ПАМЯТИ)...")
        
        # Creating переменные окружения
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["LOG_LEVEL"] = LOG_LEVEL

        # Starting forwarder в отдельном процессе
        forwarder_process = subprocess.Popen(
            [sys.executable, "-c", "from message_forwarder import start_forwarding; import asyncio; asyncio.run(start_forwarding())"],
            env=env
        )
        
        logger.info(f"Message forwarder Started в отдельном процессе с PID: {forwarder_process.pid}")
        return True
        
    except Exception as e:
        logger.error(f"Error при запуске message_forwarder: {e}")
        return False

def run_telegram_bot():
    """Запускает Telegram бота в отдельном процессе."""
    global bot_process
    
    try:
        logger.info("Запуск Telegram бота в отдельном процессе...")
        
        # Creating переменные окружения для бота
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["LOG_LEVEL"] = LOG_LEVEL

        # ОТКЛЮЧЕНО: Отдельный лог file для бота (дублирует token_bot.log)
        # bot_log_file = f'logs/test_bot4_{timestamp}.log'
        
        # Starting bot в отдельном процессе БЕЗ отдельного лог файла
        bot_process = subprocess.Popen(
            [sys.executable, "bot.py"],
            env=env
        )
        
        logger.info(f"Telegram bot Started с PID: {bot_process.pid}")
        return True
        
    except Exception as e:
        logger.error(f"Error при запуске бота: {e}")
        return False

async def run_solana_tracker():
    """Запускает отслеживание контрактов Solana."""
    global tracker_task
    
    try:
        # Saving оригинальные дескрипторы
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        
        # ОТКЛЮЧЕНО: Отдельный лог file для solana (не нужен)
        # solana_log_file = f'logs/solana_output_{timestamp}.log'
        # solana_log_handle = None
            
        # Модифицируем переменные окружения для контроля логирования
        os.environ["LOG_LEVEL"] = LOG_LEVEL
        
        # Импортируем функцию main из solana_contract_tracker
        from solana_contract_tracker import main
        
        logger.info("Запуск отслеживания контрактов Solana...")
        print("[INFO] Отслеживание контрактов Solana Started")
        
        # Creating задачу для отслеживания
        tracker_task = asyncio.create_task(main())
        
        # Ждем завершения задачи или сигнала остановки
        await stop_event.wait()
        
        # Если трекер все еще работает, отменяем его
        if tracker_task and not tracker_task.done():
            logger.info("Остановка трекера контрактов Solana...")
            tracker_task.cancel()
            try:
                await tracker_task
            except asyncio.CancelledError:
                logger.info("Трекер Stopped")
                
    except Exception as e:
        logger.error(f"Error при запуске трекера контрактов: {e}")
        print(f"[Error] failed to запустить отслеживание контрактов: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
    finally:
        # ОБЯЗАТЕЛЬНО восстанавливаем дескрипторы
        sys.stdout = original_stdout
        sys.stderr = original_stderr

async def run_message_forwarder():
    """Функция для запуска сервиса пересылки сообщений."""
    global forwarder_task
    
    try:
        # Saving оригинальные дескрипторы
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        
        # ОТКЛЮЧЕНО: Отдельный лог file для forwarder (не нужен)
        # forwarder_log_file = f'logs/forwarder_output_{timestamp}.log'
        # forwarder_log_handle = None
            
        # Модифицируем переменные окружения для контроля логирования
        os.environ["LOG_LEVEL"] = LOG_LEVEL
        
        # Импортируем функцию start_forwarding из message_forwarder
        from message_forwarder import start_forwarding
        
        logger.info("Запуск сервиса пересылки сообщений...")
        print("[INFO] Сервис пересылки сообщений Started")
        
        # Creating задачу для пересылки сообщений
        forwarder_task = asyncio.create_task(start_forwarding())
        
        # ВАЖНО: Даём time на инициализацию и отправку уведомлений
        await asyncio.sleep(15)
        
        # Только после паузы ждем завершения задачи или сигнала остановки
        await stop_event.wait()
        
        # Если задача все еще работает, отменяем её
        if forwarder_task and not forwarder_task.done():
            logger.info("Остановка сервиса пересылки сообщений...")
            forwarder_task.cancel()
            try:
                await forwarder_task
            except asyncio.CancelledError:
                logger.info("Сервис пересылки сообщений Stopped")
                
    except Exception as e:
        logger.error(f"Error при запуске сервиса пересылки сообщений: {e}")
        print(f"[Error] failed to запустить сервис пересылки сообщений: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
    finally:
        # ОБЯЗАТЕЛЬНО восстанавливаем дескрипторы
        sys.stdout = original_stdout
        sys.stderr = original_stderr

def signal_handler(sig, frame):
    """Обработчик сигнала прерывания."""
    logger.info("received сигнал прерывания, выполняется выход...")
    print("\n[INFO] Завершение работы системы...")
    
    # Stopping процессы
    global bot_process, stop_event
    
    # Setting событие остановки для асинхронных задач
    try:
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(stop_event.set)
    except:
        pass
    
    # Stopping процесс бота если он Started
    if bot_process and bot_process.poll() is None:
        logger.info("Остановка процесса бота...")
        try:
            bot_process.terminate()
            # Даем процессу time на корректное завершение
            time.sleep(1)
            if bot_process.poll() is None:
                bot_process.kill()
        except:
            pass
    
    # Немного ждем перед завершением программы
    time.sleep(1)
    logger.info("Выход из программы")
    sys.exit(0)

async def main():
    """Основная асинхронная функция программы."""
    try:
        print("[INFO] Запуск системы...")
        
        # Настраиваем централизованное логирование
        configure_system_logging()
        
        # Initializing базу данных
        if not await init_system_database():
            logger.error("failed to инициализировать базу данных")
            return
        
        # Starting тестовый bot (subprocess)
        if not run_telegram_bot():
            logger.error("failed to запустить Telegram бота")
            return
        
        # Даем time для инициализации
        await asyncio.sleep(5)
        
        # Starting другие компоненты параллельно
        tasks = [
            asyncio.create_task(run_solana_tracker()),
            asyncio.create_task(run_message_forwarder())
        ]
        
        # Starting задачи параллельно
        print("[INFO] Запуск компонентов системы...")
        await asyncio.gather(*tasks)
        
    except KeyboardInterrupt:
        logger.info("Программа остановлена пользователем")
        print("[INFO] Программа остановлена пользователем")
    except Exception as e:
        logger.error(f"Непредвиденная Error: {e}")
        print(f"[Error] Непредвиденная Error: {e}")
    finally:
        # Graceful shutdown всех компонентов
        await shutdown_system()


if __name__ == "__main__":
    # Регистрируем обработчик сигнала прерывания
    signal.signal(signal.SIGINT, signal_handler)
    
    # Настройка для Windows, если необходимо
    if sys.version_info >= (3, 8) and sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # Starting программу
    try:
        print("[INFO] system запущена. Нажмите Ctrl+C для выхода.")
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Завершение работы...")
    finally:
        print("[INFO] Программа завершена.")
import logging
import sys
import os
import signal
import time
import asyncio
import subprocess

from logging_config import setup_logging
logger = setup_logging('token_bot')

# Создаем директорию для логов, если она не существует
if not os.path.exists('logs'):
    os.makedirs('logs')

# Получаем текущую временную метку для имен файлов
timestamp = time.strftime("%Y%m%d-%H%M%S")

# Глобальный уровень логирования: можно изменить на INFO для более подробных логов
LOG_LEVEL = "WARNING"  # Доступные варианты: DEBUG, INFO, WARNING, ERROR, CRITICAL

# Настройка логирования
# Файловый обработчик для детальных логов
file_handler = logging.FileHandler(f'logs/main_{timestamp}.log', encoding='utf-8')
file_handler.setLevel(logging.INFO)  # В файл пишем все информационные сообщения
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Настройка логирования

logger = setup_logging('main')


# Процессы и задачи для управления компонентами
bot_process = None
tracker_task = None
forwarder_task = None
stop_event = asyncio.Event()

def configure_system_logging():
    """Централизованная настройка логирования для всей системы."""
    # Настройка корневого логгера
    root_logger = logging.getLogger()
    
    # Удаляем существующие обработчики
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Файловый обработчик для всех логов
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    all_log_file = f'logs/all_components_{timestamp}.log'
    all_handler = logging.FileHandler(all_log_file, encoding='utf-8')
    all_handler.setLevel(logging.INFO)
    all_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    all_handler.setFormatter(all_formatter)
    
    # Добавляем обработчик
    root_logger.addHandler(all_handler)
    root_logger.setLevel(logging.INFO)
    
    # Настройка популярных модулей
    for module_name in ['asyncio', 'telethon', 'httpx', 'telegram', 'config']:
        module_logger = logging.getLogger(module_name)
        module_logger.setLevel(logging.INFO)
    
    logger.info(f"Настроено логирование всех компонентов")
    logger.info(f"Объединенный лог всех компонентов: {all_log_file}")

async def init_system_database():
    """Инициализация базы данных системы."""
    try:
        import db_storage
        conn = db_storage.get_db_connection()
        db_storage.init_database(conn)
        conn.close()
        logger.info("База данных инициализирована")
        return True
    except Exception as e:
        logger.error(f"Ошибка инициализации БД: {e}")
        return False

async def shutdown_system():
    """Graceful shutdown всех системных компонентов."""
    try:
        logger.info("Начало выключения системы...")
        
        # Останавливаем планировщик
        from task_scheduler import scheduler
        scheduler.stop()
        logger.info("Планировщик остановлен")
        
        # Закрываем HTTP клиент
        from http_client import http_client
        http_client.close()
        logger.info("HTTP клиент закрыт")
        
        logger.info("Система успешно выключена")
        
    except Exception as e:
        logger.error(f"Ошибка при завершении работы системы: {e}")

def run_telegram_bot():
    """Запускает Telegram бота в отдельном процессе."""
    global bot_process
    
    try:
        logger.info("Запуск Telegram бота в отдельном процессе...")
        
        # Создаем переменные окружения для бота
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["LOG_LEVEL"] = LOG_LEVEL

        # Создаем имя файла лога для бота
        bot_log_file = f'logs/test_bot4_{timestamp}.log'
        
        # Запускаем бот в отдельном процессе
        with open(bot_log_file, 'a', encoding='utf-8') as bot_log:
            bot_process = subprocess.Popen(
                [sys.executable, "test_bot4.py"],
                stdout=bot_log,
                stderr=bot_log,
                env=env
            )
        
        logger.info(f"Telegram бот запущен с PID: {bot_process.pid}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
        return False

async def run_solana_tracker():
    """Запускает отслеживание контрактов Solana."""
    global tracker_task
    
    try:
        # Сохраняем оригинальные дескрипторы
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        
        solana_log_file = f'logs/solana_output_{timestamp}.log'
        
        # Открываем файл БЕЗ контекстного менеджера
        solana_log_handle = None
        
        try:
            # Открываем файл
            solana_log_handle = open(solana_log_file, 'a', encoding='utf-8')
            
            # Перенаправляем stdout и stderr
            sys.stdout = solana_log_handle
            sys.stderr = solana_log_handle
            
            # Модифицируем переменные окружения для контроля логирования
            os.environ["LOG_LEVEL"] = LOG_LEVEL
            
            # Импортируем функцию main из solana_contract_tracker
            from solana_contract_tracker import main
            
            # Возвращаем стандартный вывод основному процессу СРАЗУ после импорта
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            
            logger.info("Запуск отслеживания контрактов Solana...")
            print("[INFO] Отслеживание контрактов Solana запущено")
            
            # Создаем задачу для отслеживания
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
                    logger.info("Трекер остановлен")
                    
        finally:
            # ОБЯЗАТЕЛЬНО восстанавливаем дескрипторы
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            
            # Закрываем файл в блоке finally
            if solana_log_handle:
                try:
                    solana_log_handle.flush()  # Сбрасываем буфер
                    solana_log_handle.close()
                except Exception as close_error:
                    logger.warning(f"Ошибка при закрытии файла лога: {close_error}")
            
    except Exception as e:
        logger.error(f"Ошибка при запуске трекера контрактов: {e}")
        print(f"[ОШИБКА] Не удалось запустить отслеживание контрактов: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Убеждаемся, что стандартный вывод восстановлен
        sys.stdout = original_stdout
        sys.stderr = original_stderr

async def run_message_forwarder():
    """Функция для запуска сервиса пересылки сообщений."""
    global forwarder_task
    
    try:
        # Сохраняем оригинальные дескрипторы
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        
        forwarder_log_file = f'logs/forwarder_output_{timestamp}.log'
        
        # Открываем файл БЕЗ контекстного менеджера
        forwarder_log_handle = None
        
        try:
            # Открываем файл
            forwarder_log_handle = open(forwarder_log_file, 'a', encoding='utf-8')
            
            # Перенаправляем stdout и stderr
            sys.stdout = forwarder_log_handle
            sys.stderr = forwarder_log_handle
            
            # Модифицируем переменные окружения для контроля логирования
            os.environ["LOG_LEVEL"] = LOG_LEVEL
            
            # Импортируем функцию start_forwarding из message_forwarder
            from message_forwarder import start_forwarding
            
            # Возвращаем стандартный вывод основному процессу СРАЗУ после импорта
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            
            logger.info("Запуск сервиса пересылки сообщений...")
            print("[INFO] Сервис пересылки сообщений запущен")
            
            # Создаем задачу для пересылки сообщений
            forwarder_task = asyncio.create_task(start_forwarding())
            
            # ВАЖНО: Даём время на инициализацию и отправку уведомлений
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
                    logger.info("Сервис пересылки сообщений остановлен")
                    
        finally:
            # ОБЯЗАТЕЛЬНО восстанавливаем дескрипторы
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            
            # Закрываем файл в блоке finally
            if forwarder_log_handle:
                try:
                    forwarder_log_handle.flush()  # Сбрасываем буфер
                    forwarder_log_handle.close()
                except Exception as close_error:
                    logger.warning(f"Ошибка при закрытии файла лога форвардера: {close_error}")
            
    except Exception as e:
        logger.error(f"Ошибка при запуске сервиса пересылки сообщений: {e}")
        print(f"[ОШИБКА] Не удалось запустить сервис пересылки сообщений: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Убеждаемся, что стандартный вывод восстановлен
        sys.stdout = original_stdout
        sys.stderr = original_stderr

def signal_handler(sig, frame):
    """Обработчик сигнала прерывания."""
    logger.info("Получен сигнал прерывания, выполняется выход...")
    print("\n[INFO] Завершение работы системы...")
    
    # Останавливаем процессы
    global bot_process, stop_event
    
    # Устанавливаем событие остановки для асинхронных задач
    try:
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(stop_event.set)
    except:
        pass
    
    # Останавливаем процесс бота если он запущен
    if bot_process and bot_process.poll() is None:
        logger.info("Остановка процесса бота...")
        try:
            bot_process.terminate()
            # Даем процессу время на корректное завершение
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
        
        # Инициализируем базу данных
        if not await init_system_database():
            logger.error("Не удалось инициализировать базу данных")
            return
        
        # Запускаем тестовый бот (subprocess)
        if not run_telegram_bot():
            logger.error("Не удалось запустить Telegram бота")
            return
        
        # Даем время для инициализации
        await asyncio.sleep(5)
        
        # Запускаем другие компоненты параллельно
        tasks = [
            asyncio.create_task(run_solana_tracker()),
            asyncio.create_task(run_message_forwarder())
        ]
        
        # Запускаем задачи параллельно
        print("[INFO] Запуск компонентов системы...")
        await asyncio.gather(*tasks)
        
    except KeyboardInterrupt:
        logger.info("Программа остановлена пользователем")
        print("[INFO] Программа остановлена пользователем")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка: {e}")
        print(f"[ОШИБКА] Непредвиденная ошибка: {e}")
    finally:
        # Graceful shutdown всех компонентов
        await shutdown_system()


if __name__ == "__main__":
    # Регистрируем обработчик сигнала прерывания
    signal.signal(signal.SIGINT, signal_handler)
    
    # Настройка для Windows, если необходимо
    if sys.version_info >= (3, 8) and sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # Запускаем программу
    try:
        print("[INFO] Система запущена. Нажмите Ctrl+C для выхода.")
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Завершение работы...")
    finally:
        print("[INFO] Программа завершена.")
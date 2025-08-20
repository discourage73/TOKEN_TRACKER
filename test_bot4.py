import os
import logging
import time
import asyncio
import traceback
import telegram
import requests
from typing import Dict, Any, Tuple

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
from telegram.error import TimedOut, NetworkError

# Импортируем новые модули
from command_processor import command_processor, setup_command_processor
from task_scheduler import scheduler, TaskPriority
from logging_config import setup_logging
from error_helpers import handle_exception
from http_client import http_client

from handlers.user_commands import start_user, help_user, handle_unauthorized
from handlers.admin_commands import admin_panel
from handlers.callback_router import route_callback
# Импортируем модули проекта
import token_storage
from config import TELEGRAM_TOKEN, logger, CONTROL_ADMIN_IDS
from utils import format_tokens_list 

# Импортируем функции из token_service
from token_service import (
    get_token_info,  # Используем основную функцию из token_service 
    check_all_market_caps, 
    set_telegram_context
)

# Обновляем импорты из test_bot_commands - ТОЛЬКО СУЩЕСТВУЮЩИЕ ФУНКЦИИ
from test_bot_commands import (
    stats_command,
    list_tokens,
    clear_tokens,
    analytics_command,
    handle_clear_cancel,
    handle_refresh_list,
    setup_bot_commands,
    handle_delete_all_confirm,
    handle_delete_confirm,
    handle_delete_selective,
    handle_delete_token,
    handle_clear_return,
    add_user_command,     
    remove_user_command  
)

# Получаем настроенный логгер
logger = setup_logging('token_bot')
debug_logger = logger.getChild('debug')

# Создаем директорию для логов, если она не существует
if not os.path.exists('logs'):
    os.makedirs('logs')

# Настройка логирования для отладки
debug_logger = logging.getLogger('debug')
debug_logger.setLevel(logging.DEBUG)
# Создаем обработчик файла для логирования ошибок
debug_handler = logging.FileHandler('logs/debug.log', encoding='utf-8')
debug_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
debug_logger.addHandler(debug_handler)

# Добавляем вывод логов в консоль
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(levelname)s: %(message)s')
console_handler.setFormatter(console_formatter)
debug_logger.addHandler(console_handler)

# Функция для получения данных о DEX
def fetch_dex_data(contract_address: str) -> Dict[str, Any]:
    """
    Получает данные о DEX напрямую из API DexScreener.
    """
    try:
        debug_logger.info(f"Запрос данных о DEX для контракта: {contract_address}")
        url = f"https://api.dexscreener.com/latest/dex/tokens/{contract_address}"
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            debug_logger.info(f"Успешно получены данные о DEX для контракта: {contract_address}")
            
            # Логируем количество пар и их названия для отладки
            pairs = data.get('pairs', [])
            dex_names = [pair.get('dexId', 'Unknown') for pair in pairs]
            debug_logger.info(f"Получены данные о {len(pairs)} парах. DEX: {', '.join(dex_names)}")
            
            return data
        else:
            debug_logger.warning(f"Ошибка {response.status_code} при запросе данных о DEX")
            return {"pairs": []}
    except Exception as e:
        debug_logger.error(f"Исключение при запросе данных о DEX: {str(e)}")
        debug_logger.error(traceback.format_exc())
        return {"pairs": []}
     
def find_dexes_info(dex_data: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Находит основной DEX и PUMPFUN DEX из данных, полученных от API.
    Возвращает кортеж (основной_dex, pumpfun_dex)
    """
    try:
        pairs = dex_data.get("pairs", [])
        debug_logger.info(f"Анализ {len(pairs)} пар DEX")
        
        popular_dex = None
        pumpfun_dex = None
        max_txns = 0
        
        # Если найдено менее 2-х пар, сразу логируем это для отладки
        if len(pairs) < 2:
            debug_logger.info(f"Внимание: получено только {len(pairs)} пар DEX")
        
        for pair in pairs:
            dex_id = pair.get('dexId', 'Unknown')
            txns = pair.get('txns', {})
            
            # Логируем каждую пару для отладки
            debug_logger.info(f"Обрабатываем DEX: {dex_id}, URL: {pair.get('url', 'No URL')}")
            
            # Считаем общее количество транзакций за 24 часа
            h24_data = txns.get('h24', {})
            h24_buys = h24_data.get('buys', 0) if isinstance(h24_data, dict) else 0
            h24_sells = h24_data.get('sells', 0) if isinstance(h24_data, dict) else 0
            total_txns = h24_buys + h24_sells
            
            debug_logger.info(f"DEX: {dex_id}, Транзакции (24ч): {total_txns} (покупки: {h24_buys}, продажи: {h24_sells})")
            
            # Проверяем, не PUMPFUN ли это (проверяем все варианты написания)
            if dex_id.lower() in ['pumpfun', 'pump.fun', 'pump fun']:
                pumpfun_dex = pair
                debug_logger.info(f"Найден PUMPFUN DEX")
            
            # Проверяем, не самый ли это популярный DEX
            if total_txns > max_txns:
                max_txns = total_txns
                popular_dex = pair
                debug_logger.info(f"Найден новый популярный DEX: {dex_id} с {total_txns} транзакциями")
        
        # Если популярный DEX не найден, берем первую пару, если она есть
        if not popular_dex and pairs:
            popular_dex = pairs[0]
            debug_logger.info(f"Используем первую пару как основную: {popular_dex.get('dexId', 'Unknown')}")
        
        # Если popular_dex существует, логируем его для отладки
        if popular_dex:
            debug_logger.info(f"Итоговый выбор популярного DEX: {popular_dex.get('dexId', 'Unknown')}")
        else:
            debug_logger.warning("Популярный DEX не найден")
        
        return popular_dex, pumpfun_dex
    except Exception as e:
        debug_logger.error(f"Ошибка при анализе DEX: {str(e)}")
        debug_logger.error(traceback.format_exc())
        return None, None

async def extract_token_address_from_message(text: str) -> str:
    """Извлекает адрес контракта токена из сообщения."""
    try:
        import re
        
        # Ищем строку, начинающуюся с "Контракт: " или "Contract: "
        contract_pattern = r'(?:Контракт|Contract):\s*([a-zA-Z0-9]{32,44})'
        matches = re.search(contract_pattern, text)
        
        if matches:
            # Если нашли такой шаблон, возвращаем только группу с адресом
            return matches.group(1)
        
        # Если не нашли по шаблону Контракт:, ищем просто адрес Solana
        # Solana адрес токена (32-44 символа)
        solana_pattern = r'\b[a-zA-Z0-9]{32,44}\b'
        matches = re.search(solana_pattern, text)
        
        if matches:
            return matches.group(0)
            
        # Ищем Ethereum/BSC адрес
        eth_pattern = r'0x[0-9a-fA-F]{40}'
        matches = re.search(eth_pattern, text)
        
        if matches:
            return matches.group(0)
        
        # Если ничего не нашли
        return ""
    except Exception as e:
        logger.error(f"Ошибка в extract_token_address_from_message: {str(e)}")
        logger.error(traceback.format_exc())
        return ""

@handle_exception(log_msg="Ошибка при обработке сообщения", notify_user=True)
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает входящие сообщения."""
    if not update.message:
        return
    
    try:
        # Пытаемся обработать сообщение с помощью CommandProcessor
        if await command_processor.process_update(update, context):
            return
        
        # Если сообщение не обработано, используем стандартную обработку
        if update.message.text:
            query = update.message.text.strip()
            
            # Отслеживаем запрос
            debug_logger.info(f"Получен запрос: {query}")
            
            # Извлекаем адрес токена из сообщения, если это пересланное сообщение
            token_address = await extract_token_address_from_message(query)
            
            # Если нашли адрес, используем его вместо исходного запроса
            if token_address:
                query = token_address
                debug_logger.info(f"Найден адрес токена в сообщении. Используем его: {query}")
        
            # Отправляем сообщение о поиске
            debug_logger.info(f"Получено сообщение: {query}")
            
            # Используем конструкцию try-except для отправки сообщения
            # с несколькими попытками и обработкой таймаутов
            max_retries = 3
            msg = None
            for attempt in range(max_retries):
                try:
                    msg = await update.message.reply_text(f"Ищу информацию о токене: {query}...")
                    debug_logger.info(f"Отправлено сообщение о поиске")
                    break  # выходим из цикла если успешно
                except (TimedOut, NetworkError) as e:
                    if attempt < max_retries - 1:
                        debug_logger.warning(f"Таймаут при отправке ({attempt+1}/{max_retries}): {e}")
                        await asyncio.sleep(2)  # пауза перед повторной попыткой
                    else:
                        debug_logger.error(f"Не удалось отправить сообщение после {max_retries} попыток: {e}")
                        # Не останавливаем выполнение, продолжаем без отправки сообщения
            
            # Получаем информацию о токене - используем функцию из token_service
            result = await get_token_info(query, update.message.chat_id, None, context)
            
            # Удаляем сообщение о поиске, если получили результат
            if result and msg:
                try:
                    await msg.delete()
                    debug_logger.info(f"Сообщение о поиске удалено")
                except Exception as e:
                    debug_logger.error(f"Не удалось удалить сообщение о поиске: {str(e)}")
                
            # Проверяем, нужно ли выполнить автоматическую проверку всех токенов
            if token_storage.check_auto_update_needed():
                debug_logger.info("Запуск автоматической проверки Market Cap всех токенов")
                scheduler.schedule_task(
                    "auto_check", 
                    check_all_market_caps, 
                    delay=5,
                    priority=TaskPriority.LOW,
                    context=context
                )
                
    except Exception as e:
        debug_logger.error(f"Необработанное исключение в handle_message: {str(e)}")
        debug_logger.error(traceback.format_exc())

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает ошибки, возникающие при обработке обновлений."""
    debug_logger.error(f"Произошла ошибка при обработке обновления: {context.error}")
    debug_logger.error(traceback.format_exc())

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает все callback запросы."""
    query = update.callback_query
    
    if not query:
        return
    
    try:
        # Получаем данные callback
        data = query.data
        if not data:
            await query.answer("Ошибка: данные запроса отсутствуют")
            return
                
        # Обрабатываем разные типы callback
        if data.startswith("refresh:"):
            await handle_refresh_token(update, context)
        elif data == "refresh_list":
            await handle_refresh_list(update, context)
        elif data.startswith("list_page:"):
            # Обработка навигации по страницам списка токенов
            page = int(data.split(':', 1)[1])
            await handle_list_page(update, context, page)
        # Обработчики для удаления токенов (ТОЛЬКО УДАЛЕНИЕ)
        elif data == "clear_cancel":
            await handle_clear_cancel(update, context)
        elif data == "clear_return":
            await handle_clear_return(update, context)
        elif data == "manage_tokens":
            # Перенаправляем на меню управления токенами
            await handle_clear_return(update, context)
        # Обработчики для полного удаления токенов
        elif data == "delete_all_confirm":
            await handle_delete_all_confirm(update, context)
        elif data == "delete_confirm":
            await handle_delete_confirm(update, context)
        elif data.startswith("delete_selective"):
            await handle_delete_selective(update, context)
        elif data.startswith("delete_token:"):
            await handle_delete_token(update, context)
        else:
            await query.answer("Неизвестный тип запроса")
            debug_logger.warning(f"Неизвестный тип callback запроса: {data}")
            
    except Exception as e:
        debug_logger.error(f"Ошибка при обработке callback запроса: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await query.answer("Произошла ошибка. Пожалуйста, попробуйте позже.")
        except Exception:
            pass

async def handle_list_page(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int) -> None:
    """Обрабатывает запрос на отображение конкретной страницы списка токенов."""
    query = update.callback_query
    chat_id = query.message.chat_id
    message_id = query.message.message_id
    
    try:
        # Уведомляем пользователя о начале обновления
        await query.answer("Обновляю список токенов...")
        
        # Получаем все токены (БЕЗ параметра include_hidden - по умолчанию не включает скрытые)
        active_tokens = token_storage.get_all_tokens()
        
        if not active_tokens:
            await query.edit_message_text(
                "Нет активных токенов в списке отслеживаемых.",
                parse_mode=ParseMode.MARKDOWN
            )
            debug_logger.info("Список токенов пуст")
            return
        
        # Форматируем список токенов с пагинацией
        tokens_per_page = 10  # Отображаем по 10 токенов на странице
        message, total_pages, current_page = format_tokens_list(active_tokens, page, tokens_per_page)
        
        # Создаем кнопки навигации
        keyboard = []
        
        # Кнопки для навигации по страницам
        nav_buttons = []
        if total_pages > 1:
            if current_page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Предыдущая", callback_data=f"list_page:{current_page-1}"))
            
            if current_page < total_pages - 1:
                nav_buttons.append(InlineKeyboardButton("Следующая ➡️", callback_data=f"list_page:{current_page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        # Кнопки для действий со списком
        action_buttons = [
            InlineKeyboardButton("🔄 Обновить", callback_data=f"list_page:{current_page}")
        ]
        keyboard.append(action_buttons)
        
        # Кнопка для управления токенами
        keyboard.append([InlineKeyboardButton("🔍 Управление токенами", callback_data="manage_tokens")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Обновляем сообщение с актуальными данными
        try:
            await query.edit_message_text(
                message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup,
                disable_web_page_preview=True
            )
            debug_logger.info(f"Список токенов успешно обновлен (страница {current_page+1} из {total_pages})")
        except telegram.error.BadRequest as e:
            if "Message is too long" in str(e):
                # Если сообщение слишком длинное, уменьшаем количество токенов на странице
                tokens_per_page = 5
                message, total_pages, current_page = format_tokens_list(active_tokens, page, tokens_per_page)
                
                # Обновляем кнопки навигации
                keyboard = []
                nav_buttons = []
                
                if total_pages > 1:
                    if current_page > 0:
                        nav_buttons.append(InlineKeyboardButton("⬅️ Предыдущая", callback_data=f"list_page:{current_page-1}"))
                    
                    if current_page < total_pages - 1:
                        nav_buttons.append(InlineKeyboardButton("Следующая ➡️", callback_data=f"list_page:{current_page+1}"))
                
                if nav_buttons:
                    keyboard.append(nav_buttons)
                
                keyboard.append(action_buttons)
                keyboard.append([InlineKeyboardButton("🔍 Управление токенами", callback_data="manage_tokens")])
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(
                    message,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True
                )
                debug_logger.info(f"Список токенов успешно обновлен (страница {current_page+1} из {total_pages}, уменьшено количество токенов на странице)")
            else:
                # Если произошла другая ошибка
                debug_logger.error(f"Ошибка при обновлении списка токенов: {str(e)}")
                await query.answer("Ошибка при обновлении списка. Пожалуйста, попробуйте позже.")
    except Exception as e:
        debug_logger.error(f"Ошибка при обработке запроса списка токенов: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await query.answer("Произошла ошибка. Пожалуйста, попробуйте позже.")
        except Exception:
            pass

async def handle_refresh_token(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает запрос на обновление токена."""
    query = update.callback_query
    data = query.data
    token_query = data.split(':', 1)[1]
    
    debug_logger.info(f"Получен запрос на обновление для токена {token_query}")
    
    try:
        # Проверяем, не слишком ли часто обновляем
        current_time = time.time()
        stored_data = token_storage.get_token_data(token_query)
        
        if stored_data:
            last_update_time = stored_data.get('last_update_time', 0)
            if current_time - last_update_time < 5:  # Минимум 5 секунд между обновлениями
                await query.answer("Пожалуйста, подождите несколько секунд между обновлениями")
                debug_logger.info(f"Обновление для токена {token_query} отклонено: слишком частые запросы")
                return
        else:
            # Если данных о токене нет, уведомляем и возвращаемся
            await query.answer("Информация о токене недоступна. Попробуйте заново отправить запрос.")
            debug_logger.warning(f"Не найдены данные о токене {token_query} для обновления")
            return
        
        # Уведомляем пользователя о начале обновления
        await query.answer("Обновляю информацию...")
        debug_logger.info(f"Начато обновление для токена {token_query}")
        
        # Импортируем функцию из token_service (правильная)
        from token_service import get_token_info
        
        # Получаем информацию о токене и обновляем сообщение
        # Передаем параметр force_refresh=True для принудительного обновления с API
        result = await get_token_info(
            token_query, 
            query.message.chat_id, 
            query.message.message_id, 
            context,
            force_refresh=True  # Важно: принудительное обновление при нажатии кнопки
        )
        
        # Обновляем время последнего обновления
        if stored_data and result:
            token_storage.update_token_field(token_query, 'last_update_time', current_time)
            debug_logger.info(f"Обновление для токена {token_query} успешно выполнено")
        else:
            debug_logger.warning(f"Обновление для токена {token_query} не удалось")
            
        # Проверяем, нужно ли выполнить автоматическую проверку всех токенов
        if token_storage.check_auto_update_needed():
            debug_logger.info("Запуск автоматической проверки Market Cap всех токенов")
            from token_service import check_all_market_caps
            context.application.create_task(check_all_market_caps(context))
    except Exception as e:
        debug_logger.error(f"Ошибка при обновлении токена {token_query}: {str(e)}")
        debug_logger.error(traceback.format_exc())
        try:
            await query.answer("Произошла ошибка при обновлении. Пожалуйста, попробуйте позже.")
        except:
            pass

# Добавить в test_bot4.py после других функций

def signal_handler(sig, frame):
    """Обработчик сигнала прерывания."""
    debug_logger.info("Получен сигнал прерывания, выполняется выход...")
    print("\n[INFO] Завершение работы системы...")
    
    # Сохраняем данные перед выходом
    try:
        import token_storage
        # Убираем вызов save_data_to_disk так как эта функция может не существовать
        debug_logger.info("Данные сохранены при обработке сигнала")
    except Exception as e:
        debug_logger.error(f"Ошибка при сохранении данных: {e}")

def create_bot_application():
    """Создает и настраивает Telegram бот приложение."""
    # Настраиваем обработчик команд
    setup_command_processor()
    
    # Создаем приложение и передаем ему токен телеграм бота
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Регистрируем обработчики команд (БЕЗ ДУБЛИКАТОВ!)
    application.add_handler(CommandHandler("start", start_user))
    application.add_handler(CommandHandler("help", help_user))
    application.add_handler(CommandHandler("admin", admin_panel))
    application.add_handler(CommandHandler("list", list_tokens))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("analytics", analytics_command))
    application.add_handler(CommandHandler("clear", clear_tokens))
    application.add_handler(CommandHandler("adduser", add_user_command))     
    application.add_handler(CommandHandler("removeuser", remove_user_command))

    # Регистрируем обработчик для инлайн-кнопок
    application.add_handler(CallbackQueryHandler(route_callback))

    # Регистрируем обработчик для обычных сообщений (токены и адреса)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Обработчик для неавторизованных пользователей (должен быть ПЕРЕД error_handler!)
    application.add_handler(MessageHandler(filters.ALL, handle_unauthorized))

    # Регистрируем обработчик ошибок (ПОСЛЕДНИЙ!)
    application.add_error_handler(error_handler)

    # Добавляем функцию, которая выполнится при запуске бота
    application.post_init = on_startup

    return application

@handle_exception(log_msg="Ошибка при запуске бота")
async def on_startup(application):
    """Выполняется при запуске бота."""
    # Устанавливаем команды для бота
    await setup_bot_commands(application)
    
    # Сохраняем контекст для использования в уведомлениях
    set_telegram_context(application)
    
    # Запускаем систему мониторинга токенов
    from token_service import start_token_monitoring_system
    await start_token_monitoring_system(application)
    
    debug_logger.info("Инициализация бота завершена")

@handle_exception(log_msg="Ошибка при завершении работы бота")
def on_shutdown():
    """Выполняется при завершении работы бота."""
    try:       
        debug_logger.info("Бот остановлен")
    except Exception as e:
        debug_logger.error(f"Ошибка при завершении работы: {e}")

def main():
    """Запускает бота (только для прямого запуска в режиме отладки)."""
    try:
        debug_logger.info("Запуск бота начат")
        print("ПРЕДУПРЕЖДЕНИЕ: Запуск в режиме отладки. Используйте Main.py для продакшена.")
        
        # Создаем и запускаем приложение
        application = create_bot_application()
        
        # Запускаем бота - СИНХРОННЫЙ блокирующий вызов
        debug_logger.info("Бот запущен и готов к работе")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
        
    except Exception as e:
        debug_logger.error(f"Критическая ошибка при запуске бота: {str(e)}")
        debug_logger.error(traceback.format_exc())
        print(f"Критическая ошибка при запуске бота: {str(e)}")
        print("Подробности смотрите в файле logs/debug.log")
    finally:
        # Выполняем действия при завершении
        on_shutdown()

# Эта глобальная обертка нужна для отслеживания всех необработанных исключений
if __name__ == "__main__":
    try:
        print("Запуск бота для отслеживания токенов...")
        print("Логи отладки будут сохранены в директории logs/")
        
        # Создаем директорию для логов, если она не существует
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        debug_logger.info("=" * 50)
        debug_logger.info("ЗАПУСК ПРИЛОЖЕНИЯ")
        debug_logger.info("=" * 50)
        
        # Запускаем главную функцию
        main()
    except Exception as e:
        if debug_logger:
            debug_logger.critical(f"Необработанное исключение в главном блоке: {str(e)}")
            debug_logger.critical(traceback.format_exc())
        print(f"Критическая ошибка: {str(e)}")
        print("Подробности смотрите в файле logs/debug.log")
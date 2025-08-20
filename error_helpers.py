import functools
import logging
import traceback
import asyncio
from typing import Callable, TypeVar, Any

T = TypeVar('T')
logger = logging.getLogger(__name__)

def handle_exception(
    log_msg: str = None, 
    return_value: Any = None, 
    notify_user: bool = False
) -> Callable:
    """
    Декоратор для единообразной обработки исключений.
    
    Args:
        log_msg: Сообщение для логирования (помимо исключения)
        return_value: Значение, возвращаемое в случае исключения
        notify_user: Отправлять ли уведомление пользователю
    
    Returns:
        Декорированная функция
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Формируем сообщение об ошибке
                error_message = f"Ошибка в {func.__name__}: {str(e)}"
                if log_msg:
                    error_message = f"{log_msg}: {error_message}"
                
                # Логируем ошибку
                logger.error(error_message)
                logger.error(traceback.format_exc())
                
                # Если нужно отправить уведомление пользователю
                if notify_user and len(args) > 1 and hasattr(args[1], "bot"):
                    context = args[1]
                    update = args[0]
                    if hasattr(update, "message") and update.message:
                        chat_id = update.message.chat_id
                        try:
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text="Произошла ошибка при обработке запроса. Пожалуйста, попробуйте позже."
                            )
                        except Exception:
                            logger.error("Не удалось отправить сообщение об ошибке пользователю")
                
                return return_value
                
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Формируем сообщение об ошибке
                error_message = f"Ошибка в {func.__name__}: {str(e)}"
                if log_msg:
                    error_message = f"{log_msg}: {error_message}"
                
                # Логируем ошибку
                logger.error(error_message)
                logger.error(traceback.format_exc())
                
                return return_value
        
        # Определяем, асинхронная функция или нет
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator
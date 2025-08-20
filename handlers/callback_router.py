# handlers/callback_router.py - Роутинг callback запросов

from telegram import Update
from telegram.ext import ContextTypes

# Константы для callback'ов
ADMIN_CALLBACKS = {
    "admin_tokens", "admin_users", "admin_back",
    "tokens_list", "tokens_clear", "tokens_analytics", "tokens_stats", 
    "users_add", "users_remove", "users_list", "users_toggle"
}

ADMIN_CALLBACK_PREFIXES = ("activate_", "deactivate_", "authorize_", "remove_", "confirm_remove_")

def is_admin_callback(callback_data: str) -> bool:
    """Проверяет, является ли callback админским."""
    return (
        callback_data in ADMIN_CALLBACKS or 
        callback_data.startswith(ADMIN_CALLBACK_PREFIXES)
    )

async def route_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Роутер для всех callback запросов."""
    query = update.callback_query
    
    if is_admin_callback(query.data):
        print(f"[DEBUG] Админский callback: {query.data}")  # Отладка
        from .admin_commands import handle_admin_callbacks
        await handle_admin_callbacks(update, context)
    else:
        print(f"[DEBUG] Обычный callback: {query.data}")  # Отладка
        # Импорт существующего обработчика из test_bot4.py
        import sys
        current_module = sys.modules['__main__']  # test_bot4.py
        if hasattr(current_module, 'handle_callback'):
            await current_module.handle_callback(update, context)
        else:
            await query.answer("❓ Неизвестная команда")
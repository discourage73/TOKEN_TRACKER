"""Роутер для callback запросов."""

from telegram import Update
from telegram.ext import ContextTypes

from bot_commands import handle_callback_router

async def route_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Основной роутер для всех callback запросов."""
    await handle_callback_router(update, context)
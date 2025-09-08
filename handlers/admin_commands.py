"""Обработчики админских команд."""

from telegram import Update
from telegram.ext import ContextTypes

from bot_commands import admin_command

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /admin - перенаправляет на admin_command из bot_commands."""
    await admin_command(update, context)
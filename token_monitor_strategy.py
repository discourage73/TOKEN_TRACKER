import logging
import time
from typing import Dict, Any, List, Optional, Set, Tuple
from enum import Enum
from dataclasses import dataclass

logger = logging.getLogger(__name__)

class TokenCategory(Enum):
    """Категории токенов по активности."""
    HOT = 0        # Токен проявляет высокую активность (рост >10% за последний час)
    ACTIVE = 1     # Токен проявляет умеренную активность (рост 5-10%)
    STABLE = 2     # Токен стабилен (изменения <5%)
    INACTIVE = 3   # Токен не проявляет активности долгое время

@dataclass
class TokenMonitorConfig:
    """Конфигурация мониторинга для токена."""
    check_interval: int      # Интервал проверки в секундах
    growth_threshold: float  # Порог для определения роста (в процентах)
    category: TokenCategory  # Категория токена

class TokenMonitorStrategy:
    """
    Стратегия мониторинга токенов с адаптивной частотой проверок.
    """
    
    def __init__(self):
        # Таблица стратегий для разных категорий токенов
        self.strategy_table: Dict[TokenCategory, TokenMonitorConfig] = {
            TokenCategory.HOT: TokenMonitorConfig(
                check_interval=30,        # каждые 30 секунд
                growth_threshold=5.0,     # 5% для отправки уведомления
                category=TokenCategory.HOT
            ),
            TokenCategory.ACTIVE: TokenMonitorConfig(
                check_interval=180,       # каждые 3 минуты
                growth_threshold=10.0,    # 10% для отправки уведомления
                category=TokenCategory.ACTIVE
            ),
            TokenCategory.STABLE: TokenMonitorConfig(
                check_interval=600,       # каждые 10 минут
                growth_threshold=15.0,    # 15% для отправки уведомления
                category=TokenCategory.STABLE
            ),
            TokenCategory.INACTIVE: TokenMonitorConfig(
                check_interval=1800,      # каждые 30 минут
                growth_threshold=20.0,    # 20% для отправки уведомления
                category=TokenCategory.INACTIVE
            )
        }
        
        # Хранение категорий токенов
        self.token_categories: Dict[str, TokenCategory] = {}
        
        # Хранение времени последней проверки
        self.last_check_time: Dict[str, float] = {}
        
        logger.info("Стратегия мониторинга токенов инициализирована")
    
    def get_token_category(self, query: str) -> TokenCategory:
        """
        Возвращает текущую категорию токена.
    
        Args:
            query: Запрос токена
        
        Returns:
            Категория токена или TokenCategory.STABLE по умолчанию
        """
        return self.token_categories.get(query, TokenCategory.STABLE)

    def categorize_token(self, token_data: Dict[str, Any]) -> TokenCategory:
        """
        Определяет категорию токена на основе его активности.
        
        Args:
            token_data: Данные о токене
            
        Returns:
            Категория токена
        """
        # Извлекаем данные о возрасте и росте
        added_time = token_data.get('added_time', 0)
        current_time = time.time()
        token_age_seconds = current_time - added_time
        
        # Получаем данные о росте
        token_info = token_data.get('token_info', {})
        initial_data = token_data.get('initial_data', {})
        
        current_mcap = token_info.get('raw_market_cap', 0)
        initial_mcap = initial_data.get('raw_market_cap', 0)
        
        # Вычисляем рост в процентах
        growth_percent = 0
        if initial_mcap and current_mcap and initial_mcap > 0:
            growth_percent = ((current_mcap / initial_mcap) - 1) * 100
        
        # Анализируем историю роста
        price_history = token_data.get('price_history', [])
        recent_growth = 0
        
        if len(price_history) >= 2:
            recent_prices = price_history[-2:]  # Последние две записи
            if recent_prices[0] > 0:
                recent_growth = ((recent_prices[1] / recent_prices[0]) - 1) * 100
        
        # Определяем категорию на основе возраста и роста
        if token_age_seconds < 3600:  # < 1 час
            return TokenCategory.HOT
        
        if token_age_seconds < 86400:  # < 24 часа
            if growth_percent > 100 or recent_growth > 10:
                return TokenCategory.HOT
            elif growth_percent > 50 or recent_growth > 5:
                return TokenCategory.ACTIVE
            else:
                return TokenCategory.STABLE
        
        # Для токенов старше 24 часов
        if growth_percent > 200 or recent_growth > 20:
            return TokenCategory.ACTIVE
        elif growth_percent > 50 or recent_growth > 5:
            return TokenCategory.STABLE
        else:
            return TokenCategory.INACTIVE
    
    def should_check_token(self, query: str, token_data: Dict[str, Any]) -> bool:
        """
        Определяет, нужно ли проверять токен в текущий момент.
        
        Args:
            query: Запрос токена
            token_data: Данные о токене
            
        Returns:
            True, если токен нужно проверить, иначе False
        """
        current_time = time.time()
        
        # Получаем время последней проверки
        last_check = self.last_check_time.get(query, 0)
        
        # Определяем категорию токена
        category = self.token_categories.get(query)
        
        # Если категория не определена, определяем её
        if category is None:
            category = self.categorize_token(token_data)
            self.token_categories[query] = category
        
        # Получаем конфигурацию для этой категории
        config = self.strategy_table[category]
        
        # Проверяем, прошло ли достаточно времени с последней проверки
        if current_time - last_check >= config.check_interval:
            # Обновляем время последней проверки
            self.last_check_time[query] = current_time
            return True
        
        return False
    
    def update_token_category(self, query: str, token_data: Dict[str, Any]) -> None:
        """
        Обновляет категорию токена на основе новых данных.
        
        Args:
            query: Запрос токена
            token_data: Данные о токене
        """
        new_category = self.categorize_token(token_data)
        old_category = self.token_categories.get(query)
        
        if old_category != new_category:
            self.token_categories[query] = new_category
            logger.info(f"Токен {query} переведен из категории {old_category} в {new_category}")
    
    def get_check_interval(self, query: str) -> int:
        """
        Возвращает интервал проверки для токена.
        
        Args:
            query: Запрос токена
            
        Returns:
            Интервал проверки в секундах
        """
        # Получаем категорию токена
        category = self.token_categories.get(query, TokenCategory.STABLE)
        
        # Возвращаем интервал из таблицы стратегий
        return self.strategy_table[category].check_interval
    
    def get_growth_threshold(self, query: str) -> float:
        """
        Возвращает порог роста для токена.
        
        Args:
            query: Запрос токена
            
        Returns:
            Порог роста в процентах
        """
        # Получаем категорию токена
        category = self.token_categories.get(query, TokenCategory.STABLE)
        
        # Возвращаем порог из таблицы стратегий
        return self.strategy_table[category].growth_threshold
    
    def should_notify_growth(self, query: str, current_growth: float) -> bool:
        """
        Определяет, нужно ли отправлять уведомление о росте токена.
        
        Args:
            query: Запрос токена
            current_growth: Текущий рост в процентах
            
        Returns:
            True, если нужно отправить уведомление, иначе False
        """
        # Получаем порог роста для токена
        threshold = self.get_growth_threshold(query)
        
        # Сравниваем с текущим ростом
        return current_growth >= threshold
    
    def get_all_tokens_by_category(self) -> Dict[TokenCategory, List[str]]:
        """
        Возвращает все токены, сгруппированные по категориям.
        
        Returns:
            Словарь категория -> список токенов
        """
        result = {category: [] for category in TokenCategory}
        
        for token, category in self.token_categories.items():
            result[category].append(token)
        
        return result
    
    def get_tokens_for_check(self, all_tokens: Dict[str, Dict[str, Any]]) -> List[str]:
        """
        Возвращает список токенов, которые нужно проверить сейчас.
        
        Args:
            all_tokens: Словарь со всеми токенами
            
        Returns:
            Список токенов для проверки
        """
        tokens_to_check = []
        current_time = time.time()
        
        for query, token_data in all_tokens.items():
            # Если токен скрыт, пропускаем его
            if token_data.get('hidden', False):
                continue
            
            # Если токен еще не отнесен к категории, делаем это
            if query not in self.token_categories:
                category = self.categorize_token(token_data)
                self.token_categories[query] = category
            
            # Получаем время последней проверки
            last_check = self.last_check_time.get(query, 0)
            
            # Получаем интервал проверки
            check_interval = self.get_check_interval(query)
            
            # Проверяем, нужно ли проверять токен сейчас
            if current_time - last_check >= check_interval:
                tokens_to_check.append(query)
        
        return tokens_to_check

# Создаем глобальный экземпляр стратегии
token_monitor_strategy = TokenMonitorStrategy()
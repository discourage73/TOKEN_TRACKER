"""Модификации token_service.py для батчинга"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any

from token_monitor_strategy import TokenCategory

logger = logging.getLogger(__name__)

class BatchTokenProcessor:
    """Обработчик токенов с батчингом"""
    
    def __init__(self, batch_size: int = 10, batch_timeout: float = 5.0):
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.pending_batches = []
        self.processing = False
    
    async def process_tokens_batch(self, token_queries: List[str]) -> Dict[str, Any]:
        """Обрабатывает список токенов в батчах"""
        if not token_queries:
            return {}
        
        results = {}
        
        # Разбиваем на батчи
        for i in range(0, len(token_queries), self.batch_size):
            batch = token_queries[i:i + self.batch_size]
            
            logger.info(f"Обработка батча {i//self.batch_size + 1}: {len(batch)} токенов")
            
            # Обрабатываем батч
            batch_results = await self._process_single_batch(batch)
            results.update(batch_results)
            
            # Пауза между батчами
            if i + self.batch_size < len(token_queries):
                await asyncio.sleep(0.5)
        
        return results
    
    async def _process_single_batch(self, batch: List[str]) -> Dict[str, Any]:
        """Обрабатывает один батч токенов"""
        tasks = []
        
        for query in batch:
            task = asyncio.create_task(self._process_single_token(query))
            tasks.append((query, task))
        
        # Ждем выполнения всех задач в батче с таймаутом
        results = {}
        
        try:
            await asyncio.wait_for(
                asyncio.gather(*[task for _, task in tasks], return_exceptions=True),
                timeout=self.batch_timeout
            )
        except asyncio.TimeoutError:
            logger.warning(f"Таймаут батча из {len(batch)} токенов")
        
        # Собираем результаты
        for query, task in tasks:
            try:
                if task.done():
                    result = task.result()
                    results[query] = result
                else:
                    task.cancel()
                    results[query] = None
            except Exception as e:
                logger.error(f"Ошибка обработки токена {query}: {e}")
                results[query] = None
        
        return results
    
    async def _process_single_token(self, query: str) -> Optional[Dict[str, Any]]:
        """Обрабатывает один токен"""
        try:
            # Здесь должна быть логика обработки токена
            # Например, запрос к API, обновление данных и т.д.
            
            # Имитация обработки
            await asyncio.sleep(0.1)
            
            return {
                'query': query,
                'processed': True,
                'timestamp': time.time()
            }
            
        except Exception as e:
            logger.error(f"Ошибка при обработке токена {query}: {e}")
            return None

class BatchMarketCapChecker:
    """Проверка маркет-капов с батчингом"""
    
    def __init__(self, batch_size: int = 15):
        self.batch_size = batch_size
        self.processor = BatchTokenProcessor(batch_size=batch_size)
    
    async def check_multiple_market_caps(self, token_addresses: List[str]) -> Dict[str, Any]:
        """Проверяет маркет-капы нескольких токенов"""
        logger.info(f"Запуск батчинговой проверки {len(token_addresses)} токенов")
        
        # Используем batch_market_cap для получения данных
        try:
            from batch_market_cap import batch_get_market_caps
            market_caps = await batch_get_market_caps(token_addresses)
            
            # Обрабатываем результаты
            results = {}
            for address, market_cap in market_caps.items():
                results[address] = {
                    'market_cap': market_cap,
                    'status': 'success' if market_cap is not None else 'failed',
                    'timestamp': time.time()
                }
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка батчинговой проверки маркет-капов: {e}")
            return {}
    
    async def update_tokens_with_batching(self, tokens_data: Dict[str, Any]) -> Dict[str, Any]:
        """Обновляет токены используя батчинг"""
        token_addresses = list(tokens_data.keys())
        
        if not token_addresses:
            return {}
        
        # Получаем маркет-капы батчами
        market_cap_results = await self.check_multiple_market_caps(token_addresses)
        
        # Обновляем данные токенов
        updated_tokens = {}
        
        for address, token_data in tokens_data.items():
            market_cap_data = market_cap_results.get(address, {})
            
            # Обновляем информацию о токене
            updated_token = token_data.copy()
            if market_cap_data.get('market_cap'):
                updated_token['current_market_cap'] = market_cap_data['market_cap']
                updated_token['last_update'] = market_cap_data['timestamp']
            
            updated_tokens[address] = updated_token
        
        logger.info(f"Обновлено {len(updated_tokens)} токенов через батчинг")
        return updated_tokens

# Глобальные экземпляры
batch_token_processor = BatchTokenProcessor(batch_size=10)
batch_market_cap_checker = BatchMarketCapChecker(batch_size=15)

async def process_tokens_with_batching(token_queries: List[str]) -> Dict[str, Any]:
    """Публичная функция для батчинговой обработки токенов"""
    return await batch_token_processor.process_tokens_batch(token_queries)

async def check_market_caps_batch(token_addresses: List[str]) -> Dict[str, Any]:
    """Публичная функция для батчинговой проверки маркет-капов"""
    return await batch_market_cap_checker.check_multiple_market_caps(token_addresses)
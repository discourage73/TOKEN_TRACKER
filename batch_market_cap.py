"""system батчинга запросов маркет-капа tokens"""

import asyncio
import aiohttp
import logging
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from collections import defaultdict

logger = logging.getLogger(__name__)

@dataclass
class TokenBatch:
    """Batch of tokens for market cap checking"""
    addresses: List[str]
    timestamp: float
    results: Dict[str, Any]

class MarketCapBatcher:
    """Batches market cap requests for efficiency"""
    
    def __init__(self, batch_size: int = 30, batch_timeout: float = 15.0, request_delay: float = 1.5):
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.request_delay = request_delay
        self.pending_requests = defaultdict(list)
        self.batch_results = {}
        self.session = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def get_market_caps(self, token_addresses: List[str]) -> Dict[str, Optional[float]]:
        """Get market caps for multiple tokens using batching"""
        if not token_addresses:
            return {}
        
        # Process in batches
        results = {}
        for i in range(0, len(token_addresses), self.batch_size):
            batch = token_addresses[i:i + self.batch_size]
            batch_results = await self._process_batch(batch)
            results.update(batch_results)
            
            # Delay between batches
            if i + self.batch_size < len(token_addresses):
                await asyncio.sleep(self.request_delay)
        
        return results
    
    async def _process_batch(self, token_addresses: List[str]) -> Dict[str, Optional[float]]:
        """Process a single batch of token addresses using DexScreener batch API"""
        results = {}
        
        # DexScreener поддерживает максимум 30 токенов за раз
        if len(token_addresses) > 30:
            logger.warning(f"⚠️ Batch size {len(token_addresses)} exceeds API limit of 30. Splitting...")
            
            # Разбиваем на части по 30 токенов
            for i in range(0, len(token_addresses), 30):
                sub_batch = token_addresses[i:i + 30]
                sub_results = await self._fetch_batch_tokens(sub_batch)
                results.update(sub_results)
                
                # Пауза между частями
                if i + 30 < len(token_addresses):
                    await asyncio.sleep(self.request_delay)
            
            return results
        else:
            # Если меньше 30 токенов, делаем один запрос
            return await self._fetch_batch_tokens(token_addresses)
    
    async def _fetch_batch_tokens(self, token_addresses: List[str]) -> Dict[str, Optional[float]]:
        """Fetch market caps for multiple tokens in one API request"""
        results = {}
        
        try:
            # Создаем URL с несколькими токенами через запятую
            tokens_param = ",".join(token_addresses)
            url = f"https://api.dexscreener.com/latest/dex/tokens/{tokens_param}"
            
            logger.debug(f"🔍 Batch API request for {len(token_addresses)} tokens")
            
            async with self.session.get(url, timeout=self.batch_timeout) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data is None or not isinstance(data, dict):
                        logger.warning(f"Invalid response format from batch API")
                        return {addr: None for addr in token_addresses}
                    
                    pairs = data.get('pairs', [])
                    
                    # Создаем структуру для хранения лучших пар по токенам
                    best_pairs = {}  # token_address -> {'mcap': value, 'quality': score}
                    
                    # Сначала найдем лучшую пару для каждого токена
                    for pair in pairs:
                        if not isinstance(pair, dict):
                            continue
                        
                        base_token = pair.get('baseToken', {})
                        if not isinstance(base_token, dict):
                            continue
                        
                        token_address = base_token.get('address', '')
                        if token_address not in token_addresses:
                            continue
                            
                        # Извлекаем market cap
                        fdv = pair.get('fdv')
                        market_cap_field = pair.get('marketCap')
                        
                        current_mcap = None
                        if fdv and fdv > 0:
                            current_mcap = float(fdv)
                        elif market_cap_field and market_cap_field > 0:
                            current_mcap = float(market_cap_field)
                        
                        if current_mcap:
                            # Рассчитываем качество пары
                            liquidity_usd = pair.get('liquidity', {}).get('usd', 0) or 0
                            volume_24h = pair.get('volume', {}).get('h24', 0) or 0
                            pair_quality = liquidity_usd + (volume_24h * 0.01)  # объем влияет меньше
                            
                            # Если это первая пара или лучше предыдущей
                            if (token_address not in best_pairs or 
                                pair_quality > best_pairs[token_address]['quality']):
                                
                                best_pairs[token_address] = {
                                    'mcap': current_mcap,
                                    'quality': pair_quality,
                                    'liquidity': liquidity_usd,
                                    'volume': volume_24h,
                                    'dex': pair.get('dexId', 'unknown')
                                }
                    
                    # Заполняем результаты на основе лучших пар
                    for addr in token_addresses:
                        if addr in best_pairs:
                            best = best_pairs[addr]
                            results[addr] = best['mcap']
                            logger.debug(f"Selected {best['dex']} pair for {addr[:8]}...: ${best['mcap']:,.0f}")
                        else:
                            results[addr] = None
                    
                    logger.debug(f"✅ Batch processed: {sum(1 for v in results.values() if v is not None)}/{len(token_addresses)} successful")
                    return results
                    
                else:
                    logger.warning(f"Batch API returned status {response.status}")
                    return {addr: None for addr in token_addresses}
                    
        except Exception as e:
            logger.error(f"Error in batch API request: {e}")
            return {addr: None for addr in token_addresses}
    
    async def _fetch_single_token(self, address: str) -> Optional[float]:
        """Fetch market cap for single token"""
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{address}"
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Проверяем что data не None и является словарем
                    if data is None or not isinstance(data, dict):
                        return None
                    
                    pairs = data.get('pairs', [])
                    
                    if pairs and len(pairs) > 0:
                        first_pair = pairs[0]
                        if not isinstance(first_pair, dict):
                            return None
                        
                        # Используем готовые значения из API
                        fdv = first_pair.get('fdv')
                        market_cap_field = first_pair.get('marketCap')
                        
                        # Приоритет: fdv > marketCap
                        if fdv and fdv > 0:
                            return float(fdv)
                        elif market_cap_field and market_cap_field > 0:
                            return float(market_cap_field)
                        else:
                            return None
                
                return None
                
        except Exception as e:
            logger.error(f"Error fetching token {address}: {e}")
            return None

# Global batcher instance with API-compliant settings (max 30 tokens per request)
market_cap_batcher = MarketCapBatcher(batch_size=30, batch_timeout=15.0, request_delay=1.5)

async def batch_get_market_caps(token_addresses: List[str]) -> Dict[str, Optional[float]]:
    """Public function to get market caps in batches"""
    async with MarketCapBatcher() as batcher:
        return await batcher.get_market_caps(token_addresses)

async def get_market_cap_batch(addresses: List[str]) -> Dict[str, Optional[float]]:
    """Alternative batch function"""
    return await batch_get_market_caps(addresses)
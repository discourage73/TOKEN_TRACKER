# Создайте новый file message_parsers.py
import re
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Pattern

logger = logging.getLogger(__name__)

class MessageParser(ABC):
    """Абстрактный класс для парсеров сообщений."""
    
    @abstractmethod
    def can_parse(self, text: str) -> bool:
        """
        Проверяет, может ли парсер обработать данный текст.
        
        Args:
            text: Текст messages
            
        Returns:
            True, если парсер может обработать текст, иначе False
        """
        pass
    
    @abstractmethod
    def parse(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Парсит текст messages.
        
        Args:
            text: Текст messages
            
        Returns:
            Словарь с извлеченными данными или None
        """
        pass
    
    @abstractmethod
    def format(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Форматирует извлеченные data в нужный формат.
        
        Args:
            data: Извлеченные data
            
        Returns:
            Отформатированный текст или None
        """
        pass

class RayCyanBotParser(MessageParser):
    """Парсер для сообщений ray_cyan_bot."""
    
    def __init__(self):
        # Шаблоны для извлечения данных
        self.buy_pattern = re.compile(r'BUY ([^\s\(\)]+)')
        self.solscan_pattern = re.compile(r'https://solscan.io/account/([a-zA-Z0-9]{32,})')
        self.swap_pattern = re.compile(r'^(\s*[a-zA-Z0-9]+\S+)\s+swapped')
        self.address_pattern = re.compile(r'([a-zA-Z0-9]{40,})')
    
    def can_parse(self, text: str) -> bool:
        """
        Проверяет, может ли парсер обработать message ray_cyan_bot.
        
        Args:
            text: Текст messages
            
        Returns:
            True, если это message от ray_cyan_bot, иначе False
        """
        return "BUY" in text
    
    def parse(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Парсит message ray_cyan_bot.
        
        Args:
            text: Текст messages
            
        Returns:
            Словарь с извлеченными данными или None
        """
        try:
            # Checking, что это message о покупке token
            if "BUY" not in text:
                return None
            
            # Извлекаем название token
            buy_match = self.buy_pattern.search(text)
            token_name = buy_match.group(1) if buy_match else "UNKNOWN"
            
            # Убедимся, что в токене нет закрывающей скобки
            if token_name.endswith("]"):
                token_name = token_name[:-1]
            
            # Извлекаем полный адрес кошелька
            full_wallet = None
            
            # Ищем в ссылках solscan
            solscan_match = self.solscan_pattern.search(text)
            
            if solscan_match:
                full_wallet = solscan_match.group(1)
            
            # Если не нашли в URL, ищем строки, которые могут быть кошельками
            if not full_wallet:
                # Ищем строки с "swapped"
                swap_lines = [line for line in text.split('\n') if "swapped" in line]
                
                if swap_lines:
                    for line in swap_lines:
                        # Ищем кошелек в начале строки с "swapped"
                        swap_match = self.swap_pattern.search(line)
                        
                        if swap_match:
                            # Извлекаем полное имя перед "swapped"
                            wallet_prefix = swap_match.group(1).strip()
                            
                            # Ищем полный адрес, соответствующий этому префиксу
                            for potential_addr in self.address_pattern.findall(text):
                                if potential_addr.startswith(wallet_prefix[:5]):
                                    full_wallet = potential_addr
                                    break
            
            # Если все еще не нашли, ищем любую достаточно длинную строку
            if not full_wallet:
                # Ищем в строках текста
                for line in text.split('\n'):
                    wallet_matches = self.address_pattern.findall(line)
                    
                    for wallet in wallet_matches:
                        # Checking, что это не похоже на Contract (не последняя строка)
                        if wallet and line != text.split('\n')[-1]:
                            full_wallet = wallet
                            break
            
            # Извлекаем адрес контракта
            lines = text.split('\n')
            contract_address = ""
            
            # Checking последнюю строку на наличие контракта
            if lines and lines[-1].strip():
                last_line = lines[-1].strip()
                
                if re.match(r'^[a-zA-Z0-9]{30,}$', last_line) and (not full_wallet or full_wallet != last_line):
                    contract_address = last_line
            
            # Если не нашли в последней строке, ищем в любом месте текста
            if not contract_address:
                for line in text.split('\n'):
                    contract_matches = re.findall(r'([a-zA-Z0-9]{30,})', line)
                    
                    for contract in contract_matches:
                        if contract and (not full_wallet or full_wallet != contract):
                            contract_address = contract
                            break
            
            # Возвращаем извлеченные data
            return {
                'token_name': token_name,
                'wallet': full_wallet,
                'contract_address': contract_address
            }
            
        except Exception as e:
            logger.error(f"Error при парсинге messages ray_cyan_bot: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def format(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Форматирует извлеченные data в нужный формат.
        
        Args:
            data: Извлеченные data
            
        Returns:
            Отформатированный текст или None
        """
        try:
            if not data:
                return None
                
            token_name = data.get('token_name', 'UNKNOWN')
            wallet = data.get('wallet')
            contract_address = data.get('contract_address', '')
            
            # Форматируем message по требуемому шаблону
            formatted_text = f"""🟢 BUY {token_name}"""
            
            # Adding строку с кошельком, если нашли
            if wallet:
                formatted_text += f"\nSmart money : {wallet}"
            
            # Adding адрес контракта, если нашли
            if contract_address:
                formatted_text += f"\n{contract_address}"
            
            return formatted_text
            
        except Exception as e:
            logger.error(f"Error при форматировании данных ray_cyan_bot: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

class WhaleAlertParser(MessageParser):
    """Парсер для сообщений о китах (Whale Alerts)."""
    
    def __init__(self):
        # Шаблоны для извлечения данных
        self.whale_info_pattern = re.compile(r'(A .+? Whale just bought \$[\d.]+[KMB]? of .+?)(?=\(|\n|$)')
        self.mc_pattern = re.compile(r'\(MC:?\s*\$([\d.]+[KMB]?)\)')
        self.contract_pattern = re.compile(r'([A-Za-z0-9]{30,})')
    
    def can_parse(self, text: str) -> bool:
        """
        Проверяет, может ли парсер обработать message о ките.
        
        Args:
            text: Текст messages
            
        Returns:
            True, если это message о ките, иначе False
        """
        return "New Token Whale Alert" in text and "just bought" in text and "just sold" not in text
    
    def parse(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Парсит message о ките.
        
        Args:
            text: Текст messages
            
        Returns:
            Словарь с извлеченными данными или None
        """
        try:
            # Checking, что это message о покупке кита
            if not ("New Token Whale Alert" in text and "just bought" in text and "just sold" not in text):
                return None
            
            # Извлекаем основную информацию о покупке кита
            whale_info_match = self.whale_info_pattern.search(text)
            whale_info = whale_info_match.group(1).strip() if whale_info_match else ""
            
            # Если не нашли информацию о ките, это не интересующее нас message
            if not whale_info:
                return None
            
            # Checking, есть ли "just sold" в тексте messages (дополнительная Check)
            if "just sold" in whale_info:
                return None
                
            # Извлекаем информацию о маркет капе
            mc_match = self.mc_pattern.search(text)
            mc_info = mc_match.group(1) if mc_match else ""
            
            # Извлекаем адрес контракта
            contract_match = self.contract_pattern.search(text)
            contract_address = contract_match.group(1) if contract_match else ""
            
            # Возвращаем извлеченные data
            return {
                'whale_info': whale_info,
                'market_cap': mc_info,
                'contract_address': contract_address
            }
            
        except Exception as e:
            logger.error(f"Error при парсинге messages о ките: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def format(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Форматирует извлеченные data в нужный формат.
        
        Args:
            data: Извлеченные data
            
        Returns:
            Отформатированный текст или None
        """
        try:
            if not data:
                return None
                
            whale_info = data.get('whale_info', '')
            market_cap = data.get('market_cap', '')
            contract_address = data.get('contract_address', '')
            
            # Формируем информацию о маркет капе
            mc_info = f"(MC: ${market_cap})" if market_cap else ""
            
            # Форматируем message по требуемому шаблону
            formatted_text = f"""New Token Whale Alert
🟢 {whale_info} {mc_info}

{contract_address}"""
            
            return formatted_text
            
        except Exception as e:
            logger.error(f"Error при форматировании данных о ките: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

# Фабрика парсеров
class MessageParserFactory:
    """Фабрика для создания подходящего парсера сообщений."""
    
    def __init__(self):
        self.parsers: List[MessageParser] = [
            RayCyanBotParser(),
            WhaleAlertParser()
        ]
    
    def get_parser(self, text: str) -> Optional[MessageParser]:
        """
        Возвращает подходящий парсер для текста messages.
        
        Args:
            text: Текст messages
            
        Returns:
            Парсер сообщений или None, если подходящий парсер не найден
        """
        for parser in self.parsers:
            if parser.can_parse(text):
                return parser
        return None

# Creating глобальный экземпляр фабрики
parser_factory = MessageParserFactory()
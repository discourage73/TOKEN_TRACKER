import sqlite3
import pandas as pd
from datetime import datetime
import json
import logging
import os

logger = logging.getLogger(__name__)

def export_tokens_analytics() -> str:
    """
    Экспортирует данные токенов в Excel файл.
    
    Объединяет данные из mcap_monitoring и tokens таблиц:
    - mcap_monitoring: market cap данные, множители, статус активности
    - tokens: сигналы каналов, времена, статус отправки
    
    Returns:
        str: Путь к созданному Excel файлу
    """
    try:
        logger.info("🔄 Начинаем экспорт аналитики токенов")
        
        # Подключаемся к базе данных
        conn = sqlite3.connect("tokens_tracker_database.db")
        
        # SQL запрос для объединения данных из обеих таблиц
        query = """
        SELECT 
            -- Данные из mcap_monitoring
            m.contract,
            m.initial_mcap,
            m.curr_mcap,
            m.ath_mcap,
            m.created_time as monitoring_start,
            m.updated_time as last_updated,
            m.ath_time,
            m.last_alert_multiplier,
            m.is_active,
            
            -- Данные из tokens (сигналы и каналы)
            t.channels,
            t.channel_times,
            t.channel_count,
            t.first_seen,
            t.signal_reached_time,
            t.time_to_threshold,
            t.message_sent,
            t.message_id,
            t.token_info,
            t.raw_api_data
            
        FROM mcap_monitoring m
        LEFT JOIN tokens t ON m.contract = t.contract
        ORDER BY m.created_time DESC
        """
        
        # Выполняем запрос
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        logger.info(f"📊 Получено {len(df)} записей для экспорта")
        
        # Обрабатываем данные для лучшей читаемости
        df = process_export_data(df)
        
        # Сортируем по real_multiplier от большего к меньшему
        df = df.sort_values('real_multiplier', ascending=False, na_position='last')
        
        # Создаем имя файла с текущей датой
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"tokens_analytics_{timestamp}.xlsx"
        filepath = os.path.join("exports", filename)
        
        # Создаем папку exports если её нет
        os.makedirs("exports", exist_ok=True)
        
        # Экспортируем в Excel с четырьмя листами
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            # Основной лист с данными
            df.to_excel(writer, sheet_name='Tokens_Analytics', index=False)
            
            # Общая статистика
            main_stats_df, daily_stats_df = create_stats_summary_separate(df)
            main_stats_df.to_excel(writer, sheet_name='Statistics', index=False)
            
            # Дневная статистика в отдельном листе с отдельными столбцами
            if not daily_stats_df.empty:
                daily_stats_df.to_excel(writer, sheet_name='Daily_Stats', index=False)
            
            # Аналитика каналов
            channels_df = create_channels_analytics(df)
            if not channels_df.empty:
                channels_df.to_excel(writer, sheet_name='Channels', index=False)
            
            # Theory анализ
            theory_df = create_theory_analysis(df)
            if not theory_df.empty:
                theory_df.to_excel(writer, sheet_name='Theory', index=False)
            
            # Настраиваем автоширину столбцов для всех листов
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    
                    adjusted_width = min(max_length + 2, 50)  # Максимум 50 символов
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        logger.info(f"✅ Экспорт завершен: {filepath}")
        return filepath
        
    except Exception as e:
        logger.error(f"❌ Ошибка при экспорте аналитики: {e}")
        raise

def process_export_data(df: pd.DataFrame) -> pd.DataFrame:
    """Обрабатывает данные для улучшения читаемости в Excel."""
    try:
        # Вычисляем реальный множитель
        df['real_multiplier'] = (df['ath_mcap'] / df['initial_mcap']).round(2)
        
        # Обрабатываем JSON поля для читаемости
        df['token_name'] = df['token_info'].apply(extract_token_name)
        df['token_symbol'] = df['token_info'].apply(extract_token_symbol)
        
        # Обрабатываем каналы
        df['channels_list'] = df['channels'].apply(parse_channels_json)
        df['signals_count'] = df['channel_count'].fillna(0)
        
        # Форматируем даты
        date_columns = ['monitoring_start', 'last_updated', 'ath_time', 'first_seen', 'signal_reached_time']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Добавляем расчетные поля
        df['days_in_monitoring'] = (datetime.now() - df['monitoring_start']).dt.days
        df['mcap_change_percent'] = ((df['curr_mcap'] - df['initial_mcap']) / df['initial_mcap'] * 100).round(2)
        
        # Переупорядочиваем колонки для удобства (убираем ненужные)
        column_order = [
            'contract', 'token_name', 'is_active',
            'signals_count', 'initial_mcap', 'curr_mcap', 'ath_mcap',
            'real_multiplier', 'monitoring_start', 'first_seen', 
            'ath_time', 'time_to_threshold'
        ]
        
        # Столбцы для исключения
        columns_to_exclude = [
            'message_sent', 'token_symbol', 'last_alert_multiplier', 
            'mcap_change_percent', 'raw_api_data', 'token_info', 'message_id',
            'channels_list', 'signal_reached_time', 'days_in_monitoring', 'channel_count'
        ]
        
        # Оставляем только нужные колонки
        existing_columns = [col for col in column_order if col in df.columns]
        remaining_columns = [col for col in df.columns if col not in existing_columns and col not in columns_to_exclude]
        df = df[existing_columns + remaining_columns]
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {e}")
        return df

def extract_token_name(token_info_json):
    """Извлекает имя токена из JSON."""
    try:
        if pd.isna(token_info_json) or not token_info_json:
            return "Unknown"
        data = json.loads(token_info_json)
        return data.get('name', data.get('ticker', 'Unknown'))
    except:
        return "Unknown"

def extract_token_symbol(token_info_json):
    """Извлекает символ токена из JSON."""
    try:
        if pd.isna(token_info_json) or not token_info_json:
            return "UNK"
        data = json.loads(token_info_json)
        return data.get('ticker', data.get('symbol', 'UNK'))
    except:
        return "UNK"

def parse_channels_json(channels_json):
    """Парсит JSON каналов в читаемую строку."""
    try:
        if pd.isna(channels_json) or not channels_json:
            return ""
        channels = json.loads(channels_json)
        if isinstance(channels, list):
            return ", ".join(channels[:5])  # Показываем первые 5 каналов
        return str(channels)
    except:
        return "Error parsing channels"

def create_stats_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Создает сводную статистику."""
    # Основная статистика
    total_tokens = len(df)
    active_tokens = len(df[df['is_active'] == 1])
    rug_ratio = int(((total_tokens - active_tokens) / total_tokens * 100)) if total_tokens > 0 else 0
    
    stats = {
        'Metric': [
            'Total Tokens',
            'Active Tokens',
            'Tokens with Growth ≥2x',
            'Tokens with Growth ≥5x',
            'Tokens with Growth ≥10x',
            'Average Signals per Token',
            'High Growth Rate (≥2x/Total)',
            'RUG Ratio'
        ],
        'Value': [
            total_tokens,
            active_tokens,
            len(df[df['real_multiplier'] >= 2]),
            len(df[df['real_multiplier'] >= 5]),
            len(df[df['real_multiplier'] >= 10]),
            df['signals_count'].mean().round(1),
            f"{(len(df[df['real_multiplier'] >= 2]) / total_tokens * 100):.1f}%" if total_tokens > 0 else "0%",
            f"{rug_ratio}%"
        ]
    }
    
    main_stats_df = pd.DataFrame(stats)
    
    # Добавляем пустые строки для разделения
    separator_df = pd.DataFrame({
        'Metric': ['', ''],
        'Value': ['', '']
    })
    
    # Создаем статистику по дням
    daily_stats_df = create_daily_stats(df)
    
    # Объединяем все в один DataFrame
    result_df = pd.concat([main_stats_df, separator_df, daily_stats_df], ignore_index=True)
    
    return result_df

def create_stats_summary_separate(df: pd.DataFrame):
    """Создает отдельно основную статистику и дневную статистику."""
    # Основная статистика
    total_tokens = len(df)
    active_tokens = len(df[df['is_active'] == 1])
    rug_ratio = int(((total_tokens - active_tokens) / total_tokens * 100)) if total_tokens > 0 else 0
    
    main_stats = {
        'Metric': [
            'Total Tokens',
            'Active Tokens',
            'Tokens with Growth ≥2x',
            'Tokens with Growth ≥5x',
            'Tokens with Growth ≥10x',
            'Average Signals per Token',
            'High Growth Rate (≥2x/Total)',
            'RUG Ratio'
        ],
        'Value': [
            total_tokens,
            active_tokens,
            len(df[df['real_multiplier'] >= 2]),
            len(df[df['real_multiplier'] >= 5]),
            len(df[df['real_multiplier'] >= 10]),
            df['signals_count'].mean().round(1),
            f"{(len(df[df['real_multiplier'] >= 2]) / total_tokens * 100):.1f}%" if total_tokens > 0 else "0%",
            f"{rug_ratio}%"
        ]
    }
    
    main_stats_df = pd.DataFrame(main_stats)
    daily_stats_df = create_daily_stats_separate(df)
    
    return main_stats_df, daily_stats_df

def create_daily_stats_separate(df: pd.DataFrame) -> pd.DataFrame:
    """Создает дневную статистику с отдельными столбцами."""
    try:
        from datetime import datetime, timedelta
        import pytz
        
        # Московский часовой пояс
        msk_tz = pytz.timezone('Europe/Moscow')
        
        # Получаем все уникальные даты из данных
        df_copy = df.copy()
        df_copy['monitoring_start'] = pd.to_datetime(df_copy['monitoring_start'])
        
        if df_copy['monitoring_start'].empty:
            return pd.DataFrame()
        
        # Определяем диапазон дат
        min_date = df_copy['monitoring_start'].min().date()
        max_date = df_copy['monitoring_start'].max().date()
        
        daily_data = []
        current_date = min_date
        
        while current_date <= max_date:
            # Период с 14:00 текущего дня до 13:59 следующего дня (МСК)
            period_start = datetime.combine(current_date, datetime.min.time().replace(hour=14, minute=0))
            period_end = period_start + timedelta(days=1) - timedelta(minutes=1)  # 13:59 следующего дня
            
            # Конвертируем в UTC для сравнения
            period_start_utc = msk_tz.localize(period_start).astimezone(pytz.UTC).replace(tzinfo=None)
            period_end_utc = msk_tz.localize(period_end).astimezone(pytz.UTC).replace(tzinfo=None)
            
            # Фильтруем токены за этот период
            day_tokens = df_copy[
                (df_copy['monitoring_start'] >= period_start_utc) & 
                (df_copy['monitoring_start'] <= period_end_utc)
            ]
            
            if len(day_tokens) > 0:
                total_day_tokens = len(day_tokens)
                day_growth_2x = len(day_tokens[day_tokens['real_multiplier'] >= 2])
                day_active = len(day_tokens[day_tokens['is_active'] == 1])
                day_rug_ratio = int(((total_day_tokens - day_active) / total_day_tokens * 100)) if total_day_tokens > 0 else 0
                day_growth_rate = round((day_growth_2x / total_day_tokens * 100), 1) if total_day_tokens > 0 else 0
                
                daily_data.append({
                    'Day': current_date.strftime('%Y-%m-%d'),
                    'Time_Period': '14:00-13:59 MSK',
                    'Total_Tokens': total_day_tokens,
                    'Tokens_Growth_2x': day_growth_2x,
                    'RUG_Ratio_Percent': day_rug_ratio,
                    'High_Growth_Rate_Percent': day_growth_rate
                })
            
            current_date += timedelta(days=1)
        
        if not daily_data:
            return pd.DataFrame()
        
        # Создаем DataFrame с дневной статистикой
        daily_df = pd.DataFrame(daily_data)
        
        return daily_df
        
    except Exception as e:
        logger.error(f"Ошибка при создании дневной статистики: {e}")
        return pd.DataFrame()

def create_channels_analytics(df: pd.DataFrame) -> pd.DataFrame:
    """Создает аналитику каналов."""
    try:
        import json
        from collections import defaultdict
        
        # Собираем статистику по каналам
        channel_stats = defaultdict(lambda: {
            'total_signals': 0,
            'successful_signals': 0, 
            'tokens': [],
            'entry_positions': []  # для расчета среднего места входа
        })
        
        logger.info("🔄 Анализируем каналы...")
        
        for _, token in df.iterrows():
            try:
                # Получаем каналы токена
                channels_data = token.get('channels')
                if pd.isna(channels_data) or not channels_data or channels_data.strip() == '':
                    continue
                
                # Парсим каналы - сначала пробуем как JSON, потом как строку с запятыми
                channels = []
                try:
                    # Пытаемся парсить как JSON массив
                    channels = json.loads(channels_data)
                    if not isinstance(channels, list):
                        channels = []
                except (json.JSONDecodeError, TypeError):
                    # Если не JSON, парсим как строку с разделителями
                    if isinstance(channels_data, str):
                        channels = [ch.strip() for ch in channels_data.split(',') if ch.strip()]
                
                if not channels:
                    continue
                
                # Определяем успешность токена
                real_multiplier = token.get('real_multiplier', 0)
                is_successful = real_multiplier >= 2
                
                # Получаем времена вхождения каналов
                channel_times_json = token.get('channel_times', '{}')
                try:
                    if pd.isna(channel_times_json) or not channel_times_json or channel_times_json.strip() == '':
                        channel_times = {}
                    else:
                        channel_times = json.loads(channel_times_json)
                        if not isinstance(channel_times, dict):
                            channel_times = {}
                except (json.JSONDecodeError, TypeError):
                    channel_times = {}
                
                # Сортируем каналы по времени вхождения (кто первый вошел)
                channels_with_times = []
                for channel in channels:
                    entry_time = channel_times.get(channel, '')
                    channels_with_times.append((channel, entry_time))
                
                # Сортируем по времени (ранние сигналы первые)
                channels_with_times.sort(key=lambda x: x[1] if x[1] else '9999-12-31')
                
                # Обновляем статистику для каждого канала
                for position, (channel, entry_time) in enumerate(channels_with_times, 1):
                    channel_stats[channel]['total_signals'] += 1
                    channel_stats[channel]['entry_positions'].append(position)
                    
                    if is_successful:
                        channel_stats[channel]['successful_signals'] += 1
                    
                    # Добавляем токен в список
                    token_info = {
                        'contract': token.get('contract', 'Unknown')[:12] + '...',
                        'token_name': token.get('token_name', 'Unknown'),
                        'multiplier': real_multiplier,
                        'successful': is_successful,
                        'entry_position': position,
                        'entry_time': entry_time
                    }
                    channel_stats[channel]['tokens'].append(token_info)
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке токена {token.get('contract', 'Unknown')}: {e}")
                continue
        
        if not channel_stats:
            return pd.DataFrame()
        
        # Формируем итоговые данные
        channels_data = []
        
        for channel_name, stats in channel_stats.items():
            total_signals = stats['total_signals']
            successful_signals = stats['successful_signals']
            success_rate = (successful_signals / total_signals * 100) if total_signals > 0 else 0
            
            # Средняя позиция входа
            avg_entry_position = sum(stats['entry_positions']) / len(stats['entry_positions']) if stats['entry_positions'] else 0
            
            # Сортируем токены по успешности (успешные первые, потом по множителю)
            tokens_sorted = sorted(stats['tokens'], key=lambda x: (-x['successful'], -x['multiplier']))
            
            # Создаем строку со списком токенов
            tokens_list = []
            for token in tokens_sorted[:10]:  # Показываем первые 10 токенов
                status = "✅" if token['successful'] else "❌"
                tokens_list.append(f"{status} {token['token_name']} ({token['multiplier']:.1f}x, pos.{token['entry_position']})")
            
            tokens_string = " | ".join(tokens_list)
            if len(tokens_sorted) > 10:
                tokens_string += f" | ...and {len(tokens_sorted) - 10} more"
            
            channels_data.append({
                'Channel_Name': channel_name,
                'Total_Signals': total_signals,
                'Successful_Signals': successful_signals,
                'Success_Rate_Percent': round(success_rate, 1),
                'Average_Entry_Position': round(avg_entry_position, 1),
                'Top_Tokens': tokens_string
            })
        
        # Сортируем каналы по проценту успешности, потом по количеству сигналов
        channels_data.sort(key=lambda x: (-x['Success_Rate_Percent'], -x['Total_Signals']))
        
        channels_df = pd.DataFrame(channels_data)
        logger.info(f"📊 Проанализировано {len(channels_df)} каналов")
        
        return channels_df
        
    except Exception as e:
        logger.error(f"❌ Ошибка при создании аналитики каналов: {e}")
        return pd.DataFrame()

def create_daily_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Создает статистику по дням (с 14:00 до 13:59 следующего дня по МСК)."""
    try:
        from datetime import datetime, timedelta
        import pytz
        
        # Московский часовой пояс
        msk_tz = pytz.timezone('Europe/Moscow')
        
        # Получаем все уникальные даты из данных
        df_copy = df.copy()
        df_copy['monitoring_start'] = pd.to_datetime(df_copy['monitoring_start'])
        
        if df_copy['monitoring_start'].empty:
            return pd.DataFrame({
                'Metric': ['Daily Statistics (14:00-13:59 MSK)'],
                'Value': ['No data available']
            })
        
        # Определяем диапазон дат
        min_date = df_copy['monitoring_start'].min().date()
        max_date = df_copy['monitoring_start'].max().date()
        
        daily_data = []
        current_date = min_date
        
        while current_date <= max_date:
            # Период с 14:00 текущего дня до 13:59 следующего дня (МСК)
            period_start = datetime.combine(current_date, datetime.min.time().replace(hour=14, minute=0))
            period_end = period_start + timedelta(days=1) - timedelta(minutes=1)  # 13:59 следующего дня
            
            # Конвертируем в UTC для сравнения (предполагаем что данные в UTC)
            period_start_utc = msk_tz.localize(period_start).astimezone(pytz.UTC).replace(tzinfo=None)
            period_end_utc = msk_tz.localize(period_end).astimezone(pytz.UTC).replace(tzinfo=None)
            
            # Фильтруем токены за этот период
            day_tokens = df_copy[
                (df_copy['monitoring_start'] >= period_start_utc) & 
                (df_copy['monitoring_start'] <= period_end_utc)
            ]
            
            if len(day_tokens) > 0:
                total_day_tokens = len(day_tokens)
                day_growth_2x = len(day_tokens[day_tokens['real_multiplier'] >= 2])
                day_active = len(day_tokens[day_tokens['is_active'] == 1])
                day_rug_ratio = int(((total_day_tokens - day_active) / total_day_tokens * 100)) if total_day_tokens > 0 else 0
                day_growth_rate = f"{(day_growth_2x / total_day_tokens * 100):.1f}%" if total_day_tokens > 0 else "0%"
                
                daily_data.append({
                    'Day': current_date.strftime('%Y-%m-%d'),
                    'Time Period': f"14:00-13:59 MSK",
                    'Total Tokens': total_day_tokens,
                    'Tokens ≥2x': day_growth_2x,
                    'RUG Ratio': f"{day_rug_ratio}%",
                    'Growth Rate': day_growth_rate
                })
            
            current_date += timedelta(days=1)
        
        if not daily_data:
            return pd.DataFrame({
                'Metric': ['Daily Statistics'],
                'Value': ['No daily data available']
            })
        
        # Создаем DataFrame со статистикой по дням
        daily_df = pd.DataFrame(daily_data)
        
        # Преобразуем в формат Metric/Value для единообразия
        daily_stats_formatted = []
        daily_stats_formatted.append({'Metric': 'Daily Statistics (14:00-13:59 MSK)', 'Value': ''})
        daily_stats_formatted.append({'Metric': 'Day | Time | Total | ≥2x | RUG% | Growth%', 'Value': ''})
        
        for _, row in daily_df.iterrows():
            daily_stats_formatted.append({
                'Metric': f"{row['Day']} | {row['Time Period']} | {row['Total Tokens']} | {row['Tokens ≥2x']} | {row['RUG Ratio']} | {row['Growth Rate']}",
                'Value': ''
            })
        
        return pd.DataFrame(daily_stats_formatted)
        
    except Exception as e:
        logger.error(f"Ошибка при создании дневной статистики: {e}")
        return pd.DataFrame({
            'Metric': ['Daily Statistics Error'],
            'Value': [f'Error: {str(e)}']
        })

def create_theory_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Создает анализ теории токенов по стартовой капитализации."""
    try:
        logger.info("🔄 Анализируем теорию токенов по маркет кап...")
        
        # Конвертируем market cap в числовые значения
        df_copy = df.copy()
        df_copy['initial_mcap'] = pd.to_numeric(df_copy['initial_mcap'], errors='coerce')
        df_copy['ath_mcap'] = pd.to_numeric(df_copy['ath_mcap'], errors='coerce')
        df_copy['curr_mcap'] = pd.to_numeric(df_copy['curr_mcap'], errors='coerce')
        
        # Убираем токены с некорректными данными
        df_clean = df_copy.dropna(subset=['initial_mcap', 'ath_mcap'])
        df_clean = df_clean[df_clean['initial_mcap'] > 0]
        
        theory_data = []
        
        # ТЕОРИЯ 1: Токены, стартовавшие с маркет кап < 1M
        theory1_tokens = df_clean[df_clean['initial_mcap'] < 1000000]
        theory1_total = len(theory1_tokens)
        
        if theory1_total > 0:
            # Сколько достигли 1M
            theory1_reached_1m = len(theory1_tokens[theory1_tokens['ath_mcap'] >= 1000000])
            theory1_1m_percent = round((theory1_reached_1m / theory1_total) * 100, 1)
            
            # Из тех, что достигли 1M, сколько достигли 2M
            theory1_1m_tokens = theory1_tokens[theory1_tokens['ath_mcap'] >= 1000000]
            theory1_reached_2m = len(theory1_1m_tokens[theory1_1m_tokens['ath_mcap'] >= 2000000])
            theory1_2m_percent = round((theory1_reached_2m / theory1_reached_1m) * 100, 1) if theory1_reached_1m > 0 else 0
        else:
            theory1_reached_1m = 0
            theory1_1m_percent = 0
            theory1_2m_percent = 0
        
        # ТЕОРИЯ 2: Токены, стартовавшие с маркет кап > 1M
        theory2_tokens = df_clean[df_clean['initial_mcap'] > 1000000]
        theory2_total = len(theory2_tokens)
        
        if theory2_total > 0:
            # Сколько достигли x2 и более от начальной капы
            theory2_reached_2x = len(theory2_tokens[theory2_tokens['ath_mcap'] >= (theory2_tokens['initial_mcap'] * 2)])
            theory2_2x_percent = round((theory2_reached_2x / theory2_total) * 100, 1)
        else:
            theory2_reached_2x = 0
            theory2_2x_percent = 0
        
        # Формируем результаты
        theory_data = [
            {
                'Category': 'THEORY 1: Tokens Starting < 1M',
                'Metric': 'Total Tokens',
                'Count': theory1_total,
                'Percentage': '100%'
            },
            {
                'Category': 'THEORY 1: Tokens Starting < 1M',
                'Metric': 'Reached 1M Market Cap',
                'Count': theory1_reached_1m,
                'Percentage': f'{theory1_1m_percent}%'
            },
            {
                'Category': 'THEORY 1: Tokens Starting < 1M',
                'Metric': 'After reaching 1M, reached 2M',
                'Count': theory1_reached_2m,
                'Percentage': f'{theory1_2m_percent}%'
            },
            {
                'Category': '',
                'Metric': '',
                'Count': '',
                'Percentage': ''
            },
            {
                'Category': 'THEORY 2: Tokens Starting > 1M',
                'Metric': 'Total Tokens',
                'Count': theory2_total,
                'Percentage': '100%'
            },
            {
                'Category': 'THEORY 2: Tokens Starting > 1M',
                'Metric': 'Reached x2+ Multiplier',
                'Count': theory2_reached_2x,
                'Percentage': f'{theory2_2x_percent}%'
            },
            {
                'Category': '',
                'Metric': '',
                'Count': '',
                'Percentage': ''
            },
            {
                'Category': 'SUMMARY',
                'Metric': 'Theory 1 Success Rate (< 1M → 1M)',
                'Count': f'{theory1_reached_1m}/{theory1_total}',
                'Percentage': f'{theory1_1m_percent}%'
            },
            {
                'Category': 'SUMMARY',
                'Metric': 'Theory 1 Advanced Rate (1M → 2M)',
                'Count': f'{theory1_reached_2m}/{theory1_reached_1m}' if theory1_reached_1m > 0 else '0/0',
                'Percentage': f'{theory1_2m_percent}%'
            },
            {
                'Category': 'SUMMARY',
                'Metric': 'Theory 2 Success Rate (> 1M → x2)',
                'Count': f'{theory2_reached_2x}/{theory2_total}',
                'Percentage': f'{theory2_2x_percent}%'
            }
        ]
        
        theory_df = pd.DataFrame(theory_data)
        logger.info(f"📊 Theory анализ завершен: Theory1 ({theory1_total} tokens), Theory2 ({theory2_total} tokens)")
        
        return theory_df
        
    except Exception as e:
        logger.error(f"❌ Ошибка при создании theory анализа: {e}")
        return pd.DataFrame({
            'Category': ['Error'],
            'Metric': ['Theory Analysis Failed'],
            'Count': [str(e)],
            'Percentage': ['N/A']
        })

# Функция для использования в bot_commands.py
def handle_analytics_export():
    """Создает Excel файл аналитики и возвращает путь к нему."""
    try:
        filepath = export_tokens_analytics()
        return filepath
    except Exception as e:
        logger.error(f"Ошибка при создании аналитики: {e}")
        raise

if __name__ == "__main__":
    # Тестирование
    logging.basicConfig(level=logging.INFO)
    try:
        filepath = export_tokens_analytics()
        print(f"Файл создан: {filepath}")
    except Exception as e:
        print(f"Ошибка: {e}")
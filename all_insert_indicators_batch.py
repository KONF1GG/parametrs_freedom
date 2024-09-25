import asyncio
from aiochclient import ChClient
from aiohttp import ClientSession
import aiohttp
from dotenv import dotenv_values
import logging
from datetime import datetime, timedelta
from tqdm.asyncio import tqdm
from dateutil.relativedelta import relativedelta  # Используем для работы с месяцами

# Настройка логирования
logging.basicConfig(level=logging.INFO)
config = dotenv_values('.env')

MAX_CONCURRENT_INSERTS = 100
MAX_CONCURRENT_REQUESTS_TO_1C = 5
BATCH_SIZE = 10000  # Размер пакета вставки


def get_end_date():
    """
    Возвращает дату, которая на 3 месяца вперед от текущего.
    """
    return (datetime.now() + relativedelta(months=3)).replace(day=1)


async def get_data_from_setting(client):
    query = 'SELECT DISTINCT name, params, type FROM settings'
    try:
        all_rows = await client.fetch(query)
        return all_rows
    except Exception as e:
        logging.error(f"Ошибка выполнения запроса: {e}")
        return None


def get_first_days_of_month(year):
    return [datetime(year, month, 1).strftime('%Y%m%d') for month in range(1, 13)]


def get_query_url(name, params, request_date):
    query_param_string = f'http://server1c.freedom1.ru/UNF_CRM_WS/hs/Grafana/anydata?query={name}'
    for param in params.split(','):
        param = param.strip()
        query_param_string += f'&{param}' + (f'={request_date}' if param == 'dt_dt' else '')
    return query_param_string


async def get_urls_for_months(setting_name, params):
    """
    Генерирует URL для первого числа каждого месяца до текущего месяца + два месяца.
    """
    current_year = datetime.now().year
    end_date = get_end_date()

    # Генерируем запросы до конца месяца + 2 месяца
    request_dates = [datetime(current_year, month, 1).strftime('%Y%m%d') for month in range(1, end_date.month + 1)]

    return [get_query_url(setting_name, params, request_date) for request_date in request_dates]


async def get_urls_for_days(setting_name, params, start_date):
    """
    Генерирует URL для каждого дня, начиная с start_date, до текущего месяца + два месяца.
    """
    urls = []
    current_date = start_date
    end_date = get_end_date()

    while current_date <= end_date:
        query_url = get_query_url(setting_name, params, current_date.strftime('%Y%m%d'))
        urls.append(query_url)
        current_date += timedelta(days=1)

    return urls


async def fetch_json(session, url, semaphore):
    async with semaphore:
        try:
            logging.debug(f"Отправка запроса: {url}")
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                response.raise_for_status()
                logging.debug(f"Успешный ответ от {url}")
                json_data = await response.json()
                logging.info(f"Данные получены с {url}")
                return json_data
        except asyncio.TimeoutError:
            logging.error(f"Таймаут при запросе {url}")
        except Exception as e:
            logging.error(f"Ошибка при получении JSON из {url}: {e}")
        return None


async def insert_indicators(client, semaphore, indicators):
    async with semaphore:
        try:
            indicator_values = []
            for indicator in indicators:
                indicator_date = datetime.strptime(indicator['dt'], '%d.%m.%Y').date()
                indicator_values.append((
                    f"'{indicator_date}'",
                    f"'{indicator['prop']}'",
                    indicator['value'],
                    f"'{indicator.get('pick1')}'",
                    f"'{indicator.get('pick2')}'",
                    f"'{indicator.get('pick3')}'",
                    f"'{indicator.get('pick4')}'",
                    f"'{indicator.get('pick5')}'"
                ))

            # Формирование SQL-запроса
            insert_query = '''
            INSERT INTO grafana.indicators
            SELECT
                {} AS date,
                {} AS prop,
                {} AS value,
                {} AS pick1,
                {} AS pick2,
                {} AS pick3,
                {} AS pick4,
                {} AS pick5,
                cityHash64({}, {}, {}, {}, {}, {}, {}, {}) AS hash
            WHERE hash NOT IN (SELECT hash FROM grafana.indicators)
            '''

            values_placeholder = ', '.join(f'({", ".join(value)})' for value in indicator_values)

            await client.execute(insert_query.format(*values_placeholder))
        except Exception as e:
            logging.error(f"Ошибка при вставке индикаторов: {e}")


async def main():
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_INSERTS)  # Семафор для вставок в ClickHouse
    semaphore_1c = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS_TO_1C)  # Семафор для запросов к 1С

    async with ClientSession() as session:
        client = ChClient(session, url=config.get('URL'), user=config.get('USER'),
                          password=config.get('PASSWORD'), database=config.get('DATABASE'))
        settings_list = await get_data_from_setting(client)

        settings = {setting[0]: setting for setting in settings_list}

        for setting_name, setting_params in settings.items():
            if setting_name == 'planned':
                urls = await get_urls_for_months(setting_name, setting_params[1])
            else:
                start_date = datetime(2024, 1, 1)  # Начальная дата
                urls = await get_urls_for_days(setting_name, setting_params[1], start_date)

            fetch_tasks = [fetch_json(session, url, semaphore_1c) for url in urls]
            logging.info(f"Отправляем запросы для {setting_name}, всего запросов: {len(fetch_tasks)}")
            all_json_responses = await asyncio.gather(*fetch_tasks)

            insert_tasks = []
            indicators_batch = []
            for json_response in all_json_responses:
                if json_response:
                    for indicator in json_response:
                        indicators_batch.append(indicator)

                        # Если пакет полон, вставляем
                        if len(indicators_batch) >= BATCH_SIZE:
                            insert_tasks.append(insert_indicators(client, semaphore, indicators_batch))
                            indicators_batch = []  # Очищаем пакет для следующих данных

            # Вставляем оставшиеся индикаторы
            if indicators_batch:
                insert_tasks.append(insert_indicators(client, semaphore, indicators_batch))

            # Ожидаем завершения всех вставок
            await asyncio.gather(*insert_tasks)


if __name__ == '__main__':
    asyncio.run(main())

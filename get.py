import asyncio
from aiochclient import ChClient
from aiohttp import ClientSession
import aiohttp
from dotenv import dotenv_values
import logging
from datetime import datetime, timedelta
from tqdm.asyncio import tqdm
from dateutil.relativedelta import relativedelta
import json
import requests
import cityhash
import re
from get_logins import get_logins

# Настройка логирования
logging.basicConfig(level=logging.INFO)
config = dotenv_values('.env')

MAX_CONCURRENT_INSERTS = 10
MAX_CONCURRENT_REQUESTS_TO_1C = 5
BATCH_SIZE = 50  # Размер пакета для вставки

API_TOKEN = config.get('API_TOKEN')
CHAT_ID = config.get('CHAT_ID')

DELAY_BETWEEN_REQUESTS = 2


def send_telegram_message(message):
    url = f'https://api.telegram.org/bot{API_TOKEN}/sendMessage'
    data = {'chat_id': CHAT_ID, 'text': message}
    try:
        response = requests.post(url, data=data)
        if response.status_code != 200:
            logging.error(f"Ошибка отправки уведомления: {response.status_code}, {response.text}")
    except Exception as e:
        logging.error(f"Ошибка при отправке уведомления: {e}")


def get_end_date():
    return datetime.now()


async def get_data_from_setting(client):
    query = 'SELECT DISTINCT name, params, type, updateFrom, prop FROM settings'
    try:
        all_rows = await client.fetch(query)
        return all_rows
    except Exception as e:
        send_telegram_message(f"Ошибка выполнения запроса: {e}")
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


async def get_urls_for_months(setting_name, params, start_date, updateFrom=None):
    """
    Генерирует URL для первого числа каждого месяца начиная с даты start_date.
    """
    current_year = datetime.now().year
    end_date = get_end_date()

    request_dates = [datetime(current_year, month, 1).strftime('%Y%m%d') for month in
                     range(int(start_date.split('-')[1]), end_date.month + 1)]

    if updateFrom:
        request_dates.append(updateFrom.replace('-', ''))

    return [get_query_url(setting_name, params, request_date) for request_date in request_dates]


async def get_urls_for_days(setting_name, params, start_date, update_date=None):
    """
    Генерирует URL для каждого дня, начиная с start_date, до текущего месяца + два месяца.
    """
    urls = []
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = get_end_date()

    while current_date <= end_date:
        query_url = get_query_url(setting_name, params, current_date.strftime('%Y%m%d'))
        urls.append(query_url)
        current_date += timedelta(days=1)

    if update_date is not None:
        query_url = get_query_url(setting_name, params, update_date.replace('-', ''))
        urls.append(query_url)

    return urls


async def fetch_json(session, url, semaphore):
    async with semaphore:
        try:
            logging.debug(f"Отправка запроса: {url}")

            # Добавляем задержку перед каждым запросом
            await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                response.raise_for_status()
                logging.debug(f"Успешный ответ от {url}")

                json_data = await response.json()
                logging.info(f"Данные получены с {url}")

                # Извлечение даты из URL
                match = re.search(r'&dt_dt=(\d{8})', url)
                extracted_date = None
                if match:
                    extracted_date = datetime.strptime(match.group(1), '%Y%m%d').strftime('%d.%m.%Y')
                else:
                    logging.warning(f"Не удалось найти дату в URL {url}")

                # Обработка случая, когда json_data — это список
                if isinstance(json_data, list):
                    for item in json_data:
                        if isinstance(item, dict) and 'dt' not in item:
                            item['dt'] = extracted_date
                    return json_data

                # Обработка случая, когда json_data — это словарь
                elif isinstance(json_data, dict):
                    if 'dt' not in json_data:
                        json_data['dt'] = extracted_date
                    return json_data
                else:
                    logging.error(f"Ожидался словарь или список, но получен {type(json_data)}: {json_data}")
                    return None

        except asyncio.TimeoutError:
            logging.error(f"Таймаут при запросе {url}")
            send_telegram_message(f"Таймаут при запросе: {e}")
        except Exception as e:
            logging.error(f"Ошибка при получении JSON из {url}: {e}")
            send_telegram_message(f"Ошибка при получении JSON из: {e}")
        return None


async def insert_indicators(client, semaphore, indicators_batch):
    async with semaphore:
        if not indicators_batch:
            return

        value_strings = []
        for indicator in indicators_batch:
            date = datetime.strptime(indicator['dt'], '%d.%m.%Y').date()
            prop = indicator['prop']
            value = indicator.get('value', 0)
            variable1 = indicator.get('pick1', '')
            variable2 = indicator.get('pick2', '')
            variable3 = indicator.get('pick3', '')
            variable4 = indicator.get('pick4', '')
            variable5 = indicator.get('pick5', '')

            value_string = (
                f"SELECT "
                f"'{date}' AS date, "
                f"'{prop}' AS prop, "
                f"{value} AS value, "
                f"'{variable1}' AS variable1, "
                f"'{variable2}' AS variable2, "
                f"'{variable3}' AS variable3, "
                f"'{variable4}' AS variable4, "
                f"'{variable5}' AS variable5, "
                f"cityHash64('{date}', '{prop}', {value}, '{variable1}', '{variable2}', '{variable3}', '{variable4}', '{variable5}') AS hash"
            )
            value_strings.append(value_string)

        values = ' UNION ALL '.join(value_strings)

        query = f'''
            INSERT INTO grafana.indicators (date, prop, value, variable1, variable2, variable3, variable4, variable5, hash)
            SELECT *
            FROM (
                {values}
            ) AS tmp
            WHERE tmp.hash NOT IN (SELECT hash FROM grafana.indicators);
        '''
        try:
            await client.execute(query)
        except Exception as e:
            logging.error(f"Ошибка при вставке индикаторов: {e}")
            send_telegram_message(f"Ошибка при вставке индикаторов: {e}")


async def delete_data_from_db(client, start_date, end_date, setting_prop):
    query = f'''
    DELETE FROM grafana.indicators
    WHERE date >= '{start_date}' AND date <= '{end_date}' AND prop = '{setting_prop}'
    '''

    try:
        await client.execute(query)
    except Exception as e:
        logging.error(f"Ошибка при удалении данных: {e}")
        send_telegram_message(f"Ошибка при удалении данных: {e}")


async def main():
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_INSERTS)
    semaphore_1c = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS_TO_1C)

    async with ClientSession() as session:
        client = ChClient(session, url=config.get('URL'), user=config.get('USER'),
                          password=config.get('PASSWORD'), database=config.get('DATABASE'))
        settings_list = await get_data_from_setting(client)

        settings = {setting['name']: {'params': setting['params'],
                                      'type': setting['type'],
                                      'updateFrom': setting['updateFrom'],
                                      'prop': setting['prop']} for setting in settings_list}

        for setting_name, setting_info in settings.items():
            setting_params = setting_info['params']
            setting_type = setting_info['type']
            setting_updateFrom = setting_info['updateFrom']
            setting_prop = setting_info['prop']
            yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
            start_data_for_type_3 = (datetime.now()).strftime('%Y-%m-%d')
            end_date = (datetime.now()).strftime('%Y-%m-%d')

            # Определяем start_date, если есть updateFrom, то используем его
            if setting_updateFrom and setting_updateFrom != 'None':
                setting_updateFrom = datetime.strptime(setting_updateFrom, '%d.%m.%Y').strftime('%Y-%m-%d')
                start_date = setting_updateFrom
                if start_date < start_data_for_type_3:
                    start_data_for_type_3 = start_date
            else:
                start_date = yesterday

            match setting_type:
                case 1:
                    await delete_data_from_db(client, start_date, end_date, setting_prop)
                    urls = await get_urls_for_days(setting_name, setting_params,
                                                   start_date) if setting_name != 'planned' else await get_urls_for_months(
                        setting_name, setting_params, start_date)
                case 2:
                    await delete_data_from_db(client, start_date, end_date, setting_prop)
                    urls = await get_urls_for_months(setting_name, setting_params, start_date)
                case 3:
                    await delete_data_from_db(client, start_data_for_type_3, end_date, setting_prop)
                    urls = await get_urls_for_days(setting_name, setting_params,
                                                   start_date) if setting_name != 'planned' else await get_urls_for_months(
                        setting_name, setting_params, start_data_for_type_3)

            fetch_tasks = [fetch_json(session, url, semaphore_1c) for url in urls]
            logging.info(f"Отправляем запросы для {setting_name}, всего запросов: {len(fetch_tasks)}")
            all_json_responses = await asyncio.gather(*fetch_tasks)

            insert_tasks = []
            indicators_batch = []
            for json_response in all_json_responses:
                if json_response is not None and json_response:
                    for indicator in json_response:
                        indicators_batch.append(indicator)
                        if len(indicators_batch) >= BATCH_SIZE:
                            insert_tasks.append(insert_indicators(client, semaphore, indicators_batch))
                            indicators_batch = []

                if json_response is None or not json_response:
                    logging.info(f"Получен пустой ответ для настройки {setting_name}")
                    continue

            # Вставляем оставшиеся индикаторы, если есть
            if indicators_batch:
                insert_tasks.append(insert_indicators(client, semaphore, indicators_batch))

            # Ожидаем завершения всех задач вставки
            for _ in tqdm(asyncio.as_completed(insert_tasks), total=len(insert_tasks),
                          desc=f"Inserting Indicators for {setting_name}"):
                await _


if __name__ == "__main__":
    # send_telegram_message("Запуск программы парсинга логинов")
    # asyncio.run(get_logins())
    # send_telegram_message("Завершение программы парсинга логинов")
    send_telegram_message("Запуск программы для парсинга параметров")
    asyncio.run(main())
    send_telegram_message("Завершение программы парсинга параметров")

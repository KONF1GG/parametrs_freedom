import asyncio
from aiochclient import ChClient
from aiohttp import ClientSession
import aiohttp
from dotenv import dotenv_values
import logging
from datetime import datetime, timedelta
from tqdm.asyncio import tqdm
from dateutil.relativedelta import relativedelta  # Для работы с месяцами
import json
import requests
import cityhash



# Настройка логирования
logging.basicConfig(level=logging.INFO)
config = dotenv_values('.env')

MAX_CONCURRENT_INSERTS = 10
MAX_CONCURRENT_REQUESTS_TO_1C = 5
BATCH_SIZE = 100 # Размер пакета для вставки

API_TOKEN = config.get('API_TOKEN')
CHAT_ID = config.get('CHAT_ID')

# Функция для вычисления хеша
def city_hash64(*args):
    concatenated_string = ''.join(map(str, args))
    return cityhash.CityHash64(concatenated_string)

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
    """
    Возвращает дату, которая на два месяца вперед от текущего.
    """
    return (datetime.now() + relativedelta(months=2)).replace(day=1)


async def get_data_from_setting(client):
    query = 'SELECT DISTINCT name, params, type, updateFrom, prop FROM settings'
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


async def get_urls_for_months(setting_name, params, yesterday, updateFrom=None):
    """
    Генерирует URL для первого числа каждого месяца до текущего месяца + два месяца.
    """
    current_year = datetime.now().year
    end_date = get_end_date()

    # Генерируем запросы до конца месяца + 2 месяца
    request_dates = [datetime(current_year, month, 1).strftime('%Y%m%d') for month in
                     range(int(yesterday.split('-')[1]), end_date.month + 1)]
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
            await asyncio.sleep(1)
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


async def insert_indicators(client, semaphore, indicators_batch):
    async with semaphore:
        if not indicators_batch:
            return

        # Создаем строки значений для вставки с использованием UNION ALL и алиасов для каждого столбца
        value_strings = []
        for indicator in indicators_batch:
            value_string = (
                f"SELECT "
                f"'{datetime.strptime(indicator['dt'], '%d.%m.%Y').date()}' AS date, "
                f"'{indicator['prop']}' AS prop, "
                f"{indicator['value']} AS value, "
                f"'{indicator.get('pick1', '')}' AS variable1, "
                f"'{indicator.get('pick2', '')}' AS variable2, "
                f"'{indicator.get('pick3', '')}' AS variable3, "
                f"'{indicator.get('pick4', '')}' AS variable4, "
                f"'{indicator.get('pick5', '')}' AS variable5"
            )
            value_strings.append(value_string)

        # Объединяем строки с помощью UNION ALL
        values = ' UNION ALL '.join(value_strings)

        query = f'''
            INSERT INTO grafana.indicators (date, prop, value, variable1, variable2, variable3, variable4, variable5)
            SELECT *
            FROM (
                {values}
            ) AS tmp
            WHERE cityHash64(date, prop, value, variable1, variable2, variable3, variable4, variable5) NOT IN 
            (SELECT hash FROM grafana.indicators)
        '''
        try:
            # Выполняем запрос
            await client.execute(query)
        except Exception as e:
            logging.error(f"Ошибка при вставке индикаторов: {e}")


async def delete_data_from_db(client, start_date, end_date, setting_prop, updateFrom=None):
    query = '''
    DELETE FROM grafana.indicators
    WHERE date >= '{}' AND date <= '{}' AND prop = '{}'
    '''.format(start_date, end_date, setting_prop)

    if updateFrom != 'None':
        query += " OR date = '{}'".format(updateFrom)

    try:
        await client.execute(query)
    except Exception as e:
        logging.error(f"Ошибка при удалении данных: {e}")


async def main():

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_INSERTS)  # Семафор для вставок в ClickHouse
    semaphore_1c = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS_TO_1C)  # Семафор для запросов к 1С

    async with ClientSession() as session:
        client = ChClient(session, url=config.get('URL'), user=config.get('USER'),
                          password=config.get('PASSWORD'), database=config.get('DATABASE'))
        settings_list = await get_data_from_setting(client)

        # Изменяем на более понятный формат
        settings = {setting['name']: {'params': setting['params'],
                                      'type': setting['type'],
                                      'updateFrom': setting['updateFrom'],
                                      'prop': setting['prop']} for setting in settings_list}

        for setting_name, setting_info in settings.items():
            setting_params = setting_info['params']
            setting_type = setting_info['type']
            if setting_info['updateFrom'] != "None":
                setting_updateFrom = datetime.strptime(setting_info['updateFrom'], '%d.%m.%Y').strftime("%Y-%m-%d")
            else:
                setting_updateFrom = setting_info['updateFrom']
            setting_prop = setting_info['prop']
            yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')  # Начальная дата
            start_data_for_type_3 = (datetime.now() - relativedelta(months=3)).strftime('%Y-%m-%d')  # 3 месяца назад
            end_date = (datetime.now() - timedelta(1) + relativedelta(months=2)).strftime('%Y-%m-%d')  # 3 месяца вперед
            if setting_name != 'planned':
                continue
            match setting_type:
                case 1:
                    if setting_name == 'planned':
                        urls = await get_urls_for_months(setting_name, setting_params, yesterday)
                    else:
                        urls = await get_urls_for_days(setting_name, setting_params, yesterday)
                case 2:
                    await delete_data_from_db(client, yesterday, end_date, setting_prop, setting_updateFrom)
                    if setting_name == 'planned':
                        urls = await get_urls_for_months(setting_name, setting_params, yesterday, setting_updateFrom)
                    else:
                        urls = await get_urls_for_days(setting_name, setting_params, yesterday, setting_updateFrom)
                case 3:
                    await delete_data_from_db(client, start_data_for_type_3, end_date, setting_prop)
                    if setting_name == 'planned':
                        urls = await get_urls_for_months(setting_name, setting_params, yesterday)
                    else:
                        urls = await get_urls_for_days(setting_name, setting_params, yesterday)

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


if __name__ == '__main__':
    # send_telegram_message("Программа запущена")
    start = datetime.now()
    asyncio.run(main())
    print(datetime.now() - start)
    send_telegram_message(f"Программа завершена успешно"
                          f"время выполнения - {datetime.now() - start}")

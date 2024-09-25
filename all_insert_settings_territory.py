import requests
from clickhouse_driver import Client
from dotenv import dotenv_values
import datetime
import logging
import threading
import aiohttp
import asyncio

# Загрузка переменных из файла .env
config = dotenv_values('.env')

# Настройка логирования
logging.basicConfig(level=logging.INFO)


# Функция для вставки территории в базу данных с проверкой на дубликаты
def insert_territory(territory, results):
    try:
        client = Client(host=config.get('HOST'), user=config.get('USER'), password=config.get('PASSWORD'),
                        database=config.get('DATABASE'))
        client.execute('TRUNCATE TABLE settings')
        client.execute(
            '''
            INSERT INTO territories
            SELECT
                '{}',
                '{}',
                '{}',
                '{}',
                cityHash64('{}', '{}', '{}', '{}') AS hash
            WHERE hash NOT IN (SELECT hash FROM territories)
            '''.format(
                territory['name'],
                territory['group1'],
                territory['maingroup'],
                territory['department'],
                territory['name'],
                territory['group1'],
                territory['maingroup'],
                territory['department']
            )
        )
        logging.info(f"Inserting territory: {territory['name']}")
        results.append(f"Inserted territory: {territory['name']}")
    except Exception as e:
        logging.error(f"Ошибка при вставке территории: {e}")

def insert_setting(setting, results):
    try:
        client = Client(host=config.get('HOST'), user=config.get('USER'), password=config.get('PASSWORD'), database=config.get('DATABASE'))
        client.execute('TRUNCATE TABLE settings')
        client.execute(
            '''
            INSERT INTO settings
            SELECT
                '{}',
                '{}',
                '{}',
                '{}',
                '{}',
                '{}',
                '{}',
                '{}',
                '{}',
                '{}',
                cityHash64('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}') AS hash
            WHERE hash NOT IN (SELECT hash FROM settings)
            '''.format(
                setting['name'],
                setting['params'],
                setting.get('prop'),
                setting.get('pick1'),
                setting.get('pick2'),
                setting.get('pick3'),
                setting.get('pick4'),
                setting.get('pick5'),
                setting['type'],
                setting.get('updateFrom'),
                setting['name'],
                setting['params'],
                setting.get('prop'),
                setting.get('pick1'),
                setting.get('pick2'),
                setting.get('pick3'),
                setting.get('pick4'),
                setting.get('pick5')
            )
        )
        logging.info(f"Inserting setting: {setting['name']}")
        results.append(f"Inserted setting: {setting['name']}")
    except Exception as e:
        logging.error(f"Ошибка при вставке настройки: {e}")


# Async function to fetch data from the server
async def fetch_data_from_1c(session, url):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        logging.error(f"Ошибка при получении данных из 1С: {e}")
        return None


# Функция для загрузки данных территорий с потоками
def load_territories(territory_data):
    required_keys = ['name', 'group1', 'maingroup', 'department']
    results = []

    try:
        threads = []
        for territory in territory_data:
            if not all(key in territory for key in required_keys):
                logging.warning(f"Skipping territory due to missing keys: {territory}")
                continue

            if any(value == '' for value in territory.values()):
                logging.warning(f"Skipping territory due to empty values: {territory}")
                continue

            thread = threading.Thread(target=insert_territory, args=(territory, results))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for result in results:
            logging.info(result)
    except Exception as e:
        logging.error(f"An error occurred: {e}")


# Функция для загрузки данных настроек с потоками
def load_settings(setting_data):
    required_keys = ['name', 'params', 'prop', 'type']
    results = []

    try:
        threads = []
        for setting in setting_data:
            if not all(key in setting for key in required_keys):
                logging.warning(f"Skipping setting due to missing keys: {setting}")
                continue

            for i in range(1, 6):
                if f'pick{i}' not in setting:
                    setting[f'pick{i}'] = ''

            thread = threading.Thread(target=insert_setting, args=(setting, results))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for result in results:
            logging.info(result)
    except Exception as e:
        logging.error(f"An error occurred: {e}")


# Основная асинхронная функция
async def main():
    async with aiohttp.ClientSession() as session:
        # Получаем данные территорий
        territory_url = 'http://server1c.freedom1.ru/UNF_CRM_WS/hs/Grafana/anydata?query=territories'
        territory_data = await fetch_data_from_1c(session, territory_url)

        if territory_data:
            load_territories(territory_data)

        # Получаем данные настроек
        settings_url = 'http://server1c.freedom1.ru/UNF_CRM_WS/hs/Grafana/anydata?query=settings'
        settings_data = await fetch_data_from_1c(session, settings_url)

        if settings_data:
            load_settings(settings_data)


if __name__ == '__main__':
    start = datetime.datetime.now()
    asyncio.run(main())
    print(f"Total time: {datetime.datetime.now() - start}")

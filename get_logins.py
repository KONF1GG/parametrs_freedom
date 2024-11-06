import asyncio
from dotenv import dotenv_values
from aiochclient import ChClient
from aiohttp import ClientSession
import logging

config = dotenv_values('.env')
logging.basicConfig(level=logging.INFO)

CHUNK_SIZE = 1000  # Размер чанка


def escape_string(value):
    """Экранирует строковые значения для вставки в ClickHouse."""
    if value is None:
        return 'NULL'
    return "'{}'".format(value.replace("'", "''"))  # Экранирование одинарной кавычки


async def get_logins():
    async with ClientSession() as session:
        client = ChClient(session, url=config.get('URL'), user=config.get('USER'),
                          password=config.get('PASSWORD'), database=config.get('DATABASE'))

        # Удаление данных из таблицы
        delete_query = '''
        TRUNCATE TABLE Network.login_coords;
        '''
        try:
            await client.execute(delete_query)
            logging.info("Данные удалены из таблицы Network.login_coords.")
        except Exception as e:
            logging.error(f"Ошибка при удалении данных: {e}")
            return

        # Получение данных с API
        logins_query = 'http://server1c.freedom1.ru/UNF_CRM_WS/hs/Grafana/anydata?query=logins4map'
        try:
            async with session.get(logins_query) as response:
                data = await response.json()
                logging.info(f"Получено {len(data)} записей с API.")
        except Exception as e:
            logging.error(f"Ошибка при запросе данных: {e}")
            return

        # Подготовка и вставка данных по чанкам
        insert_query = '''
        INSERT INTO Network.login_coords (login, address, city, latitude, longitude, timeto, status) 
        VALUES
        '''

        # Обработка данных чанками
        for i in range(0, len(data), CHUNK_SIZE):
            chunk = data[i:i + CHUNK_SIZE]
            values = []
            for entry in chunk:
                # Формируем строку значений, используя escape_string для экранирования
                values.append(
                    f"({escape_string(entry.get('login'))}, {escape_string(entry.get('address'))}, {escape_string(entry.get('city'))}, "
                    f"{entry.get('latitude', 'NULL')}, {entry.get('longitude', 'NULL')}, "
                    f"{escape_string(entry.get('timeto', None))}, {escape_string(entry.get('status', None))})"
                )

            # Выполняем вставку, если есть данные в чанке
            if values:
                try:
                    await client.execute(insert_query + ", ".join(values))
                    logging.info(f"Чанк {i // CHUNK_SIZE + 1} успешно вставлен ({len(values)} записей).")
                except Exception as e:
                    logging.error(f"Ошибка при вставке чанка {i // CHUNK_SIZE + 1}: {e}")
            else:
                logging.info(f"Чанк {i // CHUNK_SIZE + 1} пуст, пропускаем.")


# Точка входа
if __name__ == "__main__":
    asyncio.run(get_logins())

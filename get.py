import asyncio
from aiochclient import ChClient
from aiohttp import ClientSession
import aiohttp
from dotenv import dotenv_values
import logging
from datetime import datetime, timedelta
from tqdm.asyncio import tqdm
from dateutil.relativedelta import relativedelta
import requests
import cityhash
import re
from get_logins import get_logins
import urllib.parse
import os
import json

# Настройка логирования
config = dotenv_values(".env")

# Создаем папку для логов если её нет
LOGS_DIR = "logs"
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

# Получаем текущую дату для именования файлов логов
current_date = datetime.now().strftime("%Y-%m-%d")
current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Настройка логирования в файлы
log_filename = os.path.join(LOGS_DIR, f"execution_{current_date}.log")
error_log_filename = os.path.join(LOGS_DIR, f"errors_{current_date}.log")

# Создаем форматтер для логов
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Настройка основного логгера
logger = logging.getLogger("main")
logger.setLevel(logging.INFO)

# Обработчик для основного лога
file_handler = logging.FileHandler(log_filename, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Обработчик для консоли
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Настройка логгера для ошибок
error_logger = logging.getLogger("errors")
error_logger.setLevel(logging.ERROR)

error_handler = logging.FileHandler(error_log_filename, encoding="utf-8")
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(formatter)
error_logger.addHandler(error_handler)

# Отключаем дублирование логов
logger.propagate = False
error_logger.propagate = False

# Глобальные переменные для статистики
execution_stats = {
    "start_time": None,
    "end_time": None,
    "total_settings": 0,
    "processed_settings": 0,
    "total_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
    "total_indicators": 0,
    "inserted_indicators": 0,
    "settings_details": [],
    "error_count": 0,
    "error_details": [],
}


def log_error(message, exception=None):
    """Логирует ошибку в файл ошибок и собирает статистику"""
    error_message = f"{message}: {str(exception)}" if exception else message

    # Логируем в файл ошибок
    error_logger.error(error_message)

    # Собираем статистику ошибок
    execution_stats["error_count"] += 1
    execution_stats["error_details"].append(
        {
            "timestamp": datetime.now().isoformat(),
            "message": error_message,
            "type": type(exception).__name__ if exception else "Error",
        }
    )


def log_info(message):
    """Логирует информационное сообщение"""
    logger.info(message)


def log_warning(message):
    """Логирует предупреждение"""
    logger.warning(message)


def create_telegram_error_report():
    """Создает отчет об ошибках для отправки в Telegram"""
    if execution_stats["error_count"] == 0:
        return None

    # Формируем отчет об ошибках
    report_lines = [
        "🚨 ОТЧЕТ ОБ ОШИБКАХ ВЫПОЛНЕНИЯ СКРИПТА",
        f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}",
        f"⏱️ Время выполнения: {execution_stats.get('duration', 'Неизвестно')}",
        "",
        f"❌ Всего ошибок: {execution_stats['error_count']}",
        f"📊 Обработано настроек: {execution_stats['processed_settings']}/{execution_stats['total_settings']}",
        f"🌐 Всего запросов: {execution_stats['total_requests']}",
        f"✅ Успешных: {execution_stats['successful_requests']}",
        f"❌ Неудачных: {execution_stats['failed_requests']}",
        "",
        "🔍 ДЕТАЛИ ОШИБОК:",
    ]

    # Добавляем детали ошибок (максимум 10 последних)
    recent_errors = execution_stats["error_details"][-10:]
    for i, error in enumerate(recent_errors, 1):
        timestamp = datetime.fromisoformat(error["timestamp"]).strftime("%H:%M:%S")
        error_type = error["type"]
        message = (
            error["message"][:100] + "..."
            if len(error["message"]) > 100
            else error["message"]
        )

        report_lines.append(f"{i}. [{timestamp}] {error_type}: {message}")

    if len(execution_stats["error_details"]) > 10:
        report_lines.append(
            f"... и еще {len(execution_stats['error_details']) - 10} ошибок"
        )

    report_lines.extend(
        [
            "",
            "📁 Подробные логи сохранены в папке logs/",
            f"📄 Файл ошибок: errors_{current_date}.log",
        ]
    )

    return "\n".join(report_lines)


def create_telegram_success_report():
    """Создает краткий отчет об успешном выполнении для Telegram"""
    report_lines = [
        "✅ СКРИПТ ВЫПОЛНЕН УСПЕШНО",
        f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}",
        f"⏱️ Время выполнения: {execution_stats.get('duration', 'Неизвестно')}",
        "",
        f"📊 Обработано настроек: {execution_stats['processed_settings']}/{execution_stats['total_settings']}",
        f"🌐 Всего запросов: {execution_stats['total_requests']}",
        f"✅ Успешных: {execution_stats['successful_requests']}",
        f"📈 Обработано индикаторов: {execution_stats['total_indicators']}",
        "",
        "📁 Подробные отчеты сохранены в папке logs/",
    ]

    return "\n".join(report_lines)


def create_execution_report():
    """Создает отчет о выполнении скрипта"""
    report_filename = os.path.join(LOGS_DIR, f"report_{current_datetime}.json")

    execution_stats["end_time"] = datetime.now().isoformat()
    execution_stats["duration"] = str(
        datetime.fromisoformat(execution_stats["end_time"])
        - datetime.fromisoformat(execution_stats["start_time"])
    )

    try:
        with open(report_filename, "w", encoding="utf-8") as f:
            json.dump(execution_stats, f, ensure_ascii=False, indent=2)

        log_info(f"Отчет о выполнении сохранен в файл: {report_filename}")

        # Также создаем текстовый отчет
        text_report_filename = os.path.join(LOGS_DIR, f"report_{current_datetime}.txt")
        with open(text_report_filename, "w", encoding="utf-8") as f:
            f.write("=== ОТЧЕТ О ВЫПОЛНЕНИИ СКРИПТА ===\n")
            f.write(f"Дата и время запуска: {execution_stats['start_time']}\n")
            f.write(f"Дата и время завершения: {execution_stats['end_time']}\n")
            f.write(f"Общее время выполнения: {execution_stats['duration']}\n")
            f.write(f"Всего настроек: {execution_stats['total_settings']}\n")
            f.write(f"Обработано настроек: {execution_stats['processed_settings']}\n")
            f.write(f"Всего запросов: {execution_stats['total_requests']}\n")
            f.write(f"Успешных запросов: {execution_stats['successful_requests']}\n")
            f.write(f"Неудачных запросов: {execution_stats['failed_requests']}\n")
            f.write(f"Всего индикаторов: {execution_stats['total_indicators']}\n")
            f.write(
                f"Вставлено индикаторов: {execution_stats['inserted_indicators']}\n"
            )
            f.write("\n=== ДЕТАЛИ ПО НАСТРОЙКАМ ===\n")

            for setting in execution_stats["settings_details"]:
                f.write(f"\nНастройка: {setting['name']}\n")
                f.write(f"  Тип: {setting['type']}\n")
                f.write(f"  Запросов: {setting['requests']}\n")
                f.write(f"  Успешных: {setting['successful']}\n")
                f.write(f"  Индикаторов: {setting['indicators']}\n")
                f.write(f"  Время обработки: {setting['processing_time']}\n")

        log_info(f"Текстовый отчет сохранен в файл: {text_report_filename}")

        # Отправляем отчет об ошибках в Telegram, если есть ошибки
        if SEND_ERROR_REPORT:
            telegram_report = create_telegram_error_report()
            if telegram_report:
                log_info("Отправляем отчет об ошибках в Telegram")
                send_telegram_message(telegram_report)
            else:
                log_info("Ошибок не обнаружено, отчет в Telegram не отправляется")

        # Отправляем отчет об успехе, если включено
        if SEND_SUCCESS_REPORT and execution_stats["error_count"] == 0:
            success_report = create_telegram_success_report()
            log_info("Отправляем отчет об успешном выполнении в Telegram")
            send_telegram_message(success_report)

    except Exception as e:
        log_error("Ошибка при создании отчета", e)


MAX_CONCURRENT_INSERTS = 5
MAX_CONCURRENT_REQUESTS_TO_1C = 5
BATCH_SIZE = 50  # Размер пакета для вставки

API_TOKEN = config.get("API_TOKEN")
CHAT_ID = config.get("CHAT_ID")

DELAY_BETWEEN_REQUESTS = 2

# Конфигурация для генерации дат
FUTURE_MONTHS_AHEAD = int(
    config.get("FUTURE_MONTHS_AHEAD", "3")
)  # Количество месяцев вперед
FIXED_START_YEAR = int(config.get("FIXED_START_YEAR", "2024"))  # Год начала данных
FIXED_START_MONTH = int(config.get("FIXED_START_MONTH", "1"))  # Месяц начала данных

# Дополнительные конфигурационные параметры
REQUEST_TIMEOUT = int(
    config.get("REQUEST_TIMEOUT", "30")
)  # Таймаут запросов в секундах
INSERT_RETRIES = int(config.get("INSERT_RETRIES", "3"))  # Количество попыток вставки
INSERT_DELAY = int(config.get("INSERT_DELAY", "2"))  # Задержка между попытками вставки

# Настройки уведомлений в Telegram
SEND_SUCCESS_REPORT = (
    config.get("SEND_SUCCESS_REPORT", "false").lower() == "true"
)  # Отправлять отчет об успехе
SEND_ERROR_REPORT = (
    config.get("SEND_ERROR_REPORT", "true").lower() == "true"
)  # Отправлять отчет об ошибках


def sanitize_string(value):
    """
    Очищает строку от апострофов и других потенциально опасных символов для SQL
    """
    if value is None:
        return ""
    # Заменяем апострофы на пустую строку или экранируем их
    return str(value).replace("'", "").replace('"', "")


def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{API_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": message}
    try:
        response = requests.post(url, data=data)
        if response.status_code != 200:
            log_error(
                f"Ошибка отправки уведомления: {response.status_code}, {response.text}"
            )
    except Exception as e:
        log_error(f"Ошибка при отправке уведомления: {e}")


def get_end_date():
    return datetime.now()


async def get_data_from_setting(client):
    query = "SELECT DISTINCT name, params, type, updateFrom FROM settings"
    try:
        all_rows = await client.fetch(query)
        log_info(f"Получено {len(all_rows)} настроек из базы данных")
        return all_rows
    except Exception as e:
        send_telegram_message(f"Ошибка выполнения запроса: {e}")
        log_error(f"Ошибка выполнения запроса: {e}")
        return None


def get_first_days_of_month(year):
    return [datetime(year, month, 1).strftime("%Y%m%d") for month in range(1, 13)]


def get_query_url(name, params, request_date):
    query_param_string = f"http://server1c.freedom1.ru/UNF_CRM_WS/hs/Grafana/anydata?query={urllib.parse.quote(name)}"
    for param in params.split(","):
        param = param.strip()
        query_param_string += f"&{param}" + (
            f"={urllib.parse.quote(request_date)}" if param == "dt_dt" else ""
        )
    return query_param_string


async def get_urls_for_months(setting_name, params, updateFrom=None):
    """
    Генерирует URL для первого числа каждого месяца начиная с конфигурируемой даты
    до текущего месяца + конфигурируемое количество месяцев вперед.
    """
    # Конфигурируемая начальная дата
    fixed_start_date = datetime(FIXED_START_YEAR, FIXED_START_MONTH, 1)

    # Текущая дата
    now_date = datetime.now()

    # Конечная дата: текущий месяц + конфигурируемое количество месяцев вперед
    end_date = now_date + relativedelta(months=FUTURE_MONTHS_AHEAD)
    end_date = end_date.replace(day=1)  # Первое число месяца

    # Генерация списка дат с первого числа каждого месяца
    request_dates = []
    current = fixed_start_date
    while current <= end_date:
        request_dates.append(current.strftime("%Y%m%d"))
        # Переход к следующему месяцу
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    # Если указан updateFrom, добавляем его в список дат
    if updateFrom:
        request_dates.append(updateFrom.replace("-", ""))

    # Генерация URL для каждой даты
    return [
        get_query_url(setting_name, params, request_date)
        for request_date in request_dates
    ]


async def get_urls_for_days(setting_name, params, start_date, update_date=None):
    """
    Генерирует URL для каждого дня, начиная с start_date, до текущего месяца + конфигурируемое количество месяцев.
    """
    urls = []
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.now() + relativedelta(months=FUTURE_MONTHS_AHEAD)

    while start_date_obj <= end_date:
        query_url = get_query_url(
            setting_name, params, start_date_obj.strftime("%Y%m%d")
        )
        urls.append(query_url)
        start_date_obj += timedelta(days=1)

    if update_date is not None:
        query_url = get_query_url(setting_name, params, update_date.replace("-", ""))
        urls.append(query_url)

    return urls


async def fetch_json(session, url, semaphore):
    async with semaphore:
        try:
            logging.debug(f"Отправка запроса: {url}")

            # Добавляем задержку перед каждым запросом
            await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            ) as response:
                response.raise_for_status()
                logging.debug(f"Успешный ответ от {url}")

                json_data = await response.json()
                logging.debug(f"Данные получены с {url}")

                # Извлечение даты из URL
                match = re.search(r"&dt_dt=(\d{8})", url)
                extracted_date = None
                if match:
                    extracted_date = datetime.strptime(
                        match.group(1), "%Y%m%d"
                    ).strftime("%d.%m.%Y")
                else:
                    log_warning(f"Не удалось найти дату в URL {url}")

                # Обработка случая, когда json_data — это список
                if isinstance(json_data, list):
                    for item in json_data:
                        if isinstance(item, dict) and "dt" not in item:
                            item["dt"] = extracted_date
                    return json_data

                # Обработка случая, когда json_data — это словарь
                elif isinstance(json_data, dict):
                    if "dt" not in json_data:
                        json_data["dt"] = extracted_date
                    return json_data
                else:
                    log_error(
                        f"Ожидался словарь или список, но получен {type(json_data)}: {json_data}"
                    )
                    return None

        except asyncio.TimeoutError:
            log_error(f"Таймаут при запросе {url}")
            send_telegram_message(f"Таймаут при запросе: {url}")
        except Exception as e:
            log_error(f"Ошибка при получении JSON из {url}: {e}")
            send_telegram_message(f"Ошибка при получении JSON из: {e}")
        return None


async def insert_indicators(
    client, semaphore, indicators_batch, retries=INSERT_RETRIES, delay=INSERT_DELAY
):
    async with semaphore:
        if not indicators_batch:
            return

        # Подготавливаем данные для параметризованного запроса
        data_to_insert = []
        for indicator in indicators_batch:
            date = datetime.strptime(indicator["dt"], "%d.%m.%Y").date()
            prop = sanitize_string(indicator["prop"])
            value = indicator.get("value", 0)
            variable1 = sanitize_string(indicator.get("pick1", ""))
            variable2 = sanitize_string(indicator.get("pick2", ""))
            variable3 = sanitize_string(indicator.get("pick3", ""))
            variable4 = sanitize_string(indicator.get("pick4", ""))
            variable5 = sanitize_string(indicator.get("pick5", ""))

            # Вычисляем хеш безопасно
            hash_value = cityhash.CityHash64(
                f"{date}{prop}{variable1}{variable2}{variable3}{variable4}{variable5}"
            )

            data_to_insert.append(
                {
                    "date": date,
                    "prop": prop,
                    "value": value,
                    "variable1": variable1,
                    "variable2": variable2,
                    "variable3": variable3,
                    "variable4": variable4,
                    "variable5": variable5,
                    "hash": hash_value,
                }
            )

        # Используем параметризованный запрос
        query = """
            INSERT INTO grafana.indicators (date, prop, value, variable1, variable2, variable3, variable4, variable5, hash)
            SELECT date, prop, value, variable1, variable2, variable3, variable4, variable5, hash
            FROM input('date Date, prop String, value Float64, variable1 String, variable2 String, variable3 String, variable4 String, variable5 String, hash UInt64')
            WHERE hash NOT IN (SELECT hash FROM grafana.indicators)
        """

        attempt = 0
        while attempt < retries:
            try:
                # Используем execute с данными
                await client.execute(query, data_to_insert)
                log_info(f"Успешно вставлено {len(data_to_insert)} индикаторов")
                break  # Выход из цикла при успешной отправке
            except Exception as e:
                log_error(f"Ошибка при вставке индикаторов: {e}")
                attempt += 1
                if attempt < retries:
                    await asyncio.sleep(delay)  # Пауза перед следующей попыткой
                else:
                    send_telegram_message(
                        f"Ошибка при вставке индикаторов после {retries} попыток: {e}"
                    )


async def delete_data_from_db(client, start_date, end_date, setting_name):
    # Запрос на удаление данных с JOIN через подзапрос
    delete_query = f"""
    ALTER TABLE grafana.indicators DELETE 
    WHERE date >= '{start_date}' 
      AND date <= '{end_date}' 
      AND prop IN (
        SELECT prop 
        FROM grafana.settings 
        WHERE name = '{setting_name}'
      )
    """

    # Запрос на оптимизацию таблицы после удаления данных
    optimize_query = "OPTIMIZE TABLE grafana.indicators FINAL"

    try:
        # Выполнение запроса на удаление данных
        await client.execute(delete_query)

        # Выполнение запроса на оптимизацию таблицы
        await client.execute(optimize_query)

        log_info(
            f"Данные за период с {start_date} по {end_date} успешно удалены и таблица оптимизирована."
        )
    except Exception as e:
        log_error(f"Ошибка при удалении или оптимизации данных: {e}")
        send_telegram_message(f"Ошибка при удалении или оптимизации данных: {e}")


async def main():
    # Инициализация статистики
    execution_stats["start_time"] = datetime.now().isoformat()
    log_info("=== НАЧАЛО ВЫПОЛНЕНИЯ СКРИПТА ===")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_INSERTS)
    semaphore_1c = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS_TO_1C)

    async with ClientSession() as session:
        client = ChClient(
            session,
            url=config.get("URL"),
            user=config.get("USER"),
            password=config.get("PASSWORD"),
            database=config.get("DATABASE"),
        )
        settings_list = await get_data_from_setting(client)

        if settings_list is None:
            log_error("Не удалось получить список настроек")
            return

        settings = {
            setting["name"]: {
                "params": setting["params"],
                "type": setting["type"],
                "updateFrom": setting["updateFrom"],
            }
            for setting in settings_list
        }

        execution_stats["total_settings"] = len(settings)
        log_info(f"Найдено {len(settings)} настроек для обработки")

        # Вычисляем даты один раз для всех настроек
        now = datetime.now()
        yesterday = (now - timedelta(1)).strftime("%Y-%m-%d")
        start_data_for_type_3 = now.strftime("%Y-%m-%d")
        end_date = now.strftime("%Y-%m-%d")

        for setting_name, setting_info in settings.items():
            setting_start_time = datetime.now()
            log_info(f"Обработка настройки: {setting_name}")

            setting_params = setting_info["params"]
            setting_type = setting_info["type"]
            setting_updateFrom = setting_info["updateFrom"]

            # Определяем start_date, если есть updateFrom, то используем его
            if setting_updateFrom and setting_updateFrom != "None":
                setting_updateFrom = datetime.strptime(
                    setting_updateFrom, "%d.%m.%Y"
                ).strftime("%Y-%m-%d")
                start_date = setting_updateFrom
                if start_date < start_data_for_type_3:
                    start_data_for_type_3 = start_date
            else:
                start_date = yesterday

            # Статистика для текущей настройки
            setting_stats = {
                "name": setting_name,
                "type": setting_type,
                "requests": 0,
                "successful": 0,
                "indicators": 0,
                "processing_time": None,
            }

            match setting_type:
                case 1:
                    await delete_data_from_db(
                        client, start_date, end_date, setting_name
                    )
                    urls = (
                        await get_urls_for_days(
                            setting_name, setting_params, start_date
                        )
                        if setting_name != "planned"
                        else await get_urls_for_months(setting_name, setting_params)
                    )
                case 2:
                    await delete_data_from_db(
                        client, start_date, end_date, setting_name
                    )
                    urls = await get_urls_for_months(setting_name, setting_params)
                case 3:
                    await delete_data_from_db(
                        client, start_data_for_type_3, end_date, setting_name
                    )
                    urls = (
                        await get_urls_for_days(
                            setting_name, setting_params, start_date
                        )
                        if setting_name != "planned"
                        else await get_urls_for_months(setting_name, setting_params)
                    )

            setting_stats["requests"] = len(urls)
            execution_stats["total_requests"] += len(urls)

            fetch_tasks = [fetch_json(session, url, semaphore_1c) for url in urls]
            log_info(
                f"Отправляем запросы для {setting_name}, всего запросов: {len(fetch_tasks)}"
            )

            # Используем asyncio.as_completed для лучшей обработки ошибок
            all_json_responses = []
            for task in asyncio.as_completed(fetch_tasks):
                try:
                    result = await task
                    all_json_responses.append(result)
                    if result is not None:
                        setting_stats["successful"] += 1
                        execution_stats["successful_requests"] += 1
                    else:
                        execution_stats["failed_requests"] += 1
                except Exception as e:
                    log_error(f"Ошибка при выполнении запроса для {setting_name}: {e}")
                    all_json_responses.append(None)
                    execution_stats["failed_requests"] += 1

            insert_tasks = []
            indicators_batch = []
            empty_responses_count = 0

            for json_response in all_json_responses:
                if json_response is not None and json_response:
                    for indicator in json_response:
                        indicators_batch.append(indicator)
                        setting_stats["indicators"] += 1
                        execution_stats["total_indicators"] += 1
                        if len(indicators_batch) >= BATCH_SIZE:
                            insert_tasks.append(
                                insert_indicators(client, semaphore, indicators_batch)
                            )
                            indicators_batch = []
                else:
                    empty_responses_count += 1

            # Логируем только общее количество пустых ответов
            if empty_responses_count > 0:
                log_info(
                    f"Получено {empty_responses_count} пустых ответов для настройки {setting_name}"
                )

            # Вставляем оставшиеся индикаторы, если есть
            if indicators_batch:
                insert_tasks.append(
                    insert_indicators(client, semaphore, indicators_batch)
                )

            # Ожидаем завершения всех задач вставки
            for _ in tqdm(
                asyncio.as_completed(insert_tasks),
                total=len(insert_tasks),
                desc=f"Inserting Indicators for {setting_name}",
            ):
                await _

            # Завершаем статистику для настройки
            setting_end_time = datetime.now()
            setting_stats["processing_time"] = str(
                setting_end_time - setting_start_time
            )
            execution_stats["settings_details"].append(setting_stats)
            execution_stats["processed_settings"] += 1

            log_info(
                f"Настройка {setting_name} обработана за {setting_stats['processing_time']}"
            )
            log_info(f"  - Запросов: {setting_stats['requests']}")
            log_info(f"  - Успешных: {setting_stats['successful']}")
            log_info(f"  - Индикаторов: {setting_stats['indicators']}")

        log_info("=== ЗАВЕРШЕНИЕ ОБРАБОТКИ ВСЕХ НАСТРОЕК ===")
        execution_stats["inserted_indicators"] = execution_stats[
            "total_indicators"
        ]  # Предполагаем, что все вставлены

        # Создаем отчет
        create_execution_report()

        log_info("=== СКРИПТ ЗАВЕРШЕН УСПЕШНО ===")


if __name__ == "__main__":
    # send_telegram_message("Запуск программы парсинга логинов")
    asyncio.run(get_logins())
    # send_telegram_message("Завершение программы парсинга логинов")
    # send_telegram_message("Запуск программы для парсинга параметров")
    asyncio.run(main())
    # send_telegram_message("Завершение программы парсинга параметров ")

import requests
import csv
import logging
import time
import os
from datetime import datetime

# Лог-файл
log_file = f"gosagro_parser_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("gosagro_parser")

BASE_URL = "https://gosagro.kz/restapi/query/get"
API_PARAMS = {
    "code": "report$its_req_public",
    "preparam1": "",
    "preparam2": "",
    "preparam3": "",
    "preparam4": "2023-01-01 00:00:00",
    "preparam5": datetime.now().strftime("%Y-%m-%d 23:59:59")
}
PER_PAGE = 500

SAVE_FILE = f"gosagro_reestr_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

CHECKPOINT_FILE = "CheckReestr.txt"
seen_docnums = set()


def fetch_data(page):
    params = API_PARAMS.copy()
    params["page"] = page
    params["perpage"] = PER_PAGE

    try:
        logger.info(f" Запрос страницы {page}...")
        response = requests.get(BASE_URL, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()

        if "items" in data and isinstance(data["items"], list) and len(data["items"]) > 0:
            # Фильтруем дубликаты по `docnum`
            unique_data = [item for item in data["items"] if item["docnum"] not in seen_docnums]
            for item in unique_data:
                seen_docnums.add(item["docnum"])

            logger.info(f" Страница {page} загружена. Всего: {len(data['items'])}, Уникальных: {len(unique_data)}.")
            return unique_data
        else:
            logger.warning(f" Пустая страница {page}. Остановка парсинга.")
            return []

    except requests.exceptions.Timeout:
        logger.error(f" Тайм-аут на странице {page}. Повтор через 10 сек...")
        time.sleep(10)
        return fetch_data(page)  # Повторяем запрос
    except requests.exceptions.RequestException as e:
        logger.error(f" Ошибка на странице {page}: {e}")
        return []


def get_last_page():
    """Читаем номер последней загруженной страницы из CheckReestr.txt"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return 1
    return 1


def save_checkpoint(page):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        f.write(str(page))


def run_parser():
    logger.info("Запуск парсера Gosagro")
    start_page = get_last_page()
    logger.info(f" Продолжение с {start_page}-й страницы.")
    logger.info(f" Данные будут сохранены в новый файл: {SAVE_FILE}")

    with open(SAVE_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = None
        buffered_write = []
        page = start_page

        while True:
            try:
                data = fetch_data(page)
                if not data:
                    break  # Если пустая страница – завершаем парсинг

                # Инициализируем writer при первой непустой партии данных
                if writer is None:
                    fieldnames = data[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()

                buffered_write.extend(data)

                # Записываем буфер большими порциями, чтобы не дёргать файл каждую запись
                if len(buffered_write) >= 5000:
                    writer.writerows(buffered_write)
                    csvfile.flush()
                    logger.info(f" Записано {len(buffered_write)} записей в файл.")
                    buffered_write.clear()

                # Сохраняем чекпоинт
                save_checkpoint(page)

                page += 1  # Следующая страница
                time.sleep(2)  # Пауза между запросами

            except Exception as e:
                logger.error(f" Ошибка обработки страницы {page}: {e}")
                time.sleep(5)

        # Записываем остаток буфера, если остался
        if buffered_write:
            writer.writerows(buffered_write)
            csvfile.flush()
            logger.info(f" Последние {len(buffered_write)} записей сохранены.")

    logger.info(f" Парсер завершил работу. Итоговый файл: {SAVE_FILE}")


if __name__ == "__main__":
    run_parser()

import requests
import csv
import logging
import argparse
import time
import os
from datetime import datetime

log_file = f"gosagro_parser_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("gosagro_parser")


class GosagroParser:
    def __init__(self, api_url="https://gosagro.kz/restapi/query/get"):
        """Инициализация парсера"""
        self.api_url = api_url
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://gosagro.kz/",
            "Connection": "keep-alive"
        }
        self.checkpoint_file = "CheckList.txt"

    def fetch_data(self, params):
        """Запрос данных с API"""
        try:
            logger.info(f" Отправка запроса: {self.api_url} с параметрами {params}")
            response = requests.get(self.api_url, headers=self.headers, params=params)
            response.raise_for_status()

            data = response.json()
            logger.info(f" Ответ сервера содержит ключи: {data.keys()}")

            if "items" in data and isinstance(data["items"], list):
                logger.info(f" API вернул {len(data['items'])} записей.")
                return data["items"]
            else:
                logger.warning("⚠ API вернул пустой список `items`.")
                return []
        except requests.exceptions.RequestException as e:
            logger.error(f" Ошибка при запросе к API: {e}")
            return []

    def get_last_page(self):
        """Читаем чекпоинт (последняя загруженная страница)"""
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                try:
                    return int(f.read().strip())
                except ValueError:
                    return 1
        return 1

    def save_checkpoint(self, page):
        """Сохранение номера последней загруженной страницы"""
        with open(self.checkpoint_file, "w", encoding="utf-8") as f:
            f.write(str(page))

    def fetch_all_reports(self, start_date, end_date, csv_path):
        """
        Запрашивает отчёт за период с 'start_date' по 'end_date'
        и сохраняет данные в новый CSV-файл, начиная с сохраненного
        чекпоинта (номер страницы).
        """
        page = self.get_last_page()
        logger.info(f" Продолжим парсинг с {page}-й страницы (чекпоинт).")

        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = None

            while True:
                params = {
                    "code": "report$its_req_wait",
                    "preparam1": "",
                    "preparam2": "",
                    "preparam3": start_date,
                    "preparam4": end_date,
                    "page": page,
                    "perpage": 500
                }

                data = self.fetch_data(params)

                if not data:
                    logger.info(f" Пустая страница №{page}. Парсинг завершён.")
                    break

                if writer is None:
                    fieldnames = data[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()

                writer.writerows(data)
                csvfile.flush()
                os.fsync(csvfile.fileno())

                logger.info(f" Сохранено {len(data)} записей в файл: {csv_path}")

                self.save_checkpoint(page)
                page += 1

                time.sleep(2)

    def run(self, csv_path):
        logger.info(" Запуск парсера Gosagro")
        logger.info(f" Результирующий файл: {csv_path}")

        start_date = "2023-01-01 00:00:00"
        end_date = datetime.now().strftime("%Y-%m-%d 23:59:59")

        self.fetch_all_reports(start_date, end_date, csv_path)

        logger.info(" Парсер Gosagro успешно завершил работу.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Парсер API Gosagro")
    parser.add_argument("--output", help="Путь для сохранения файла CSV", required=False)
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = args.output if args.output else f"gosagro_reports_{timestamp}.csv"

    gosagro_parser = GosagroParser()
    gosagro_parser.run(csv_filename)

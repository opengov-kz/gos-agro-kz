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
        self.checkpoint_file = "CheckList.txt"  # Файл для чекпоинта

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

    def save_to_csv(self, data, csv_path):
        """Сохранение данных в CSV"""
        if not data:
            logger.warning("⚠ Список данных пуст. CSV не будет обновлен.")
            return

        file_exists = os.path.exists(csv_path)
        fieldnames = data[0].keys()

        with open(csv_path, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()  # Записываем заголовки, если файл новый

            writer.writerows(data)
            csvfile.flush()
            os.fsync(csvfile.fileno())  # Принудительно записываем на диск

        logger.info(f" Данные сохранены в {csv_path} ({len(data)} записей).")

    def get_last_page(self):
        """Читаем чекпоинт (последняя загруженная страница)"""
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, "r") as f:
                try:
                    return int(f.read().strip())
                except ValueError:
                    return 1
        return 1

    def save_checkpoint(self, page):
        """Сохранение номера последней загруженной страницы"""
        with open(self.checkpoint_file, "w") as f:
            f.write(str(page))

    def fetch_all_reports(self, start_date, end_date, csv_path):
        """Запрашивает отчет за весь период с постраничной загрузкой"""
        page = self.get_last_page()

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
            if data:
                self.save_to_csv(data, csv_path)  # Дописываем данные в CSV
                self.save_checkpoint(page)  # Обновляем чекпоинт
                page += 1
            else:
                break  # Если данных нет, завершаем

            logger.info(f" Загружено {page - self.get_last_page()} страниц.")
            time.sleep(2)  # Пауза перед следующим запросом

    def run(self, csv_path):
        """Основной метод для запуска парсинга"""
        logger.info(" Запуск парсера Gosagro")
        start_date = "2023-01-01 00:00:00"
        end_date = datetime.now().strftime("%Y-%m-%d 23:59:59")

        self.fetch_all_reports(start_date, end_date, csv_path)

        logger.info(" Парсер Gosagro успешно завершил работу.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Парсер API Gosagro")
    parser.add_argument("--output", help="Путь для сохранения файла CSV", required=False)
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = args.output if args.output else "gosagro_reports.csv"

    logger.info(f" Файл будет сохранен по пути: {csv_filename}")

    parser = GosagroParser()
    parser.run(csv_filename)

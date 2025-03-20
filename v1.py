import requests
import csv
import logging
import argparse
import time
from datetime import datetime

# Настройка логирования (UTF-8)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"gosagro_parser_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding="utf-8"),
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

    def fetch_data(self, params):
        """
        Получение данных из API
        """
        try:
            logger.info(f" Отправка запроса: {self.api_url} с параметрами {params}")
            response = requests.get(self.api_url, headers=self.headers, params=params)
            response.raise_for_status()  # Проверка на ошибки HTTP

            data = response.json()
            logger.info(f" Ответ сервера: {data.keys()}")

            # Извлекаем данные из ключа 'items', а не 'data'
            if "items" in data and isinstance(data["items"], list):
                logger.info(f" API вернул {len(data['items'])} записей.")
                return data["items"]
            else:
                logger.warning("⚠️ API ответил 200 OK, но список `items` пуст.")
                return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при запросе к API: {e}")
            return []

    def save_to_csv(self, data, csv_path):
        """Сохранение данных в CSV"""
        if not data:
            logger.warning("Список данных пуст. CSV не будет создан.")
            return

        fieldnames = data[0].keys()
        logger.info(f"💾 Сохранение {len(data)} записей в {csv_path}")

        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        logger.info(f" Данные успешно сохранены в {csv_path}")

    def fetch_all_reports(self, start_date, end_date):
        """Запрашивает отчет за весь период с постраничной загрузкой"""
        all_data = []
        page = 1

        while True:
            params = {
                "code": "report$its_req_wait",
                "preparam1": "",
                "preparam2": "",
                "preparam3": start_date,
                "preparam4": end_date,
                "page": page,
                "perpage": 200
            }

            data = self.fetch_data(params)
            if data:
                all_data.extend(data)
                page += 1
            else:
                break

            logger.info(f"📥 Загружено {len(all_data)} записей на {page-1} страницах.")
            time.sleep(1)

        return all_data

    def run(self, csv_path):
        """Основной метод для запуска парсинга"""
        logger.info("🚀 Запуск парсера Gosagro")
        start_date = "2023-01-01 00:00:00"
        end_date = datetime.now().strftime("%Y-%m-%d 23:59:59")

        reports_data = self.fetch_all_reports(start_date, end_date)
        self.save_to_csv(reports_data, csv_path)

        logger.info("✅ Парсер Gosagro успешно завершил работу.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Парсер API Gosagro")
    parser.add_argument("--output", help="Путь для сохранения файла CSV", required=False)
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = args.output if args.output else f"gosagro_reports_{timestamp}.csv"

    logger.info(f"📁 Файл будет сохранен по пути: {csv_filename}")

    parser = GosagroParser()
    parser.run(csv_filename)

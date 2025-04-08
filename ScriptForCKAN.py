import os
from ckanapi import RemoteCKAN

CKAN_URL = "https://data.opengov.kz"


API_KEY = os.getenv("CKNA_API_KEY", "ВАШ_ТОКЕН_ЗДЕСЬ")


ORGANIZATION_ID = "your_organization_id_or_name"


def create_dataset(remote_ckan, dataset_name, title, owner_org):
    """
    Создаёт новый набор данных (package) на CKAN.
    :param remote_ckan: Объект RemoteCKAN (ckanapi)
    :param dataset_name: Системное имя набора (slug, уникальное имя)
    :param title: Заголовок набора
    :param owner_org: ID или имя организации
    :return: Словарь, содержащий данные о созданном наборе
    """
    dataset_dict = {
        "name": dataset_name,
        "title": title,
        "owner_org": owner_org,
        "notes": "Описание набора данных, цель и т.п."

    }
    return remote_ckan.action.package_create(**dataset_dict)


def upload_resource(remote_ckan, package_id_or_name, filepath, resource_name):
    """
    Загружает файл как ресурс в уже существующийнабор данных.
    :param remote_ckan: Объект RemoteCKAN (ckana pi)
    :param package_id_or_name: id или системное name набора, к которому добавляется ресурс
    :param filepath: путь к локальному файлу, который будем загружать
    :param resource_name: Название (label) для ресурса
    :return: Словарь с информацией о созданном ресурсе
    """
    resource_dict = {
        "package_id": package_id_or_name,
        "name": resource_name,
        "format": "CSV",
        "upload": open(filepath, "rb"),
    }
    return remote_ckan.action.resource_create(**resource_dict)


def main():
    ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)

    dataset_name = "my-test-dataset"
    title = "Мой тестовый набор"
    try:
        created_dataset = create_dataset(ckan, dataset_name, title, ORGANIZATION_ID)
        print(f"Набор данных создан: {created_dataset.get('name')}")
    except Exception as e:
        print(f"Ошибка при создании набора данных: {e}")
        return

    filepath = "test_data.csv"
    resource_name = "Мой CSV-ресурс"
    try:
        created_resource = upload_resource(ckan, dataset_name, filepath, resource_name)
        print(f"Ресурс создан, ID ресурса: {created_resource.get('id')}")
    except Exception as e:
        print(f"Ошибка при загрузке ресурса: {e}")


if __name__ == "__main__":
    main()

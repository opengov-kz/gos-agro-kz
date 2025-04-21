import os
from ckanapi import RemoteCKAN

CKAN_URL = "https://data.opengov.kz"


API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJBR2NidmhKS25Dc3UwakhFRGxOdjNkNk8weloyTTFNQmlRV0tRT2xYX0djIiwiaWF0IjoxNzQ0ODk2NDMwfQ.GNkioILw10d1Yi6GzB63ky5NV0pCh_Ub5vv8vP2EyCg"
ORGANIZATION_ID = "gosagro"


def create_dataset(remote_ckan, dataset_name, title, owner_org):
    dataset_dict = {
        "name": dataset_name,
        "title": title,
        "owner_org": owner_org,
  # это для листа ожидания
        "notes":"""Содержит информацию о заявках, находящихся в листе ожидания на получение субсидий в агропромышленном комплексе.
Эти заявки ещё не были рассмотрены окончательно или не попали в бюджетный реестр.

- amount (numeric): Запрошенная сумма субсидии.
- cat_id (integer): Идентификатор категории субсидирования.
- doc_at (date): Дата подачи документа (заявки).
- docnum (string): Уникальный номер документа.
- its_cli_title (string): Название организации (заявителя).
- region_id (integer): Код региона подачи заявки.
- stat_id (integer): Код текущего статуса заявки.
"""
#         "notes": """Содержит информацию о заявках, прошедших финальный этап обработки и внесённых в реестр бюджета по субсидированию в агропромышленном комплексе.
#          Включает как одобренные, так и отклонённые или отозванные заявки.
#
#     - amount (numeric): Фактическая сумма, указанная в заявке.
#     - doc_at (date): Дата регистрации документа в реестре.
#     - docnum (string): Уникальный номер документа.
#     - its_cli_title (string): Название организации (заявителя).
#     - its_ga_r_sub_category_id (integer): Идентификатор подкатегории субсидирования.
#     - recall_dt (date): Дата отзыва заявки (если применимо).
#     - recall_txt (string): Причина отзыва (если указана).
#     - refuse_dt (date): Дата отказа (если применимо).
#     - refuse_txt (string): Причина отказа (если указана).
#     - region_id (integer): Код региона подачи заявки.
#     - stat_id (integer): Код итогового статуса заявки.
#     """
    }
    return remote_ckan.action.package_create(**dataset_dict)


def upload_resource(remote_ckan, package_id_or_name, filepath, resource_name):
    resource_dict = {
        "package_id": package_id_or_name,
        "name": resource_name,
        "format": "CSV",
        "upload": open(filepath, "rb"),
    }
    return remote_ckan.action.resource_create(**resource_dict)


def main():
    ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
    filepath = "C:/Users/tzhex/PycharmProjects/Parcer/list/gosagro_list_20250421_215004.csv"

    filename = os.path.basename(filepath)
    base_name, ext = os.path.splitext(filename)

    dataset_name = base_name
    title = base_name

    try:
        created_dataset = create_dataset(ckan, dataset_name, title, ORGANIZATION_ID)
        print(f"Набор данных создан: {created_dataset.get('name')}")
    except Exception as e:
        print(f"Ошибка при создании набора данных: {e}")
        return

    resource_name = filename

    try:
        created_resource = upload_resource(ckan, dataset_name, filepath, resource_name)
        print(f"Ресурс создан, ID ресурса: {created_resource.get('id')}")
    except Exception as e:
        print(f"Ошибка при загрузке ресурса: {e}")


if __name__ == "__main__":
    main()

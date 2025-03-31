import csv
import psycopg2

conn = psycopg2.connect(
    dbname="GosAgro",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
conn.autocommit = False
cur = conn.cursor()

# ПОИСКА/ДОБАВЛЕНИЯ СПРАВОЧНИКОВ
def get_or_insert_region(region_name):

    if not region_name or region_name.strip()   == "":
        return None
    cur.execute("SELECT id FROM regions WHERE name = %s;", (region_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        cur.execute("INSERT INTO regions (name) VALUES (%s) RETURNING id;", (region_name,))
        return cur.fetchone()[0]

def get_or_insert_organization(org_name):
    if not org_name or org_name.strip() == "":
        return None
    cur.execute("SELECT id FROM organizations WHERE name = %s;", (org_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        cur.execute("INSERT INTO organizations (name) VALUES (%s) RETURNING id;", (org_name,))
        return cur.fetchone()[0]

def get_or_insert_status(status_name):
    if not status_name or status_name.strip() == "":
        return None
    cur.execute("SELECT id FROM statuses WHERE name = %s;", (status_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        cur.execute("INSERT INTO statuses (name) VALUES (%s) RETURNING id;", (status_name,))
        return cur.fetchone()[0]

def get_or_insert_category(cat_name):
    if not cat_name or cat_name.strip() == "":
        return None
    cur.execute("SELECT id FROM categories WHERE name = %s;", (cat_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        cur.execute("INSERT INTO categories (name) VALUES (%s) RETURNING id;", (cat_name,))
        return cur.fetchone()[0]


# ЗАГРУЗКА РЕЕСТР БЮДЖЕТА

budget_csv_path = "Reestr/gosagro_reestr.csv"
with open(budget_csv_path, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        amount   = row.get("amount")
        doc_at   = row.get("doc_at")
        docnum   = row.get("docnum")
        org_name = row.get("its_cli_title")
        cat_name = row.get("its_ga_r_sub_category_id")
        recall_dt= row.get("recall_dt")
        recall_txt = row.get("recall_txt")
        refuse_dt= row.get("refuse_dt")
        refuse_txt= row.get("refuse_txt")
        region_name = row.get("region_id")
        status_name = row.get("stat_id")

        amount_val = float(amount) if amount else None

        org_id    = get_or_insert_organization(org_name)
        region_id = get_or_insert_region(region_name)
        status_id = get_or_insert_status(status_name)
        cat_id    = get_or_insert_category(cat_name)

        cur.execute("""
            INSERT INTO documents
              (doc_num, doc_at, summa_subsidy, org_id, region_id, status_id, category_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (docnum, doc_at, amount_val, org_id, region_id, status_id, cat_id))
        new_doc_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO budget_registry (id)
            VALUES (%s)
        """, (new_doc_id,))

        if refuse_dt or refuse_txt:
            cur.execute("""
                INSERT INTO refuses (refuse_txt, refuse_at, budget_registry_id)
                VALUES (%s, %s, %s)
            """, (refuse_txt, refuse_dt, new_doc_id))

        if recall_dt or recall_txt:
            cur.execute("""
                INSERT INTO recalls (recall_txt, recall_at, budget_registry_id)
                VALUES (%s, %s, %s)
            """, (recall_txt, recall_dt, new_doc_id))

#  ЗАГРУЗКА ЛИСТ ОЖИДАНИЯ
waiting_csv_path = "list/gosagro_list_last.csv"
with open(waiting_csv_path, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        amount   = row.get("amount")
        doc_at   = row.get("doc_at")
        docnum   = row.get("docnum")
        org_name = row.get("its_cli_title")
        cat_name = row.get("cat_id")
        region_name = row.get("region_id")
        status_name = row.get("stat_id")

        amount_val = float(amount) if amount else None

        org_id    = get_or_insert_organization(org_name)
        region_id = get_or_insert_region(region_name)
        status_id = get_or_insert_status(status_name)
        cat_id    = get_or_insert_category(cat_name)


        cur.execute("""
            INSERT INTO documents
              (doc_num, doc_at, summa_subsidy, org_id, region_id, status_id, category_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (docnum, doc_at, amount_val, org_id, region_id, status_id, cat_id))
        new_doc_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO waiting_lists (id)
            VALUES (%s)
        """, (new_doc_id,))

conn.commit()

cur.close()
conn.close()

print("Загрузка CSV завершена успешно!")

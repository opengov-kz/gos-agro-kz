-- Создание базы данных
CREATE DATABASE budget_db;


-- Таблица организаций
CREATE TABLE Organization (
    org_id SERIAL PRIMARY KEY,
    TOO VARCHAR(255) NOT NULL,
    Region VARCHAR(100) NOT NULL
);

-- Таблица категорий субсидий
CREATE TABLE Sub_Cate (
    Cat_id SERIAL PRIMARY KEY,
    Cat_name VARCHAR(255) NOT NULL
);

-- Таблица статусов
CREATE TABLE Status (
    Status_id SERIAL PRIMARY KEY,
    Status_name VARCHAR(100) NOT NULL
);

-- Таблица отказов
CREATE TABLE Refuse (
    Refuse_id SERIAL PRIMARY KEY,
    Refuse_dt DATE,
    Refuse_txt TEXT
);

-- Таблица отзывов
CREATE TABLE Recall (
    Recall_id SERIAL PRIMARY KEY,
    Recall_dt DATE,
    Recall_txt TEXT
);

-- Таблица документов (заявок)
CREATE TABLE Budget_Documents (
    docnum SERIAL PRIMARY KEY,
    Summa NUMERIC(18,2) NOT NULL,
    doc_at DATE NOT NULL,
    Recall_id INT REFERENCES Recall(Recall_id) ON DELETE SET NULL,
    Refuse_id INT REFERENCES Refuse(Refuse_id) ON DELETE SET NULL,
    org_id INT REFERENCES Organization(org_id) ON DELETE CASCADE,
    Cat_id INT REFERENCES Sub_Cate(Cat_id) ON DELETE SET NULL,
    Status_ID INT REFERENCES Status(Status_id) ON DELETE SET NULL
);

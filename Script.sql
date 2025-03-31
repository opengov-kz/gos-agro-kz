CREATE TABLE "regions" (
    "id"   SERIAL,
    "name" VARCHAR(255),
    PRIMARY KEY ("id")
);

CREATE TABLE "statuses" (
    "id"   SERIAL,
    "name" VARCHAR(255),
    PRIMARY KEY ("id")
);

CREATE TABLE "categories" (
    "id"   SERIAL,
    "name" VARCHAR(255),
    PRIMARY KEY ("id")
);

CREATE TABLE "organizations" (
    "id"   SERIAL,
    "name" TEXT,
    PRIMARY KEY ("id")
);


CREATE TABLE "documents" (
    "id"            SERIAL,
    "doc_num"       VARCHAR(50) NOT NULL,
    "summa_subsidy" NUMERIC(15,2),
    "doc_at"        TIMESTAMP,

    "org_id"        INT,
    "category_id"   INT,
    "status_id"     INT,
    "region_id"     INT,

    PRIMARY KEY ("id"),

    CONSTRAINT "FK_documents_org_id"
        FOREIGN KEY ("org_id") REFERENCES "organizations"("id"),

    CONSTRAINT "FK_documents_region_id"
        FOREIGN KEY ("region_id") REFERENCES "regions"("id"),

    CONSTRAINT "FK_documents_status_id"
        FOREIGN KEY ("status_id") REFERENCES "statuses"("id"),

    CONSTRAINT "FK_documents_category_id"
        FOREIGN KEY ("category_id") REFERENCES "categories"("id")
);


CREATE TABLE "waiting_lists" (
    "id" INT,
    PRIMARY KEY ("id"),
    CONSTRAINT "FK_waiting_lists_documents"
        FOREIGN KEY ("id") REFERENCES "documents"("id")
);


CREATE TABLE "budget_registry" (
    "id" INT,
    PRIMARY KEY ("id"),
    CONSTRAINT "FK_budget_registry_documents"
        FOREIGN KEY ("id") REFERENCES "documents"("id")
);


CREATE TABLE "refuses" (
    "id"                 SERIAL,
    "refuse_txt"         TEXT,
    "refuse_at"          TIMESTAMP,
    "budget_registry_id" INT,
    PRIMARY KEY ("id"),
    CONSTRAINT "FK_refuses_budget_registry"
        FOREIGN KEY ("budget_registry_id")
            REFERENCES "budget_registry"("id")
);

CREATE TABLE "recalls" (
    "id"                 SERIAL,
    "recall_txt"         TEXT,
    "recall_at"          TIMESTAMP,
    "budget_registry_id" INT,
    PRIMARY KEY ("id"),
    CONSTRAINT "FK_recalls_budget_registry"
        FOREIGN KEY ("budget_registry_id")
            REFERENCES "budget_registry"("id")
);


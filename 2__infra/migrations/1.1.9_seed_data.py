import snowflake.connector
import pandas as pd
from pathlib import Path

USER = "MCAVAGNA"
PASSWORD = "Larraya2512!.."
ACCOUNT = "PFCBSOA-DQ82916"

WAREHOUSE = "WH_ADMIN_DES"
DATABASE = "DB_SOURCE_DES"
SCHEMA = "STAGING"


BASE_DIR = Path(__file__).resolve().parent
while BASE_DIR.name != "AI Customer Experience Engine":
    BASE_DIR = BASE_DIR.parent

DATA_DIR = BASE_DIR / "9_data" / "seeds"

PRODUCTS_FILE = DATA_DIR / "PRODUCTS.csv"
REVIEWS_FILE = DATA_DIR / "REVIEWS.csv"


def log(msg):
    print(f"[SEED] {msg}")


def connect():
    return snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )


def run():
    log("START")

    conn = connect()
    cs = conn.cursor()

    try:
        log("READ CSV")
        products = pd.read_csv(PRODUCTS_FILE)
        reviews = pd.read_csv(REVIEWS_FILE)

        log(f"PRODUCTS: {len(products)}")
        log(f"REVIEWS: {len(reviews)}")

        products = products.where(pd.notnull(products), None)
        reviews = reviews.where(pd.notnull(reviews), None)

        log("TRUNCATE TABLES")
        cs.execute("TRUNCATE TABLE TB_PRODUCTS_SRC")
        cs.execute("TRUNCATE TABLE TB_REVIEWS_SRC")

        log("INSERT PRODUCTS")
        cs.executemany(
            """
            INSERT INTO TB_PRODUCTS_SRC
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            products.values.tolist()
        )

        log("INSERT REVIEWS")
        cs.executemany(
            """
            INSERT INTO TB_REVIEWS_SRC
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            reviews.values.tolist()
        )

        log("DONE")

    finally:
        cs.close()
        conn.close()


if __name__ == "__main__":
    run()
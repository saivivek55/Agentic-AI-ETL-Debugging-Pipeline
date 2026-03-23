import csv
import io
import os
from decimal import Decimal, InvalidOperation

import boto3
import psycopg2
from botocore.config import Config
from psycopg2.extras import execute_batch

S3_BUCKET = os.environ.get("S3_BUCKET", "xxxxx")
S3_KEY = os.environ.get("S3_KEY", "input/Electric_Vehicle_Population_Data.csv")

DB_HOST = "xxxxx"
DB_PORT = 5432
DB_NAME = "ev_etl_db"
DB_USER = "xxxxx"
DB_PASSWORD = "xxxxx"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ev_population (
    vin_prefix VARCHAR(10),
    county VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(10),
    postal_code VARCHAR(20),
    model_year INT,
    make VARCHAR(100),
    model VARCHAR(100),
    ev_type VARCHAR(100),
    cafv_eligibility VARCHAR(200),
    electric_range INT,
    base_msrp NUMERIC,
    legislative_district INT,
    dol_vehicle_id BIGINT PRIMARY KEY,
    vehicle_location TEXT,
    electric_utility TEXT,
    census_tract BIGINT
);
"""

INSERT_SQL = """
INSERT INTO public.ev_population (
    vin_prefix, county, city, state, postal_code, model_year, make, model, ev_type,
    cafv_eligibility, electric_range, base_msrp, legislative_district, dol_vehicle_id,
    vehicle_location, electric_utility, census_tract
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (dol_vehicle_id) DO NOTHING;
"""


def clean(value):
    if value is None:
        return None
    value = str(value).strip()
    return value if value else None


def to_int(value):
    value = clean(value)
    if value is None:
        return None
    return int(float(value))


def to_decimal(value):
    value = clean(value)
    if value is None:
        return None
    value = value.replace(",", "")
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def lambda_handler(event, context):
    print("ETL_START")
    print(f"Reading file: {S3_BUCKET}/{S3_KEY}")

    conn = None
    rows_prepared = 0
    skipped_rows = 0

    try:
        s3 = boto3.client(
            "s3",
            region_name="us-east-2",
            config=Config(connect_timeout=3, read_timeout=60, retries={"max_attempts": 2}),
        )
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)

        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            sslmode="require",
            connect_timeout=5,
        )
        conn.autocommit = False

        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)

            stream = io.TextIOWrapper(obj["Body"], encoding="utf-8", errors="replace")
            reader = csv.DictReader(stream)

            batch = []
            batch_size = 1000

            for row in reader:
                normalized = {key: clean(value) for key, value in row.items()}

                if not any(normalized.values()):
                    skipped_rows += 1
                    continue

                dol_vehicle_id = to_int(normalized.get("DOL Vehicle ID"))
                if dol_vehicle_id is None:
                    skipped_rows += 1
                    continue

                rows_prepared += 1
                batch.append(
                    (
                        normalized.get("VIN (1-10)"),
                        normalized.get("County"),
                        normalized.get("City"),
                        normalized.get("State"),
                        normalized.get("Postal Code"),
                        to_int(normalized.get("Model Year")),
                        normalized.get("Make"),
                        normalized.get("Model"),
                        normalized.get("Electric Vehicle Type"),
                        normalized.get("Clean Alternative Fuel Vehicle (CAFV) Eligibility"),
                        to_int(normalized.get("Electric Range")),
                        to_decimal(normalized.get("Base MSRP")),
                        to_int(normalized.get("Legislative District")),
                        dol_vehicle_id,
                        normalized.get("Vehicle Location"),
                        normalized.get("Electric Utility"),
                        to_int(normalized.get("2020 Census Tract")),
                    )
                )

                if len(batch) >= batch_size:
                    execute_batch(cur, INSERT_SQL, batch, page_size=batch_size)
                    conn.commit()
                    batch.clear()

            if batch:
                execute_batch(cur, INSERT_SQL, batch, page_size=len(batch))
                conn.commit()

            cur.execute("SELECT COUNT(*) FROM public.ev_population;")
            table_total_rows = cur.fetchone()[0]

        print(f"Rows prepared: {rows_prepared}")
        print("ETL_SUCCESS")

        return {
            "statusCode": 200,
            "bucket": S3_BUCKET,
            "key": S3_KEY,
            "rows_prepared": rows_prepared,
            "skipped_rows": skipped_rows,
            "table_total_rows": table_total_rows,
        }
    except Exception as exc:
        if conn:
            conn.rollback()
        print("ETL_ERROR:", str(exc))
        raise
    finally:
        if conn:
            conn.close()

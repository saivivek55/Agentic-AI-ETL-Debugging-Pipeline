import os
import urllib.request

import psycopg2

DB_HOST = "xxxxx"
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = "xxxxx"
DB_PASSWORD = "xxxxx"

CERT_PATH = os.path.join(os.path.dirname(__file__), "global-bundle.pem")
CERT_URL = "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"


def ensure_cert():
    if not os.path.exists(CERT_PATH):
        urllib.request.urlretrieve(CERT_URL, CERT_PATH)


def main():
    ensure_cert()

    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode="verify-full",
            sslrootcert=CERT_PATH,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            print(cur.fetchone()[0])
    except Exception as exc:
        print(f"Database error: {exc}")
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()

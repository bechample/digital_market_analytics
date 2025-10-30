import requests
import argparse
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import time
import os
from datetime import date



# request urls like https://gumroad.com/products/search?&from=6
MAX_ITEMS = 631127
url = "https://gumroad.com/products/search?"
# increment by 5 each round
increment = 9

# parse command line arg for number of threads
parser = argparse.ArgumentParser(description="Gumroad products crawler")
parser.add_argument(
    "--no_of_threads",
    type=int,
    default=1,
    help="Number of threads to use for fetching pages (default: 1)",
)
args = parser.parse_args()
no_of_threads = max(1, args.no_of_threads)

def fetch_and_store(offset: int):
    """Fetch a page and store products into SQLite within the worker thread."""
    print(f"Fetching page for offset {offset}")
    response_json = requests.get(url + f"&from={offset}").json()

    conn = sqlite3.connect("data/products.db", timeout=30)
    try:
        cursor = conn.cursor()
        # set pragmas for concurrency similar to codecanyon project
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=5000")
        # ensure table exists (idempotent)
        cursor.execute("CREATE TABLE IF NOT EXISTS products (id TEXT PRIMARY KEY, name TEXT, url TEXT, permalink TEXT, seller_id TEXT, seller_name TEXT, seller_avatar_url TEXT, seller_profile_url TEXT, ratings_count INTEGER, ratings_average REAL, thumbnail_url TEXT, native_type TEXT, quantity_remaining INTEGER, is_sales_limited BOOLEAN, price_cents INTEGER, currency_code TEXT, is_pay_what_you_want BOOLEAN, duration_in_months INTEGER, recurrence TEXT, description TEXT, snapshot_date TEXT)")
        # for existing DBs, attempt to add snapshot_date column if missing
        try:
            cursor.execute("ALTER TABLE products ADD COLUMN snapshot_date TEXT")
        except sqlite3.OperationalError:
            pass
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS visited_urls (url TEXT NOT NULL, visited_date TEXT NOT NULL, PRIMARY KEY (url, visited_date))"
        )

        # simple retry loop to handle occasional SQLITE_BUSY during concurrent writes
        today_str = date.today().isoformat()

        def try_insert(product):
            attempts = 0
            while True:
                try:
                    # skip if this URL was already visited today
                    already_today = cursor.execute(
                        "SELECT 1 FROM visited_urls WHERE url = ? AND visited_date = ?",
                        (product["url"], today_str),
                    ).fetchone()
                    if already_today:
                        return

                    # upsert into products (still useful for historical storage)
                    cursor.execute(
                        "INSERT OR IGNORE INTO products (id, name, url, permalink, seller_id, seller_name, seller_avatar_url, seller_profile_url, ratings_count, ratings_average, thumbnail_url, native_type, quantity_remaining, is_sales_limited, price_cents, currency_code, is_pay_what_you_want, url, duration_in_months, recurrence, description, snapshot_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            product["id"],
                            product["name"],
                            product["url"],
                            product["permalink"],
                            product["seller"]["id"],
                            product["seller"]["name"],
                            product["seller"]["avatar_url"],
                            product["seller"]["profile_url"],
                            product["ratings"]["count"],
                            product["ratings"]["average"],
                            product["thumbnail_url"],
                            product["native_type"],
                            product["quantity_remaining"],
                            product["is_sales_limited"],
                            product["price_cents"],
                            product["currency_code"],
                            product["is_pay_what_you_want"],
                            product["url"],
                            product["duration_in_months"],
                            product["recurrence"],
                            product["description"],
                            today_str,
                        ),
                    )

                    # record that we visited this URL today
                    cursor.execute(
                        "INSERT OR IGNORE INTO visited_urls (url, visited_date) VALUES (?, ?)",
                        (product["url"], today_str),
                    )
                    break
                except sqlite3.OperationalError as e:
                    # backoff on database is locked
                    if ("locked" in str(e).lower() or "busy" in str(e).lower()) and attempts < 5:
                        attempts += 1
                        time.sleep(0.1 * attempts)
                        continue
                    raise

        # begin a short transaction and commit once
        begin_attempts = 0
        while True:
            try:
                cursor.execute("BEGIN IMMEDIATE")
                break
            except sqlite3.OperationalError as e:
                if ("locked" in str(e).lower() or "busy" in str(e).lower()) and begin_attempts < 5:
                    begin_attempts += 1
                    time.sleep(0.1 * begin_attempts)
                    continue
                raise

        try:
            for product in response_json.get("products", []):
                try_insert(product)
            commit_attempts = 0
            while True:
                try:
                    conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    if ("locked" in str(e).lower() or "busy" in str(e).lower()) and commit_attempts < 5:
                        commit_attempts += 1
                        time.sleep(0.1 * commit_attempts)
                        continue
                    raise
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()

offsets = list(range(increment, MAX_ITEMS + 1, increment))

os.makedirs("data", exist_ok=True)

if no_of_threads == 1:
    for off in offsets:
        fetch_and_store(off)
else:
    with ThreadPoolExecutor(max_workers=no_of_threads) as executor:
        list(executor.map(fetch_and_store, offsets))

    
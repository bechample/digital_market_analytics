import requests
import argparse
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import time
import os
from datetime import date
import json



# request urls like https://gumroad.com/products/search?&from=6
MAX_ITEMS = 10000 # more gumroad search cannot 
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

def fetch_and_store(query: str, offset: int):
    """Fetch a page and store products into SQLite within the worker thread."""
    conn = sqlite3.connect("data/products.db", timeout=30)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS visited_urls (url TEXT NOT NULL, visited_date TEXT NOT NULL, PRIMARY KEY (url, visited_date))"
        )
        search_url = url + f"&query={query}&from={offset}"
        
        # simple retry loop to handle occasional SQLITE_BUSY during concurrent writes
        today_str = date.today().isoformat()

        # skip entire page if this search URL was already visited today
        page_visited = cursor.execute(
            "SELECT 1 FROM visited_urls WHERE url = ? AND visited_date = ?",
            (search_url, today_str),
        ).fetchone()
        if page_visited:
            return

        print(f"Fetching query='{query}' offset={offset}")
        
        response_json = requests.get(url + f"&query={query}&from={offset}").json()


        
        # set pragmas for concurrency similar to codecanyon project (with retries)
        pragma_attempts = 0
        while True:
            try:
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.execute("PRAGMA busy_timeout=5000")
                break
            except sqlite3.OperationalError as e:
                if ("locked" in str(e).lower() or "busy" in str(e).lower()) and pragma_attempts < 10:
                    pragma_attempts += 1
                    time.sleep(0.05 * pragma_attempts)
                    continue
                # best-effort: do not crash if PRAGMA cannot be set; proceed
                try:
                    cursor.execute("PRAGMA busy_timeout=5000")
                except Exception:
                    pass
                break
        # ensure table exists (idempotent)
        cursor.execute("CREATE TABLE IF NOT EXISTS products (id TEXT PRIMARY KEY, name TEXT, url TEXT, permalink TEXT, seller_id TEXT, seller_name TEXT, seller_avatar_url TEXT, seller_profile_url TEXT, ratings_count INTEGER, ratings_average REAL, thumbnail_url TEXT, native_type TEXT, quantity_remaining INTEGER, is_sales_limited BOOLEAN, price_cents INTEGER, currency_code TEXT, is_pay_what_you_want BOOLEAN, duration_in_months INTEGER, recurrence TEXT, description TEXT, snapshot_date TEXT)")
        # for existing DBs, attempt to add snapshot_date column if missing
        try:
            cursor.execute("ALTER TABLE products ADD COLUMN snapshot_date TEXT")
        except sqlite3.OperationalError:
            pass


        def try_insert(product):
            attempts = 0
            while True:
                try:
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
            # mark the page URL visited for today
            cursor.execute(
                "INSERT OR IGNORE INTO visited_urls (url, visited_date) VALUES (?, ?)",
                (search_url, today_str),
            )
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
# build queries from 'aa' to 'zz'
queries = [chr(a) + chr(b) for a in range(ord('a'), ord('z') + 1) for b in range(ord('a'), ord('z') + 1)]

os.makedirs("data", exist_ok=True)

def fetch_total_for_query(query: str) -> int:
    try:
        data = requests.get(url + f"&query={query}&from={increment}").json()
        total = int(data.get("total", 0))
        if total > 10000:
            # record oversized result set into a jsonl file
            os.makedirs("data", exist_ok=True)
            entry = {"query": query, "total": total, "date": date.today().isoformat()}
            with open("data/too_much_results.json", "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        return total
    except Exception:
        return 0

if no_of_threads == 1:
    for q in queries:
        total = fetch_total_for_query(q)
        if total <= 0:
            continue
        per_query_offsets = list(range(increment, min(MAX_ITEMS, total) + 1, increment))
        for off in per_query_offsets:
            fetch_and_store(q, off)
else:
    for q in queries:
        total = fetch_total_for_query(q)
        if total <= 0:
            continue
        per_query_offsets = list(range(increment, min(MAX_ITEMS, total) + 1, increment))
        with ThreadPoolExecutor(max_workers=no_of_threads) as executor:
            list(executor.map(lambda off: fetch_and_store(q, off), per_query_offsets))

    
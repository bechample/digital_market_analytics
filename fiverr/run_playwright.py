#!/usr/bin/env python3
"""
Collect CodeCanyon product links using Playwright for JavaScript rendering.

Features
- Uses Playwright to handle JavaScript-rendered pages
- Paginates through listing pages automatically
- Optional "deep" mode to follow category/listing pages
- Uses a polite delay between requests
- Retries with backoff on transient errors
- Outputs CSV with one URL per line
- Logs 403 errors with curl commands

Usage
  python run_playwright.py \
      --start-url https://codecanyon.net/top-sellers \
      --output products.csv \
      --max-pages 200 \
      --delay 1.5 
"""

import argparse
import csv
import json
import re
import sys
import random
import sqlite3
import asyncio
from datetime import datetime
from typing import Optional, Set
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode

from playwright.async_api import async_playwright, Page, Response
from fake_useragent import UserAgent


CODECANYON_HOST = "fiverr.com"
BASE = f"https://{CODECANYON_HOST}"
ITEM_LINK_RE = re.compile(r"^/item/[^/]+/\d+/?$")
LISTING_ALLOW_RE = re.compile(
    r"^/(?:top-sellers|category/[^/].*|popular|search|collections/[^/].*|new)$"
)


def normalize_url(u: str) -> str:
    """Normalize URLs by stripping fragments and normalizing scheme/host."""
    p = urlparse(u)
    scheme = "https"
    netloc = CODECANYON_HOST
    path = p.path
    query = p.query
    # Remove Google Analytics/utm params
    if query:
        q = parse_qs(query, keep_blank_values=True)
        q = {k: v for k, v in q.items() if not k.lower().startswith("utm_")}
        query = urlencode(q, doseq=True)
    return urlunparse((scheme, netloc, path, "", "", "")) if not query else urlunparse((scheme, netloc, path, "", query, ""))


class CrawlDatabase:
    """Thread-safe SQLite database for crawl state."""
    
    def __init__(self, db_path: str = "crawl_state.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging for concurrency
        self._init_tables()
    
    def _init_tables(self):
        """Initialize database tables."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS visited_listings (
                url TEXT PRIMARY KEY,
                visited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS to_visit_queue (
                url TEXT PRIMARY KEY,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS found_items (
                url TEXT PRIMARY KEY,
                found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def add_visited(self, url: str):
        """Add URL to visited list."""
        self.conn.execute("INSERT OR IGNORE INTO visited_listings (url) VALUES (?)", (url,))
    
    def is_visited(self, url: str) -> bool:
        """Check if URL has been visited."""
        cursor = self.conn.execute("SELECT 1 FROM visited_listings WHERE url = ? LIMIT 1", (url,))
        return cursor.fetchone() is not None
    
    def get_all_visited(self) -> Set[str]:
        """Get all visited URLs."""
        cursor = self.conn.execute("SELECT url FROM visited_listings")
        return {row[0] for row in cursor.fetchall()}
    
    def add_to_queue(self, url: str):
        """Add URL to visit queue."""
        self.conn.execute("INSERT OR IGNORE INTO to_visit_queue (url) VALUES (?)", (url,))
    
    def remove_from_queue(self, url: str):
        """Remove URL from queue."""
        self.conn.execute("DELETE FROM to_visit_queue WHERE url = ?", (url,))
    
    def get_queue(self) -> list:
        """Get all URLs in queue."""
        cursor = self.conn.execute("SELECT url FROM to_visit_queue ORDER BY added_at")
        return [row[0] for row in cursor.fetchall()]
    
    def add_item(self, url: str):
        """Add found item."""
        self.conn.execute("INSERT OR IGNORE INTO found_items (url) VALUES (?)", (url,))
    
    def is_item_found(self, url: str) -> bool:
        """Check if item has been found."""
        cursor = self.conn.execute("SELECT 1 FROM found_items WHERE url = ? LIMIT 1", (url,))
        return cursor.fetchone() is not None
    
    def get_all_items(self) -> Set[str]:
        """Get all found items."""
        cursor = self.conn.execute("SELECT url FROM found_items")
        return {row[0] for row in cursor.fetchall()}
    
    def export_to_csv(self, output: str):
        """Export found items to CSV."""
        cursor = self.conn.execute("SELECT url FROM found_items ORDER BY url")
        items = [row[0] for row in cursor.fetchall()]
        
        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["url"])
            for item in items:
                writer.writerow([item])
    
    def close(self):
        """Close database connection."""
        self.conn.close()


def load_existing_items(output: str) -> Set[str]:
    """Load existing URLs from CSV file to avoid duplicates."""
    existing = set()
    try:
        with open(output, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            for row in reader:
                if row:
                    existing.add(row[0])
    except FileNotFoundError:
        pass
    return existing


def write_items_to_csv(items: Set[str], output: str):
    """Write items to CSV file in append mode."""
    with open(output, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for u in sorted(items):
            writer.writerow([u])


def log_403_error(request_info: dict, response_info: dict):
    """Log 403 errors to error.json."""
    # Ensure data directory exists
    import os
    os.makedirs("data", exist_ok=True)
    # Generate curl command
    def generate_curl_command(req_info):
        curl_parts = ["curl"]
        
        # Method (default is GET)
        if req_info.get('method') and req_info['method'].upper() != 'GET':
            curl_parts.append(f"-X {req_info['method']}")
        
        # URL
        curl_parts.append(f"'{req_info['url']}'")
        
        # Headers
        headers = req_info.get('headers', {})
        for key, value in headers.items():
            if isinstance(value, list):
                for v in value:
                    curl_parts.append(f"-H '{key}: {v}'")
            else:
                curl_parts.append(f"-H '{key}: {value}'")
        
        # Cookies
        cookies = req_info.get('cookies', {})
        if cookies:
            cookie_string = '; '.join([f"{k}={v}" for k, v in cookies.items()])
            curl_parts.append(f"-H 'Cookie: {cookie_string}'")
        
        # Verbose and include headers in output
        curl_parts.extend(["-v", "-i"])
        
        return ' '.join(curl_parts)
    
    curl_command = generate_curl_command(request_info)
    
    error_log = {
        "error": "HTTP 403 Forbidden",
        "timestamp": datetime.now().isoformat(),
        "curl_command": curl_command,
        "request": request_info,
        "response": response_info
    }
    
    # Append to error.json
    error_file = 'data/error.json'
    try:
        with open(error_file, 'r') as f:
            error_logs = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        error_logs = []
    
    error_logs.append(error_log)
    
    with open(error_file, 'w') as f:
        json.dump(error_logs, f, indent=2, ensure_ascii=False)
    
    print(f"[error] HTTP 403 Forbidden for {request_info['url']}. Logged to error.json")


async def extract_links(page: Page, base_url: str):
    """Extract item links and listing links from the page."""
    item_links = set()
    listing_links = set()
    
    # Wait for content to load
    await page.wait_for_load_state('networkidle')
    
    # Find all links
    links = await page.query_selector_all('a[href]')
    
    for link in links:
        try:
            href = await link.get_attribute('href')
            if not href or href.startswith('#'):
                continue
            
            abs_url = urljoin(base_url, href)
            p = urlparse(abs_url)
            
            # Only process CodeCanyon URLs
            if p.netloc not in {CODECANYON_HOST, f"www.{CODECANYON_HOST}"}:
                continue
            
            norm = normalize_url(abs_url)
            path = urlparse(norm).path
            
            if ITEM_LINK_RE.match(path):
                item_links.add(norm)
            elif LISTING_ALLOW_RE.match(path):
                listing_links.add(norm)
        except Exception:
            continue
    
    return item_links, listing_links


async def extract_pagination_links(page: Page, base_url: str, max_pages: int) -> Set[str]:
    """Extract pagination links from the page."""
    pagination_urls = set()
    
    # Get current page number from URL
    current_page = 1
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query)
    if "page" in query:
        try:
            current_page = int(query["page"][0])
        except (ValueError, KeyError):
            pass
    
    base_path = parsed.path
    
    # Find links with ?page= parameter
    links = await page.query_selector_all('a[href*="page="]')
    for link in links:
        try:
            href = await link.get_attribute('href')
            if not href or href.startswith('#'):
                continue
            
            abs_url = urljoin(base_url, href)
            norm = normalize_url(abs_url)
            parsed_norm = urlparse(norm)
            norm_query = parse_qs(parsed_norm.query)
            
            if "page" in norm_query and parsed_norm.path == base_path:
                try:
                    page_num = int(norm_query["page"][0])
                    if 1 <= page_num <= current_page + max_pages:
                        pagination_urls.add(norm)
                except (ValueError, KeyError):
                    pass
        except Exception:
            continue
    
    # Also look for "Next" links
    all_links = await page.query_selector_all('a[href]')
    for link in all_links:
        try:
            href = await link.get_attribute('href')
            if not href:
                continue
            
            link_text = await link.text_content()
            if link_text:
                link_text = link_text.strip().lower()
                if link_text in ["next", "»", "→", "›"] or ("next" in link_text or "page" in link_text.lower()):
                    abs_url = urljoin(base_url, href)
                    p = urlparse(abs_url)
                    
                    if "page" in parse_qs(p.query) and p.netloc in {CODECANYON_HOST, f"www.{CODECANYON_HOST}"}:
                        norm = normalize_url(abs_url)
                        if urlparse(norm).path == base_path:
                            pagination_urls.add(norm)
        except Exception:
            continue
    
    return pagination_urls


async def crawl_page(
    page: Page,
    url: str,
    db: CrawlDatabase,
    delay: float,
    max_pages: int
):
    """Crawl a single page."""
    current_url = normalize_url(url)
    
    # Skip if already visited
    if db.is_visited(current_url):
        return [], []
    
    db.add_visited(current_url)
    print(f"[info] Visiting {current_url}")
    
    try:
        # Navigate to the page
        response = await page.goto(current_url, wait_until='networkidle', timeout=30000)
        
        if not response:
            print(f"[warn] No response for {current_url}")
            return [], []
        
        # Check for 403 errors
        if response.status == 403:
            # Log the error
            request = {
                "url": current_url,
                "method": "GET",
                "headers": {},
                "cookies": {}
            }
            
            # Try to get cookies
            try:
                cookies = await page.context.cookies()
                request["cookies"] = {c['name']: c['value'] for c in cookies}
            except:
                pass
            
            # Get headers from the request
            try:
                headers = await page.evaluate("""() => {
                    return {
                        'User-Agent': navigator.userAgent,
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9'
                    }
                }""")
                request["headers"] = headers
            except:
                pass
            
            response_info = {
                "status": 403,
                "url": current_url,
                "headers": {}
            }
            
            log_403_error(request, response_info)
            return [], []
        
        # Extract links
        item_links, listing_links = await extract_links(page, current_url)
        
        # Add new items to database
        new_items = []
        for item in item_links:
            if not db.is_item_found(item):
                db.add_item(item)
                new_items.append(item)
        
        if new_items:
            print(f"[info] Found {len(new_items)} new items (total {len(db.get_all_items())}) from {current_url}")
        
        print(f"[debug] Found {len(item_links)} item links and {len(listing_links)} listing links")
        
        # Extract pagination links
        pagination_urls = await extract_pagination_links(page, current_url, max_pages)
        print(f"[debug] Extracted {len(pagination_urls)} pagination URLs")
        
        # Wait before next request
        await page.wait_for_timeout(int(delay * 1000))
        
        return pagination_urls, listing_links
        
    except Exception as e:
        print(f"[error] Error crawling {current_url}: {e}", file=sys.stderr)
        return [], []


async def worker(db: CrawlDatabase, playwright, delay: float, max_pages: int, worker_id: int):
    """Worker function to crawl pages in parallel."""
    # Get random user agent for this worker
    try:
        ua = UserAgent()
        user_agent = ua.random
    except:
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(
        user_agent=user_agent,
        viewport={'width': 1920, 'height': 1080}
    )
    page = await context.new_page()
    
    try:
        while True:
            # Get queue from database
            to_visit = db.get_queue()
            
            if not to_visit:
                print(f"[worker-{worker_id}] Queue empty, waiting...")
                await asyncio.sleep(5)
                continue
            
            # Get random URL from queue
            url = random.choice(to_visit)
            
            # Check if already visited
            if db.is_visited(url):
                db.remove_from_queue(url)
                continue
            
            print(f"[worker-{worker_id}] Visiting {url}")
            
            # Crawl the page
            pagination_urls, listing_links = await crawl_page(page, url, db, delay, max_pages)
            
            # Remove current URL from queue
            db.remove_from_queue(url)
            
            # Add pagination URLs to queue
            for purl in pagination_urls:
                if not db.is_visited(purl):
                    db.add_to_queue(purl)
            
            # Also follow listing pages
            for ln in listing_links:
                if not db.is_visited(ln):
                    db.add_to_queue(ln)
            
            # Wait before next request
            await page.wait_for_timeout(int(delay * 1000))
            
    finally:
        await browser.close()


async def run_crawler(
    start_url: str,
    output: str,
    delay: float,
    max_pages: int,
    browser_name: str = 'chromium',
    num_threads: int = 1
):
    """Run the crawler."""
    # Create data directory if it doesn't exist
    import os
    os.makedirs("data", exist_ok=True)
    
    # Initialize database
    db = CrawlDatabase("data/crawl_state.db")
    
    # Load existing items from CSV into database
    existing_items = load_existing_items(output)
    for item in existing_items:
        db.add_item(item)
    
    print(f"[info] Starting with {len(db.get_all_items())} existing items")
    
    # Load queue from database
    to_visit = db.get_queue()
    
    # If queue is empty, add start URL
    if not to_visit:
        start_url_norm = normalize_url(start_url)
        db.add_to_queue(start_url_norm)
        to_visit = [start_url_norm]
    
    print(f"[info] Starting {num_threads} worker thread(s)")
    
    async with async_playwright() as p:
        # Create worker tasks
        tasks = [worker(db, p, delay, max_pages, i+1) for i in range(num_threads)]
        
        # Run all workers concurrently
        await asyncio.gather(*tasks)
        
        # Export results and close database
        db.export_to_csv(output)
        db.close()
    
    print(f"[done] Total: {len(db.get_all_items())} unique product URLs in {output}")


def main():
    parser = argparse.ArgumentParser(description="Collect CodeCanyon product links using Playwright.")
    parser.add_argument("--start-url", default=None, help="Listing page to start from. Defaults to top-sellers if not provided.")
    parser.add_argument("--output", default="products.csv", help="CSV output path.")
    parser.add_argument("--max-pages", type=int, default=200, help="Max pages to attempt per listing path.")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay (seconds) between requests.")
    parser.add_argument("--browser", choices=['chromium', 'firefox', 'webkit'], default='chromium', help="Browser to use.")
    parser.add_argument("--no-of-threads", type=int, default=1, help="Number of parallel worker threads.")
    args = parser.parse_args()
    
    # Determine start URL
    start_url = args.start_url or "https://www.fiverr.com/categories/trending"
    print(f"[info] Using start URL: {start_url}")
    
    # Basic host guardrail
    host = urlparse(start_url).netloc.lower()
    if CODECANYON_HOST not in host:
        print(f"Error: start-url must be on {CODECANYON_HOST}", file=sys.stderr)
        sys.exit(1)
    
    # Run the crawler
    asyncio.run(run_crawler(
        start_url=start_url,
        output=args.output,
        delay=args.delay,
        max_pages=args.max_pages,
        browser_name=args.browser,
        num_threads=args.no_of_threads
    ))


if __name__ == "__main__":
    main()


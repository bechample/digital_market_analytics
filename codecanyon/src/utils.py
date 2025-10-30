#!/usr/bin/env python3
"""
Shared utilities for CodeCanyon data collection.

This module contains common functionality used by both the product finder
and analytics collector scripts.
"""

import sqlite3
import asyncio
import traceback
import random
from typing import Optional, Set, Dict, Any
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from playwright.async_api import async_playwright, Page, Response
from fake_useragent import UserAgent


CODECANYON_HOST = "codecanyon.net"
BASE = f"https://{CODECANYON_HOST}"


class SlowDownException(Exception):
    """Exception raised when rate limiting is detected."""
    pass


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
    
    def __init__(self, db_path: str = "data/crawl_state.db"):
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
        
        # Analytics table for sales data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS analytics_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                sales_count INTEGER,
                rating REAL,
                comments_count INTEGER,
                last_update TEXT,
                price REAL,
                created_at TEXT,
                category_path TEXT,
                tags TEXT,
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                error_message TEXT,
                FOREIGN KEY (url) REFERENCES found_items (url)
            )
        """)
        # Ensure columns exist when upgrading from older schema
        self._ensure_analytics_columns()

    def _ensure_analytics_columns(self):
        """Add missing columns to analytics_data for backward compatibility."""
        cursor = self.conn.execute("PRAGMA table_info(analytics_data)")
        cols = {row[1] for row in cursor.fetchall()}
        alter_statements = []
        if 'rating' not in cols:
            alter_statements.append("ALTER TABLE analytics_data ADD COLUMN rating REAL")
        if 'comments_count' not in cols:
            alter_statements.append("ALTER TABLE analytics_data ADD COLUMN comments_count INTEGER")
        if 'last_update' not in cols:
            alter_statements.append("ALTER TABLE analytics_data ADD COLUMN last_update TEXT")
        if 'price' not in cols:
            alter_statements.append("ALTER TABLE analytics_data ADD COLUMN price REAL")
        if 'created_at' not in cols:
            alter_statements.append("ALTER TABLE analytics_data ADD COLUMN created_at TEXT")
        if 'category_path' not in cols:
            alter_statements.append("ALTER TABLE analytics_data ADD COLUMN category_path TEXT")
        if 'tags' not in cols:
            alter_statements.append("ALTER TABLE analytics_data ADD COLUMN tags TEXT")
        for stmt in alter_statements:
            try:
                self.conn.execute(stmt)
            except Exception:
                pass
    
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
    
    def get_urls_to_process(self) -> list:
        """Get all URLs from found_items that haven't been processed for analytics yet."""
        cursor = self.conn.execute("""
            SELECT fi.url 
            FROM found_items fi 
            LEFT JOIN analytics_data ad ON fi.url = ad.url 
            WHERE ad.url IS NULL
            ORDER BY fi.found_at
        """)
        return [row[0] for row in cursor.fetchall()]
    
    def save_analytics_data(self, data: Dict[str, Any]):
        """Save analytics data to the database."""
        self._ensure_analytics_columns()
        self.conn.execute(
            """
            INSERT INTO analytics_data (
                url, sales_count, rating, comments_count, last_update, price,
                created_at, category_path, tags, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data.get('url'),
                data.get('sales_count'),
                data.get('rating'),
                data.get('comments_count'),
                data.get('last_update'),
                data.get('price'),
                data.get('created_at'),
                data.get('category_path'),
                data.get('tags'),
                data.get('error_message'),
            ),
        )
        self.conn.commit()
    
    def get_analytics_summary(self) -> Dict[str, Any]:
        """Get a summary of collected analytics data."""
        cursor = self.conn.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(sales_count) as successful_extractions,
                COUNT(error_message) as failed_extractions,
                AVG(sales_count) as avg_sales,
                MIN(sales_count) as min_sales,
                MAX(sales_count) as max_sales
            FROM analytics_data
        """)
        
        result = cursor.fetchone()
        if result:
            return {
                'total_records': result[0],
                'successful_extractions': result[1],
                'failed_extractions': result[2],
                'avg_sales': result[3],
                'min_sales': result[4],
                'max_sales': result[5]
            }
        return {}
    
    def export_to_csv(self, output: str):
        """Export found items to CSV."""
        cursor = self.conn.execute("SELECT url FROM found_items ORDER BY url")
        items = [row[0] for row in cursor.fetchall()]
        
        with open(output, "w", newline="", encoding="utf-8") as f:
            import csv
            writer = csv.writer(f)
            writer.writerow(["url"])
            for item in items:
                writer.writerow([item])
    
    def close(self):
        """Close database connection."""
        self.conn.close()


class PlaywrightManager:
    """Manages Playwright browser instances and provides common functionality."""
    
    def __init__(self, headless: bool = True, num_workers: int = 1):
        self.headless = headless
        self.num_workers = num_workers
        self.playwright = None
        self.browsers = []
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.playwright = await async_playwright().start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        for browser in self.browsers:
            await browser.close()
        if self.playwright:
            await self.playwright.stop()
    
    def get_user_agent(self) -> str:
        """Get a random user agent."""
        try:
            ua = UserAgent()
            return ua.random
        except:
            return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    
    async def create_browser(self, worker_id: int = 1) -> tuple:
        """Create a new browser instance."""
        user_agent = self.get_user_agent()
        
        browser = await self.playwright.chromium.launch(headless=self.headless)
        context = await browser.new_context(
            user_agent=user_agent,
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()
        
        self.browsers.append(browser)
        return browser, context, page
    
    async def navigate_to_page(self, page: Page, url: str, timeout: int = 30000, log_403: bool = True) -> Optional[Response]:
        """Navigate to a page with error handling."""
        from src.perform_request import perform_request
        return await perform_request(page, url, timeout, log_403)
    
    async def extract_sales_count(self, page: Page) -> Optional[int]:
        """Extract sales count from the current page using XPath."""
        try:
            # Wait for content to load
            await page.wait_for_load_state('networkidle')
            
            # Try to find the sales count element
            try:
                # Primary XPath for the sales count
                sales_element = await page.query_selector("div.item-header__sales-count strong")
                if sales_element:
                    sales_text = await sales_element.text_content()
                    if sales_text:
                        import re
                        # Extract numeric value from text like "14,100 sales" or "14,100"
                        sales_match = re.search(r'([\d,]+)', sales_text.strip())
                        if sales_match:
                            # Remove commas and convert to integer
                            sales_count = int(sales_match.group(1).replace(',', ''))
                            return sales_count
            except Exception as e:
                print(f"Error extracting sales count with primary method: {e}")
            
            # Try alternative selectors
            alternative_selectors = [
                "div[class*='sales-count'] strong",
                "div[class*='item-header'] strong:has-text('sales')",
                "strong:has-text('sales')"
            ]
            
            for selector in alternative_selectors:
                try:
                    element = await page.query_selector(selector)
                    if element:
                        sales_text = await element.text_content()
                        if sales_text:
                            import re
                            sales_match = re.search(r'([\d,]+)', sales_text.strip())
                            if sales_match:
                                sales_count = int(sales_match.group(1).replace(',', ''))
                                return sales_count
                except Exception:
                    continue
            
            return None
            
        except Exception as e:
            print(f"Error extracting sales count: {e}")
            return None

    async def extract_rating(self, page: Page) -> Optional[float]:
        """Extract rating value like 4.77 from rating block."""
        try:
            container = await page.query_selector("div.rating-detailed-small__stars")
            if not container:
                return None
            # Try numeric text next to stars
            text_content = (await container.text_content()) or ""
            import re
            m = re.search(r"(\d+\.\d+|\d+)", text_content)
            if m:
                return float(m.group(1))
            return None
        except Exception:
            return None

    async def extract_comments_count(self, page: Page) -> Optional[int]:
        """Extract number of comments from span.item-navigation-reviews-comments."""
        try:
            el = await page.query_selector("span.item-navigation-reviews-comments")
            if not el:
                return None
            txt = (await el.text_content()) or ""
            import re
            m = re.search(r"(\d+)", txt.replace(',', ''))
            return int(m.group(1)) if m else None
        except Exception:
            return None

    async def extract_last_update(self, page: Page) -> Optional[str]:
        """Extract last update timestamp or text from table row."""
        try:
            time_el = await page.query_selector("tr.js-condense-item-page-info-panel--last_update time.updated")
            if time_el:
                iso = await time_el.get_attribute("datetime")
                if iso:
                    return iso
                txt = await time_el.text_content()
                return txt.strip() if txt else None
            return None
        except Exception:
            return None

    async def extract_price(self, page: Page) -> Optional[float]:
        """Extract price from span.js-purchase-price like $7."""
        try:
            el = await page.query_selector("span.js-purchase-price")
            if not el:
                return None
            txt = (await el.text_content()) or ""
            import re
            m = re.search(r"([0-9]+(?:\.[0-9]{1,2})?)", txt.replace(',', ''))
            return float(m.group(1)) if m else None
        except Exception:
            return None

    async def extract_created_at(self, page: Page) -> Optional[str]:
        """Extract created time text from created-at row."""
        try:
            el = await page.query_selector("tr.js-condense-item-page-info-panel--created-at td.meta-attributes__attr-detail")
            if not el:
                return None
            txt = (await el.text_content()) or ""
            return txt.strip() or None
        except Exception:
            return None

    async def extract_category_path(self, page: Page) -> Optional[str]:
        """Extract breadcrumb categories as a single text path."""
        try:
            links = await page.query_selector_all("nav.breadcrumbs a.js-breadcrumb-category")
            texts = []
            for a in links:
                t = await a.text_content()
                if t:
                    texts.append(t.strip())
            if texts:
                return " / ".join(texts)
            return None
        except Exception:
            return None

    async def extract_tags(self, page: Page) -> Optional[str]:
        """Extract tags as a comma-separated string."""
        try:
            tags = await page.query_selector_all("td .meta-attributes__attr-tags a")
            tag_texts = []
            for el in tags:
                t = await el.text_content()
                if t:
                    tag_texts.append(t.strip())
            if tag_texts:
                return ", ".join(tag_texts)
            return None
        except Exception:
            return None
    
    async def process_url_for_analytics(self, page: Page, url: str) -> Dict[str, Any]:
        """Process a single URL for analytics data extraction."""
        result = {
            'url': url,
            'sales_count': None,
            'rating': None,
            'comments_count': None,
            'last_update': None,
            'price': None,
            'created_at': None,
            'category_path': None,
            'tags': None,
            'error_message': None,
            'success': False
        }
        
        try:
            print(f"Processing: {url}")
            
            # Navigate to the page
            response = await self.navigate_to_page(page, url)
            if not response:
                result['error_message'] = "Failed to navigate to page"
                return result
            
            # Extract fields
            page_content = await page.content()
            result['sales_count'] = await self.extract_sales_count(page)
            result['rating'] = await self.extract_rating(page)
            result['comments_count'] = await self.extract_comments_count(page)
            result['last_update'] = await self.extract_last_update(page)
            result['price'] = await self.extract_price(page)
            result['created_at'] = await self.extract_created_at(page)
            result['category_path'] = await self.extract_category_path(page)
            result['tags'] = await self.extract_tags(page)
            result['success'] = True
            
            if result['sales_count'] is not None:
                print(f"Extracted sales: {result['sales_count']:,}")
            if result['rating'] is not None:
                print(f"Extracted rating: {result['rating']}")
            
        except SlowDownException as e:
            result['error_message'] = f"Rate limited: {str(e)}"
            print(f"[WARN] {e}")
        except Exception as e:
            result['error_message'] = f"Unexpected error: {str(e)}"
            print(f"Error processing {url}: {e}")
            print(traceback.format_exc())
        
        return result

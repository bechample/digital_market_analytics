#!/usr/bin/env python3
"""
Collect analytics data from CodeCanyon product pages using Playwright.

This script reads URLs from the found_items table in the database,
visits each URL using Playwright, and extracts sales count data.
The extracted data is stored in the analytics table with timestamps.
"""

import asyncio
import time
from typing import Dict, Any
from src.utils import CrawlDatabase, PlaywrightManager, SlowDownException


class AnalyticsCollector:
    """Collects analytics data from CodeCanyon product pages using Playwright."""
    
    def __init__(self, db_path: str = "data/crawl_state.db"):
        self.db_path = db_path
        self.db = CrawlDatabase(db_path)
    
    async def collect_analytics_async(self, delay: float = 2.0, num_workers: int = 1):
        """Main async method to collect analytics for all URLs."""
        urls = self.db.get_urls_to_process()
        
        if not urls:
            print("No URLs to process.")
            return
        
        print(f"Found {len(urls)} URLs to process")
        
        async with PlaywrightManager(headless=True, num_workers=num_workers) as playwright_mgr:
            # Create browser instances for workers
            browsers = []
            for i in range(num_workers):
                browser, context, page = await playwright_mgr.create_browser(worker_id=i+1)
                browsers.append((browser, context, page))
            
            processed = 0
            successful = 0
            failed = 0
            
            # Process URLs in batches
            batch_size = num_workers
            for i in range(0, len(urls), batch_size):
                batch_urls = urls[i:i + batch_size]
                
                # Process batch concurrently
                tasks = []
                for j, url in enumerate(batch_urls):
                    worker_id = j % num_workers
                    browser, context, page = browsers[worker_id]
                    task = self._process_url_worker(page, url, i + j + 1, len(urls))
                    tasks.append(task)
                
                # Wait for batch to complete
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for result in batch_results:
                    if isinstance(result, Exception):
                        print(f"Worker error: {result}")
                        failed += 1
                    else:
                        processed += 1
                        if result['success']:
                            successful += 1
                        else:
                            failed += 1
                
                # Add delay between batches
                if i + batch_size < len(urls):
                    print(f"Waiting {delay} seconds before next batch...")
                    await asyncio.sleep(delay)
            
            print(f"\n=== Collection Complete ===")
            print(f"Total processed: {processed}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
    
    async def _process_url_worker(self, page, url: str, current: int, total: int) -> Dict[str, Any]:
        """Worker function to process a single URL."""
        print(f"\n[{current}/{total}] Processing: {url}")
        
        try:
            async with PlaywrightManager() as playwright_mgr:
                result = await playwright_mgr.process_url_for_analytics(page, url)
                
                # Save to database (extended fields)
                self.db.save_analytics_data(result)
                
                return result
                
        except SlowDownException as e:
            print(f"[WARN] Rate limited: {e}")
            await asyncio.sleep(30)  # Wait 30 seconds on rate limit
            return {
                'url': url,
                'sales_count': None,
                'error_message': f"Rate limited: {str(e)}",
                'success': False
            }
        except Exception as e:
            print(f"Error processing {url}: {e}")
            return {
                'url': url,
                'sales_count': None,
                'error_message': f"Unexpected error: {str(e)}",
                'success': False
            }
    
    def collect_analytics(self, delay: float = 2.0, num_workers: int = 1):
        """Synchronous wrapper for the async analytics collection."""
        asyncio.run(self.collect_analytics_async(delay, num_workers))
    
    def get_analytics_summary(self):
        """Get a summary of collected analytics data."""
        summary = self.db.get_analytics_summary()
        
        if summary:
            print(f"\n=== Analytics Summary ===")
            print(f"Total records: {summary['total_records']}")
            print(f"Successful extractions: {summary['successful_extractions']}")
            print(f"Failed extractions: {summary['failed_extractions']}")
            if summary['avg_sales']:
                print(f"Average sales: {summary['avg_sales']:,.0f}")
                print(f"Min sales: {summary['min_sales']:,}")
                print(f"Max sales: {summary['max_sales']:,}")
    
    def close(self):
        """Close database connection."""
        self.db.close()


def main():
    """Main function to run the analytics collector."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Collect analytics data from CodeCanyon product pages")
    parser.add_argument("--delay", type=float, default=2.0, 
                       help="Delay between requests in seconds (default: 2.0)")
    parser.add_argument("--workers", type=int, default=1,
                       help="Number of parallel workers (default: 1)")
    parser.add_argument("--threads", type=int, default=None,
                       help="Alias for --workers; number of parallel workers")
    parser.add_argument("--summary", action="store_true", 
                       help="Show analytics summary and exit")
    
    args = parser.parse_args()
    
    collector = AnalyticsCollector()
    
    try:
        if args.summary:
            collector.get_analytics_summary()
        else:
            effective_workers = args.threads if args.threads is not None else args.workers
            collector.collect_analytics(delay=args.delay, num_workers=effective_workers)
            collector.get_analytics_summary()
    finally:
        collector.close()


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Test script for the analytics collector.
This script tests the analytics collection with a small sample of URLs using Playwright.
"""

import asyncio
from collect_analytics import AnalyticsCollector


async def test_analytics_collection():
    """Test the analytics collection with a small sample."""
    
    # Create a test collector
    collector = AnalyticsCollector()
    
    try:
        # Get a small sample of URLs to test
        test_urls = collector.db.get_urls_to_process()[:3]  # Get first 3 URLs
        
        print(f"Testing with {len(test_urls)} URLs:")
        for i, url in enumerate(test_urls, 1):
            print(f"{i}. {url}")
        
        print("\n" + "="*50)
        print("Starting analytics collection test...")
        print("="*50)
        
        # Test with async collection
        await collector.collect_analytics_async(delay=1.0, num_workers=1)
        
        # Show summary
        collector.get_analytics_summary()
        
    finally:
        collector.close()


if __name__ == "__main__":
    asyncio.run(test_analytics_collection())

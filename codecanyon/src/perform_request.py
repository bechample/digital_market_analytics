#!/usr/bin/env python3
"""
Perform HTTP requests using Playwright with error handling and logging.

This module provides a shared method for navigating to pages and handling
common errors like rate limiting (429) and forbidden (403) responses.
"""

import json
import os
import traceback
from datetime import datetime
from typing import Optional
from playwright.async_api import Page, Response
from src.utils import SlowDownException, normalize_url


def log_403_error(request_info: dict, response_info: dict):
    """Log 403 errors to error.json."""
    # Ensure data directory exists
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


async def perform_request(
    page: Page,
    url: str,
    timeout: int = 30000,
    log_403: bool = True
) -> Optional[Response]:
    """
    Perform a request using Playwright with error handling.
    
    Args:
        page: Playwright Page object
        url: URL to navigate to
        timeout: Request timeout in milliseconds (default: 30000)
        log_403: Whether to log 403 errors to error.json (default: True)
    
    Returns:
        Response object if successful, None otherwise
    
    Raises:
        SlowDownException: If rate limiting (429) is detected
    """
    current_url = normalize_url(url)
    
    try:
        # Navigate to the page
        response = await page.goto(current_url, wait_until='networkidle', timeout=timeout)
        
        if not response:
            print(f"[warn] No response for {current_url}")
            return None
        
        # Check if response body contains "429 Too many requests"
        text = await response.text()
        if "429 Too many requests" in text:
            print(f"[warn] 429 Too many requests for {current_url}")
            raise SlowDownException(f"429 Too many requests for {current_url}")
        
        # Check for 403 errors
        if response.status == 403:
            if log_403:
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
                except Exception:
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
                except Exception:
                    pass
                
                response_info = {
                    "status": 403,
                    "url": current_url,
                    "headers": {}
                }
                
                log_403_error(request, response_info)
            else:
                print(f"[error] HTTP 403 Forbidden for {current_url}")
            
            return None
        
        return response
        
    except SlowDownException:
        # Re-raise rate limiting exceptions
        raise
    except Exception as e:
        print(f"[error] Error navigating to {url}: {e}")
        print(traceback.format_exc())
        return None

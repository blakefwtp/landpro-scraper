"""
Listing Notifier Scraper Service v5.0.0
Hybrid approach: Selenium for login + form interaction, then parallel JS fetch() for CSV export.
~30-50x faster than the old per-page Selenium CSV export approach.
"""

import os
import io
import csv
import time
import asyncio
import tempfile
import shutil
from typing import Optional
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode

from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException
import json

app = FastAPI(title="Listing Notifier Scraper", version="5.0.0")

API_SECRET = os.environ.get("API_SECRET", "")


# ─── Models ───────────────────────────────────────────

class ScrapeRequest(BaseModel):
    username: str
    password: str
    time_range: str = "7d"
    listing_status: str = "Active"
    max_pages: int = 50


class LoginTestRequest(BaseModel):
    username: str
    password: str


# ─── Auth ─────────────────────────────────────────────

def verify_auth(authorization: Optional[str] = None):
    if not API_SECRET:
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    token = authorization.replace("Bearer ", "")
    if token != API_SECRET:
        raise HTTPException(status_code=401, detail="Invalid API secret")


# ─── Logging helper ──────────────────────────────────

def log(msg: str):
    """Print with timestamp for Render logs."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ─── Selenium Helpers ─────────────────────────────────

def create_driver() -> tuple[webdriver.Chrome, str]:
    """Create a headless Chrome WebDriver with anti-detection. Returns (driver, temp_profile_path)."""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-translate")
    options.add_argument("--metrics-recording-only")
    options.add_argument("--no-first-run")
    options.add_argument("--safebrowsing-disable-auto-update")
    options.add_argument("--blink-settings=imagesEnabled=false")
    # Anti-detection flags
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    )

    temp_profile = tempfile.mkdtemp(prefix="selenium-profile-")
    options.add_argument(f"--user-data-dir={temp_profile}")

    driver = webdriver.Chrome(options=options)
    # Hide webdriver property from navigator
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.navigator.chrome = {runtime: {}, loadTimes: function(){}, csi: function(){}};
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        """
    })
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(120)  # Allow time for parallel CSV fetches
    return driver, temp_profile


def cleanup_driver(driver: Optional[webdriver.Chrome], temp_profile: Optional[str]):
    """Safely quit driver and remove temp profile directory."""
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
    if temp_profile:
        try:
            shutil.rmtree(temp_profile, ignore_errors=True)
        except Exception:
            pass


# ─── Login ────────────────────────────────────────────

def login_to_pcc(driver: webdriver.Chrome, username: str, password: str):
    """Login to PCC via Selenium."""
    log("Navigating to PCC login page...")
    driver.get("https://www.propertycontrolcenter.com/users/")
    time.sleep(3)

    log("Filling login form...")
    wait = WebDriverWait(driver, 15)
    username_field = wait.until(EC.presence_of_element_located((By.NAME, "username")))
    password_field = driver.find_element(By.NAME, "password")
    username_field.send_keys(username)
    password_field.send_keys(password)

    login_button = wait.until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "input[type='submit'][value='Login'], input.button.submit")
        )
    )
    login_button.click()
    time.sleep(5)

    # Verify login succeeded
    wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//li[@class='toproot']/a[text()='Search']")
        )
    )
    if driver.find_elements(By.NAME, "username"):
        raise Exception("Login form still present after login attempt")

    log("Login successful")


# ─── Power Search ─────────────────────────────────────

def navigate_to_power_search(driver: webdriver.Chrome):
    """Navigate to the Power Search page and wait for the form to load."""
    wait = WebDriverWait(driver, 15)

    log("Clicking Search menu...")
    search_menu = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//li[@class='toproot']/a[text()='Search']")
        )
    )
    search_menu.click()
    time.sleep(1)

    log("Clicking Power Search link...")
    power_search_link = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//a[@href='/users/?action=powersearch'][text()='Power Search']")
        )
    )
    power_search_link.click()
    time.sleep(3)

    wait.until(EC.presence_of_element_located((By.ID, "reportFilter")))

    # Close popup if present
    try:
        driver.find_element(By.ID, "panel-1012-bodyWrap")
        close_button = wait.until(EC.element_to_be_clickable((By.ID, "tool-1013")))
        close_button.click()
        time.sleep(1)
        log("Closed popup dialog")
    except Exception:
        pass


def set_filters(driver: webdriver.Chrome, listing_status: str, time_range: str) -> dict:
    """Set filter criteria on the Power Search form."""
    results = {"status_set": False, "date_set": False}

    # ── Listing Status (select dropdown) ──
    try:
        status_select = driver.find_element(By.CSS_SELECTOR, "#reportFilter select[name='status']")
        Select(status_select).select_by_visible_text(listing_status)
        results["status_set"] = True
        log(f"Set listing status to '{listing_status}'")
    except Exception as e:
        log(f"Could not set status: {e}")

    # ── Date Range (radio buttons: name='dt') ──
    # Map time_range to PCC radio button values
    dt_map = {
        "24h": "Last24Hours",
        "7d": "LastWeek",
        "14d": "LastMonth",  # No 14d option, use LastMonth as closest
        "30d": "LastMonth",
    }
    dt_value = dt_map.get(time_range, "LastWeek")

    try:
        radio = driver.find_element(
            By.CSS_SELECTOR, f"#reportFilter input[name='dt'][value='{dt_value}']"
        )
        driver.execute_script("arguments[0].click();", radio)
        results["date_set"] = True
        log(f"Set date range to '{dt_value}' (from time_range='{time_range}')")
    except Exception as e:
        log(f"Could not set date range: {e}")

    return results


def submit_search(driver: webdriver.Chrome):
    """Submit the Power Search form."""
    selectors = [
        (By.CSS_SELECTOR, "#reportFilter input[type='submit']"),
        (By.XPATH, "//input[@value='Search']"),
        (By.XPATH, "//input[@value='Run Search']"),
    ]
    for selector_type, selector_value in selectors:
        try:
            btn = driver.find_element(selector_type, selector_value)
            if btn.is_displayed():
                btn.click()
                log(f"Clicked search button")
                return
        except Exception:
            continue

    driver.execute_script("document.getElementById('reportFilter').submit()")
    log("Submitted form via JavaScript")


def get_total_pages(driver: webdriver.Chrome, per_page: int = 15) -> int:
    """Extract total page count from the result count text on the page."""
    import re
    import math
    page_text = driver.page_source

    # Look for "X results" or "X records" or "X properties" text
    matches = re.findall(r'(\d+)\s+(?:total\s+)?(?:results?|records?|listings?|properties)', page_text, re.IGNORECASE)
    if matches:
        total_results = max(int(m) for m in matches)
        total_pages = math.ceil(total_results / per_page)
        log(f"Found {total_results} total results -> {total_pages} pages")
        return total_pages

    # Fallback: check pagination links
    try:
        pagination = driver.find_elements(By.CSS_SELECTOR, "ul.pagination li a")
        page_nums = []
        for a in pagination:
            try:
                data_pagenum = a.get_attribute("data-pagenum")
                if data_pagenum:
                    page_nums.append(int(data_pagenum))
            except (ValueError, TypeError):
                continue
        if page_nums:
            return max(page_nums)
    except Exception:
        pass

    return 1


def extract_search_params(url: str) -> dict:
    """Extract query parameters from the search results URL."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    return {k: v[0] if len(v) == 1 else v for k, v in params.items()}


def build_csv_url(search_params: dict, page_num: int) -> str:
    """Build the direct CSV export URL for a given page number."""
    params = dict(search_params)
    params["export"] = "CSV"
    params["PageNum"] = str(page_num)
    return f"/users/?{urlencode(params)}"


# ─── Parallel CSV Fetch via Browser JS ────────────────

def fetch_csvs_parallel(driver: webdriver.Chrome, search_params: dict, total_pages: int) -> list:
    """
    Fetch all CSV pages in parallel using browser-native fetch().
    This bypasses the WAF (Akamai) since requests come from the actual browser session.
    Returns list of CSV text strings.
    """
    urls = [build_csv_url(search_params, page) for page in range(1, total_pages + 1)]
    log(f"Fetching {len(urls)} CSV pages in parallel via JS fetch()...")

    js = """
    var urls = arguments[0];
    var callback = arguments[arguments.length - 1];
    Promise.all(urls.map(function(url, idx) {
        return fetch(url, {credentials: 'include'})
            .then(function(response) {
                if (!response.ok) {
                    return {error: 'HTTP ' + response.status, text: '', page: idx + 1};
                }
                return response.text().then(function(text) {
                    return {error: null, text: text, page: idx + 1};
                });
            })
            .catch(function(err) {
                return {error: err.toString(), text: '', page: idx + 1};
            });
    })).then(function(results) {
        callback(results);
    });
    """
    results = driver.execute_async_script(js, urls)
    return results


def parse_csv_text(text: str) -> list:
    """Parse CSV text into list of dicts."""
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


# ─── Main Scrape Logic ────────────────────────────────

def _do_scrape(username: str, password: str, listing_status: str, time_range: str, max_pages: int) -> dict:
    """
    Full scrape flow:
    1. Selenium login (~15s)
    2. Navigate to Power Search, set filters, submit (~10s)
    3. Parallel JS fetch() of all CSV pages (~5-15s for up to 30 pages)
    Total: ~30-40s for hundreds of listings
    """
    driver = None
    temp_profile = None
    scrape_start = time.time()
    try:
        log(f"Starting scrape v5.0 (status={listing_status}, range={time_range}, max_pages={max_pages})...")
        driver, temp_profile = create_driver()
        log(f"Chrome launched in {time.time() - scrape_start:.1f}s")

        # Step 1: Login
        login_to_pcc(driver, username, password)
        log(f"Login complete at {time.time() - scrape_start:.1f}s")

        # Step 2: Navigate to Power Search and submit
        navigate_to_power_search(driver)
        set_filters(driver, listing_status, time_range)
        submit_search(driver)
        time.sleep(3)

        wait = WebDriverWait(driver, 15)
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable")))
            log("Results table found")
        except TimeoutException:
            page_text = driver.page_source.lower()
            if "no results" in page_text or "no records" in page_text or "no listings" in page_text:
                log("Search returned no results")
                return {"listings": [], "total_count": 0, "pages_scraped": 0}
            raise Exception("Timed out waiting for search results table")

        # Step 3: Determine pages and extract search params
        total_pages = get_total_pages(driver)
        pages_to_fetch = min(total_pages, max_pages)
        search_params = extract_search_params(driver.current_url)
        log(f"Found {total_pages} pages, fetching {pages_to_fetch}")
        log(f"Search setup complete at {time.time() - scrape_start:.1f}s")

        # Step 4: Parallel CSV fetch via JS
        fetch_start = time.time()
        results = fetch_csvs_parallel(driver, search_params, pages_to_fetch)
        fetch_elapsed = time.time() - fetch_start

        # Parse all CSVs
        all_listings = []
        errors = 0
        for r in results:
            if r.get("error"):
                log(f"Page {r.get('page', '?')} error: {r['error']}")
                errors += 1
            else:
                rows = parse_csv_text(r["text"])
                all_listings.extend(rows)

        # Deduplicate by InventoryID
        seen = set()
        unique_listings = []
        for listing in all_listings:
            inv_id = listing.get("InventoryID", "")
            if inv_id and inv_id not in seen:
                seen.add(inv_id)
                unique_listings.append(listing)
            elif not inv_id:
                unique_listings.append(listing)

        total_elapsed = time.time() - scrape_start
        log(f"Scrape complete: {len(unique_listings)} listings from {pages_to_fetch} pages "
            f"in {total_elapsed:.1f}s (fetch: {fetch_elapsed:.1f}s, errors: {errors})")

        return {
            "listings": unique_listings,
            "total_count": len(unique_listings),
            "pages_scraped": pages_to_fetch,
        }
    finally:
        cleanup_driver(driver, temp_profile)
        log("Chrome cleaned up")


def _do_test_login(username: str, password: str) -> dict:
    """Run login test in a blocking thread."""
    driver = None
    temp_profile = None
    try:
        driver, temp_profile = create_driver()
        login_to_pcc(driver, username, password)
        return {"success": True, "message": "Login successful"}
    finally:
        cleanup_driver(driver, temp_profile)


# ─── Endpoints ────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "listing-notifier-scraper", "version": "5.0.0"}


@app.get("/debug")
async def debug():
    """Check Chrome and ChromeDriver versions and test browser launch."""
    import subprocess
    info = {"service_version": "5.0.0"}

    try:
        result = subprocess.run(["google-chrome", "--version"], capture_output=True, text=True, timeout=10)
        info["chrome_version"] = result.stdout.strip()
    except Exception as e:
        info["chrome_version_error"] = str(e)

    try:
        result = subprocess.run(["chromedriver", "--version"], capture_output=True, text=True, timeout=10)
        info["chromedriver_version"] = result.stdout.strip()
    except Exception as e:
        info["chromedriver_version"] = "Not installed (using SeleniumManager)"

    try:
        driver, temp_profile = create_driver()
        info["browser_launch"] = "success"
        info["browser_version"] = driver.capabilities.get("browserVersion", "unknown")
        info["chromedriver_actual"] = driver.capabilities.get("chrome", {}).get("chromedriverVersion", "unknown")
        cleanup_driver(driver, temp_profile)
    except Exception as e:
        info["browser_launch"] = "failed"
        info["browser_error"] = str(e)

    return info


@app.post("/test-login")
async def test_login(
    req: LoginTestRequest,
    authorization: Optional[str] = Header(None),
):
    verify_auth(authorization)
    try:
        result = await asyncio.to_thread(_do_test_login, req.username, req.password)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Login failed: {str(e)}")


@app.post("/scrape")
async def scrape(
    req: ScrapeRequest,
    authorization: Optional[str] = Header(None),
):
    verify_auth(authorization)
    try:
        result = await asyncio.to_thread(
            _do_scrape, req.username, req.password,
            req.listing_status, req.time_range, req.max_pages,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

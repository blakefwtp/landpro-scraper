"""
Listing Notifier Scraper Service
Hybrid approach: Selenium for login + Power Search form interaction, then CSV export.
"""

import os
import time
import asyncio
import tempfile
import shutil
from typing import Optional
from datetime import datetime, timedelta

import pandas as pd
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import json

app = FastAPI(title="Listing Notifier Scraper", version="4.1.0")

API_SECRET = os.environ.get("API_SECRET", "")


# ─── Models ───────────────────────────────────────────

class ScrapeRequest(BaseModel):
    username: str
    password: str
    time_range: str = "7d"
    listing_status: str = "Active"
    max_pages: int = 30


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
    """Create a headless Chrome WebDriver. Returns (driver, temp_profile_path)."""
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
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    )

    temp_profile = tempfile.mkdtemp(prefix="selenium-profile-")
    options.add_argument(f"--user-data-dir={temp_profile}")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(15)
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


def login_and_get_cookies(driver: webdriver.Chrome, username: str, password: str) -> dict:
    """Login to PCC via Selenium and return session cookies."""
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
    log("Clicking login button...")
    login_button.click()
    time.sleep(5)

    # Verify login succeeded
    log("Verifying login success...")
    wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//li[@class='toproot']/a[text()='Search']")
        )
    )

    if driver.find_elements(By.NAME, "username"):
        raise Exception("Login form still present after login attempt")

    # Extract cookies
    cookies = {}
    for cookie in driver.get_cookies():
        cookies[cookie["name"]] = cookie["value"]

    log(f"Login successful, got {len(cookies)} cookies")
    return cookies


# ─── Power Search with Custom Filters ────────────────

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

    log("Waiting for Power Search form...")
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

    log("Power Search form loaded")


def try_set_filters(driver: webdriver.Chrome, listing_status: str, time_range: str) -> dict:
    """
    Attempt to set filter criteria on the Power Search form.
    Uses multiple selector strategies with graceful fallbacks.
    """
    results = {"status_set": False, "date_set": False}

    # ── Listing Status ──
    try:
        selects = driver.find_elements(By.CSS_SELECTOR, "#reportFilter select")
        for sel in selects:
            try:
                options = sel.find_elements(By.TAG_NAME, "option")
                option_texts = [o.text.strip() for o in options]
                if any(s in option_texts for s in ["Active", "Sold", "Pending", "New"]):
                    select_obj = Select(sel)
                    select_obj.select_by_visible_text(listing_status)
                    results["status_set"] = True
                    log(f"Set listing status to '{listing_status}' via select dropdown")
                    break
            except Exception:
                continue
    except Exception as e:
        log(f"Select strategy for status failed: {e}")

    if not results["status_set"]:
        try:
            status_labels = driver.find_elements(
                By.XPATH,
                f"//label[normalize-space()='{listing_status}']"
            )
            for label in status_labels:
                try:
                    cb = label.find_element(By.XPATH, ".//input[@type='checkbox'] | ./preceding-sibling::input[@type='checkbox'][1] | ./following-sibling::input[@type='checkbox'][1]")
                    if not cb.is_selected():
                        cb.click()
                        time.sleep(0.3)
                    results["status_set"] = True
                    log(f"Set listing status '{listing_status}' via checkbox")
                    break
                except Exception:
                    continue
        except Exception as e:
            log(f"Checkbox strategy for status failed: {e}")

    # ── Date Range ──
    days_map = {"24h": 1, "7d": 7, "14d": 14, "30d": 30}
    days = days_map.get(time_range, 7)

    try:
        dom_inputs = driver.find_elements(
            By.XPATH,
            "//input[contains(@name, 'days') or contains(@name, 'dom') or "
            "contains(@name, 'Days') or contains(@name, 'DOM') or "
            "contains(@id, 'days') or contains(@id, 'dom')]"
        )
        if dom_inputs:
            dom_inputs[0].clear()
            dom_inputs[0].send_keys(str(days))
            results["date_set"] = True
            log(f"Set days on market to {days}")
    except Exception as e:
        log(f"DOM input for date failed: {e}")

    if not results["date_set"]:
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            date_inputs = driver.find_elements(
                By.XPATH,
                "//input[contains(@name, 'date') or contains(@name, 'Date') or "
                "contains(@id, 'date') or contains(@id, 'Date') or @type='date']"
            )
            if len(date_inputs) >= 2:
                date_inputs[0].clear()
                date_inputs[0].send_keys(start_date.strftime("%m/%d/%Y"))
                date_inputs[1].clear()
                date_inputs[1].send_keys(end_date.strftime("%m/%d/%Y"))
                results["date_set"] = True
                log(f"Set date range: {start_date.strftime('%m/%d/%Y')} to {end_date.strftime('%m/%d/%Y')}")
        except Exception as e:
            log(f"Date inputs for date failed: {e}")

    if not results["date_set"]:
        try:
            selects = driver.find_elements(By.CSS_SELECTOR, "#reportFilter select")
            for sel in selects:
                try:
                    options = sel.find_elements(By.TAG_NAME, "option")
                    option_texts = [o.text.strip().lower() for o in options]
                    if any(kw in " ".join(option_texts) for kw in ["day", "week", "month", "hour"]):
                        select_obj = Select(sel)
                        target_texts = {
                            "24h": ["24 hours", "1 day", "last 24", "today"],
                            "7d": ["7 days", "1 week", "last 7", "last week"],
                            "14d": ["14 days", "2 weeks", "last 14"],
                            "30d": ["30 days", "1 month", "last 30", "last month"],
                        }
                        for target in target_texts.get(time_range, []):
                            for option in options:
                                if target.lower() in option.text.strip().lower():
                                    select_obj.select_by_visible_text(option.text.strip())
                                    results["date_set"] = True
                                    log(f"Set time range via select: {option.text.strip()}")
                                    break
                            if results["date_set"]:
                                break
                        if results["date_set"]:
                            break
                except Exception:
                    continue
        except Exception as e:
            log(f"Time select for date failed: {e}")

    return results


def submit_search_form(driver: webdriver.Chrome):
    """Find and click the search/submit button on the Power Search form."""
    selectors = [
        (By.CSS_SELECTOR, "#reportFilter input[type='submit']"),
        (By.XPATH, "//input[@value='Search']"),
        (By.XPATH, "//input[@value='Run Search']"),
        (By.XPATH, "//input[@value='Run']"),
        (By.XPATH, "//button[contains(text(), 'Search')]"),
        (By.XPATH, "//a[contains(@class, 'search-btn')]"),
        (By.CSS_SELECTOR, "form#reportFilter button[type='submit']"),
    ]

    for selector_type, selector_value in selectors:
        try:
            btn = driver.find_element(selector_type, selector_value)
            if btn.is_displayed():
                btn.click()
                log(f"Clicked search button via: {selector_value}")
                return True
        except Exception:
            continue

    try:
        driver.execute_script("document.getElementById('reportFilter').submit()")
        log("Submitted form via JavaScript")
        return True
    except Exception:
        pass

    raise Exception("Could not find or click search button on Power Search page")


def run_power_search(driver: webdriver.Chrome, listing_status: str, time_range: str, max_pages: int, deadline: float = 0) -> list:
    """Navigate to Power Search, set filters, run the search, and export results."""
    wait = WebDriverWait(driver, 15)

    navigate_to_power_search(driver)

    filter_results = try_set_filters(driver, listing_status, time_range)
    log(f"Filter results: {filter_results}")

    log("Submitting search...")
    submit_search_form(driver)
    time.sleep(3)

    log("Waiting for results table...")
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable")))
        log("Results table found")
    except TimeoutException:
        page_text = driver.page_source.lower()
        if "no results" in page_text or "no records" in page_text or "no listings" in page_text:
            log("Search returned no results")
            return []
        log(f"No results table found. Page title: {driver.title}")
        raise Exception("Timed out waiting for search results table")

    export_folder = tempfile.mkdtemp(prefix="scraper-exports-")
    csv_files = export_all_pages_selenium(driver, export_folder, max_pages, deadline)
    df = merge_csvs(csv_files)
    shutil.rmtree(export_folder, ignore_errors=True)

    listings = df.to_dict(orient="records") if not df.empty else []
    log(f"Power search complete: {len(listings)} listings from {len(csv_files)} pages")
    return listings


def export_all_pages_selenium(driver, export_folder, max_pages, deadline: float = 0):
    """Export CSV from each page via Selenium. Stops early if deadline approaching."""
    wait = WebDriverWait(driver, 20)
    current_page = 1
    csv_files = []

    def list_downloads():
        return [os.path.join(export_folder, f) for f in os.listdir(export_folder)]

    def wait_for_download(before_set, timeout=60):
        end = time.time() + timeout
        while time.time() < end:
            current = set(list_downloads())
            new_files = [
                p for p in (current - before_set)
                if (p.endswith(".csv") or p.endswith(".txt"))
                and not p.endswith(".crdownload")
            ]
            if new_files:
                return max(new_files, key=os.path.getctime)
            time.sleep(0.5)
        raise TimeoutException("Download timed out")

    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": export_folder,
    })

    while current_page <= max_pages:
        # Check deadline before starting a new page
        if deadline and time.time() > deadline - 30:
            log(f"Approaching timeout, stopping after {current_page - 1} pages (have {len(csv_files)} exports)")
            break

        log(f"Exporting page {current_page}...")
        try:
            export_link = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//a[contains(text(), 'Export Search Results')]")
                )
            )
            export_link.click()
            time.sleep(1)

            before = set(list_downloads())

            csv_link = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//div[@id='exportChooser-body']//a[contains(text(), 'CSV (Comma Separated Value) File')]")
                )
            )
            csv_link.click()

            downloaded_path = wait_for_download(before)
            new_path = os.path.join(export_folder, f"page_{current_page}.csv")
            shutil.move(downloaded_path, new_path)
            csv_files.append(new_path)
            log(f"Page {current_page} exported successfully")

        except Exception as e:
            log(f"Error exporting page {current_page}: {e}")
            break

        if current_page < max_pages:
            try:
                next_link = wait.until(
                    EC.element_to_be_clickable(
                        (By.XPATH, f"//ul[contains(@class, 'pagination')]/li[a/@data-pagenum='{current_page + 1}']/a")
                    )
                )
                next_link.click()
                time.sleep(3)
                current_page += 1
            except Exception:
                log(f"No more pages after page {current_page}")
                break
        else:
            break

    return csv_files


def merge_csvs(csv_files: list) -> pd.DataFrame:
    """Merge multiple CSV files into a single DataFrame."""
    if not csv_files:
        return pd.DataFrame()

    frames = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, dtype=str, on_bad_lines="skip")
        except Exception:
            df = pd.read_csv(csv_file, dtype=str, on_bad_lines="skip", encoding="latin1")
        frames.append(df)

    return pd.concat(frames, ignore_index=True).fillna("")


# ─── Synchronous scrape logic (runs in thread pool) ──

SCRAPE_TIMEOUT_SECONDS = 210  # 3.5 minutes — leaves headroom for the 5-min route timeout

def _do_scrape(username: str, password: str, listing_status: str, time_range: str, max_pages: int) -> dict:
    """Run the full scrape in a blocking thread with internal timeout."""
    driver = None
    temp_profile = None
    scrape_start = time.time()
    try:
        log(f"Starting scrape (max_pages={max_pages}, timeout={SCRAPE_TIMEOUT_SECONDS}s)...")
        driver, temp_profile = create_driver()
        log("Chrome launched successfully")
        login_and_get_cookies(driver, username, password)

        elapsed = time.time() - scrape_start
        remaining = SCRAPE_TIMEOUT_SECONDS - elapsed
        if remaining < 30:
            raise Exception(f"Login took too long ({elapsed:.0f}s), no time left for search")

        listings = run_power_search(driver, listing_status, time_range, max_pages, deadline=scrape_start + SCRAPE_TIMEOUT_SECONDS)
        return {
            "listings": listings,
            "total_count": len(listings),
            "pages_scraped": max_pages,
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
        login_and_get_cookies(driver, username, password)
        return {"success": True, "message": "Login successful"}
    finally:
        cleanup_driver(driver, temp_profile)


# ─── Endpoints ────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "listing-notifier-scraper", "version": "4.0.0"}


@app.get("/debug")
async def debug():
    """Check Chrome and ChromeDriver versions and test browser launch."""
    import subprocess
    info = {"service_version": "4.0.0"}

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


@app.post("/scrape-stream")
async def scrape_streamed(
    req: ScrapeRequest,
    authorization: Optional[str] = Header(None),
):
    """Streaming endpoint that sends progress events during the scrape."""
    verify_auth(authorization)

    def generate():
        driver = None
        temp_profile = None
        try:
            yield f"event: status\ndata: {json.dumps({'step': 'browser', 'message': 'Launching browser...'})}\n\n"
            driver, temp_profile = create_driver()

            yield f"event: status\ndata: {json.dumps({'step': 'login', 'message': 'Logging into Property Control Center...'})}\n\n"
            login_and_get_cookies(driver, req.username, req.password)
            yield f"event: status\ndata: {json.dumps({'step': 'logged_in', 'message': 'Login successful, running search...'})}\n\n"

            listings = run_power_search(driver, req.listing_status, req.time_range, req.max_pages)

            yield f"event: status\ndata: {json.dumps({'step': 'scraped', 'message': f'Found {len(listings)} listings'})}\n\n"

            result = {
                "listings": listings,
                "total_count": len(listings),
                "pages_scraped": req.max_pages,
            }
            yield f"event: complete\ndata: {json.dumps(result)}\n\n"

        except Exception as e:
            yield f"event: error\ndata: {json.dumps({'message': str(e)})}\n\n"
        finally:
            cleanup_driver(driver, temp_profile)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )

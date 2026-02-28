"""
Listing Notifier Scraper Service
Hybrid approach: Selenium for login + Power Search form interaction, then CSV export.
"""

import os
import time
import tempfile
import shutil
import re
import requests as http_requests  # kept for potential future HTTP-based scraping
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

app = FastAPI(title="Listing Notifier Scraper", version="3.0.0")

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


# ─── Selenium Helpers ─────────────────────────────────

def create_driver() -> webdriver.Chrome:
    """Create a headless Chrome WebDriver."""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-images")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    )

    temp_profile = tempfile.mkdtemp(prefix="selenium-profile-")
    options.add_argument(f"--user-data-dir={temp_profile}")

    driver = webdriver.Chrome(options=options)
    return driver


def login_and_get_cookies(driver: webdriver.Chrome, username: str, password: str) -> dict:
    """Login to PCC via Selenium and return session cookies."""
    driver.get("https://www.propertycontrolcenter.com/users/")
    time.sleep(3)

    wait = WebDriverWait(driver, 20)
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

    # Extract cookies for use with requests library
    cookies = {}
    for cookie in driver.get_cookies():
        cookies[cookie["name"]] = cookie["value"]

    return cookies


# ─── Power Search with Custom Filters ────────────────

def navigate_to_power_search(driver: webdriver.Chrome):
    """Navigate to the Power Search page and wait for the form to load."""
    wait = WebDriverWait(driver, 20)

    search_menu = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//li[@class='toproot']/a[text()='Search']")
        )
    )
    search_menu.click()
    time.sleep(1)

    power_search_link = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//a[@href='/users/?action=powersearch'][text()='Power Search']")
        )
    )
    power_search_link.click()
    time.sleep(5)
    wait.until(EC.presence_of_element_located((By.ID, "reportFilter")))

    # Close popup if present
    try:
        driver.find_element(By.ID, "panel-1012-bodyWrap")
        close_button = wait.until(EC.element_to_be_clickable((By.ID, "tool-1013")))
        close_button.click()
        time.sleep(2)
    except Exception:
        pass


def try_set_filters(driver: webdriver.Chrome, listing_status: str, time_range: str) -> dict:
    """
    Attempt to set filter criteria on the Power Search form.
    Returns a dict describing what was successfully set.
    Uses multiple selector strategies with graceful fallbacks.
    """
    results = {"status_set": False, "date_set": False}

    # ── Listing Status ──
    # Strategy 1: Find select elements and check if any has status-like options
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
                    print(f"Set listing status to '{listing_status}' via select dropdown")
                    break
            except Exception:
                continue
    except Exception as e:
        print(f"Strategy 1 (select) for status failed: {e}")

    # Strategy 2: Find checkboxes near status labels
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
                    print(f"Set listing status '{listing_status}' via checkbox")
                    break
                except Exception:
                    continue
        except Exception as e:
            print(f"Strategy 2 (checkbox) for status failed: {e}")

    # ── Date Range ──
    days_map = {"24h": 1, "7d": 7, "14d": 14, "30d": 30}
    days = days_map.get(time_range, 7)

    # Strategy 1: Look for "Days on Market" or "DOM" input
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
            print(f"Set days on market to {days}")
    except Exception as e:
        print(f"Strategy 1 (DOM input) for date failed: {e}")

    # Strategy 2: Look for date input fields and set a date range
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
                print(f"Set date range: {start_date.strftime('%m/%d/%Y')} to {end_date.strftime('%m/%d/%Y')}")
        except Exception as e:
            print(f"Strategy 2 (date inputs) for date failed: {e}")

    # Strategy 3: Look for a time range select
    if not results["date_set"]:
        try:
            selects = driver.find_elements(By.CSS_SELECTOR, "#reportFilter select")
            for sel in selects:
                try:
                    options = sel.find_elements(By.TAG_NAME, "option")
                    option_texts = [o.text.strip().lower() for o in options]
                    if any(kw in " ".join(option_texts) for kw in ["day", "week", "month", "hour"]):
                        select_obj = Select(sel)
                        # Try to find an option matching our time range
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
                                    print(f"Set time range via select: {option.text.strip()}")
                                    break
                            if results["date_set"]:
                                break
                        if results["date_set"]:
                            break
                except Exception:
                    continue
        except Exception as e:
            print(f"Strategy 3 (time select) for date failed: {e}")

    return results


def submit_search_form(driver: webdriver.Chrome):
    """Find and click the search/submit button on the Power Search form."""
    wait = WebDriverWait(driver, 10)

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
                print(f"Clicked search button via: {selector_value}")
                return True
        except Exception:
            continue

    # Last resort: submit the form directly via JavaScript
    try:
        driver.execute_script("document.getElementById('reportFilter').submit()")
        print("Submitted form via JavaScript")
        return True
    except Exception:
        pass

    raise Exception("Could not find or click search button on Power Search page")


def run_power_search(driver: webdriver.Chrome, listing_status: str, time_range: str, max_pages: int) -> list:
    """
    Navigate to Power Search, set filters, run the search, and export results.
    This replaces the old saved-search approach.
    """
    wait = WebDriverWait(driver, 20)

    # 1. Navigate to Power Search
    navigate_to_power_search(driver)

    # 2. Try to set search filters
    filter_results = try_set_filters(driver, listing_status, time_range)
    print(f"Filter results: {filter_results}")

    # 3. Submit the search
    submit_search_form(driver)
    time.sleep(5)

    # 4. Wait for results table
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable")))
    except TimeoutException:
        # Check if there's a "no results" message
        page_text = driver.page_source.lower()
        if "no results" in page_text or "no records" in page_text or "no listings" in page_text:
            print("Search returned no results")
            return []
        raise Exception("Timed out waiting for search results table")

    # 5. Export all pages
    export_folder = tempfile.mkdtemp(prefix="scraper-exports-")
    csv_files = export_all_pages_selenium(driver, export_folder, max_pages)
    df = merge_csvs(csv_files)
    shutil.rmtree(export_folder, ignore_errors=True)

    return df.to_dict(orient="records") if not df.empty else []


def export_all_pages_selenium(driver, export_folder, max_pages):
    """Export CSV from each page via Selenium."""
    wait = WebDriverWait(driver, 30)
    current_page = 1
    csv_files = []

    def list_downloads():
        return [os.path.join(export_folder, f) for f in os.listdir(export_folder)]

    def wait_for_download(before_set, timeout=90):
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

    # Need to set download directory for Selenium
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": export_folder,
    })

    while current_page <= max_pages:
        try:
            export_link = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//a[contains(text(), 'Export Search Results')]")
                )
            )
            export_link.click()
            time.sleep(1.5)

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

        except Exception as e:
            print(f"Error exporting page {current_page}: {e}")
            break

        if current_page < max_pages:
            try:
                next_link = wait.until(
                    EC.element_to_be_clickable(
                        (By.XPATH, f"//ul[contains(@class, 'pagination')]/li[a/@data-pagenum='{current_page + 1}']/a")
                    )
                )
                next_link.click()
                time.sleep(5)
                current_page += 1
            except Exception:
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


# ─── Endpoints ────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "listing-notifier-scraper", "version": "3.0.0"}


@app.post("/test-login")
async def test_login(
    req: LoginTestRequest,
    authorization: Optional[str] = Header(None),
):
    verify_auth(authorization)

    driver = None
    try:
        driver = create_driver()
        login_and_get_cookies(driver, req.username, req.password)
        return {"success": True, "message": "Login successful"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Login failed: {str(e)}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


@app.post("/scrape")
async def scrape(
    req: ScrapeRequest,
    authorization: Optional[str] = Header(None),
):
    verify_auth(authorization)

    driver = None
    try:
        # Launch browser and login
        driver = create_driver()
        login_and_get_cookies(driver, req.username, req.password)  # logs in via Selenium

        # Run Power Search with custom filters (driver is already logged in)
        listings = run_power_search(driver, req.listing_status, req.time_range, req.max_pages)

        # Close driver ASAP
        driver.quit()
        driver = None

        return {
            "listings": listings,
            "total_count": len(listings),
            "pages_scraped": req.max_pages,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


@app.post("/scrape-stream")
async def scrape_streamed(
    req: ScrapeRequest,
    authorization: Optional[str] = Header(None),
):
    """Streaming endpoint that sends progress events during the scrape."""
    verify_auth(authorization)

    def generate():
        driver = None
        try:
            yield f"event: status\ndata: {json.dumps({'step': 'browser', 'message': 'Launching browser...'})}\n\n"
            driver = create_driver()

            yield f"event: status\ndata: {json.dumps({'step': 'login', 'message': 'Logging into Property Control Center...'})}\n\n"
            login_and_get_cookies(driver, req.username, req.password)
            yield f"event: status\ndata: {json.dumps({'step': 'logged_in', 'message': 'Login successful, running search...'})}\n\n"

            listings = run_power_search(driver, req.listing_status, req.time_range, req.max_pages)

            driver.quit()
            driver = None

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
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )

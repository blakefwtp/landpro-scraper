"""
Listing Notifier Scraper Service
Hybrid approach: Selenium for login, then direct HTTP for data export.
"""

import os
import io
import time
import tempfile
import shutil
import re
import requests as http_requests
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import json

app = FastAPI(title="Listing Notifier Scraper", version="2.0.0")

API_SECRET = os.environ.get("API_SECRET", "")


# ─── Models ───────────────────────────────────────────

class ScrapeRequest(BaseModel):
    username: str
    password: str
    time_range: str = "7d"
    saved_search_name: str = "New Listings - last week"
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


def get_saved_search_id(cookies: dict, search_name: str) -> Optional[str]:
    """Try to find the saved search ID by loading the power search page."""
    session = http_requests.Session()
    session.cookies.update(cookies)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    })

    # Load power search page to find saved searches
    resp = session.get("https://www.propertycontrolcenter.com/users/?action=powersearch")
    if resp.status_code != 200:
        return None

    # Look for saved search links in the HTML
    # Pattern: savedSearchId=XXXXX or similar
    html = resp.text
    # Try to find the search by name in the page
    pattern = rf'data-searchid=["\'](\d+)["\'][^>]*>{re.escape(search_name)}'
    match = re.search(pattern, html, re.IGNORECASE)
    if match:
        return match.group(1)

    return None


# ─── Fallback: Full Selenium Scrape ──────────────────

def full_selenium_scrape(driver: webdriver.Chrome, search_name: str, max_pages: int) -> list:
    """Fallback: do the entire scrape with Selenium (original approach)."""
    wait = WebDriverWait(driver, 20)

    # Navigate to Power Search
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
        wait.until(EC.presence_of_element_located((By.ID, "panel-1012-bodyWrap")))
        close_button = wait.until(EC.element_to_be_clickable((By.ID, "tool-1013")))
        close_button.click()
        time.sleep(2)
    except Exception:
        pass

    # Load saved search
    load_saved_link = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//a[contains(text(), 'Load a Saved Search')]")
        )
    )
    load_saved_link.click()
    time.sleep(2)
    wait.until(EC.presence_of_element_located((By.ID, "savedPowerSearches-body")))

    search_row = wait.until(
        EC.presence_of_element_located(
            (By.XPATH, f"//td[contains(@class, 'description-td') and contains(text(), '{search_name}')]/ancestor::tr")
        )
    )
    run_link = search_row.find_element(By.XPATH, ".//td[3]/a[contains(text(), 'Run')]")
    run_link.click()
    time.sleep(5)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable")))

    # Export pages
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


# ─── Streaming Scrape ─────────────────────────────────

def scrape_stream(username: str, password: str, search_name: str, max_pages: int):
    """Generator that yields SSE events during the scrape process."""
    driver = None
    try:
        # Step 1: Launch browser
        yield f"event: status\ndata: {json.dumps({'step': 'browser', 'message': 'Launching browser...'})}\n\n"
        driver = create_driver()

        # Step 2: Login
        yield f"event: status\ndata: {json.dumps({'step': 'login', 'message': 'Logging into Property Control Center...'})}\n\n"
        cookies = login_and_get_cookies(driver, username, password)
        yield f"event: status\ndata: {json.dumps({'step': 'logged_in', 'message': 'Login successful, starting scrape...'})}\n\n"

        # Step 3: Scrape using Selenium (with progress updates)
        listings = full_selenium_scrape(driver, search_name, max_pages)

        # Close driver ASAP to free memory
        driver.quit()
        driver = None

        yield f"event: status\ndata: {json.dumps({'step': 'scraped', 'message': f'Found {len(listings)} listings'})}\n\n"

        # Step 4: Done
        result = {
            "listings": listings,
            "total_count": len(listings),
            "pages_scraped": max_pages,
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


# ─── Endpoints ────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "listing-notifier-scraper", "version": "2.0.0"}


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
        cookies = login_and_get_cookies(driver, req.username, req.password)

        # Do the full scrape
        listings = full_selenium_scrape(driver, req.saved_search_name, req.max_pages)

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

    return StreamingResponse(
        scrape_stream(req.username, req.password, req.saved_search_name, req.max_pages),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )

"""
Listing Notifier Scraper Service
Headless Selenium scraper for Property Control Center (land.com).
Accepts user credentials and returns structured listing data as JSON.
"""

import os
import io
import time
import tempfile
import shutil
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

app = FastAPI(title="Listing Notifier Scraper", version="1.0.0")

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
        return  # No secret configured, skip auth (dev mode)
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    token = authorization.replace("Bearer ", "")
    if token != API_SECRET:
        raise HTTPException(status_code=401, detail="Invalid API secret")


# ─── Selenium Helpers ─────────────────────────────────

def create_driver(export_folder: str) -> webdriver.Chrome:
    """Create a headless Chrome WebDriver."""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    )

    # Download directory
    options.add_experimental_option("prefs", {
        "download.default_directory": export_folder,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    })

    # Fresh temp profile
    temp_profile = tempfile.mkdtemp(prefix="selenium-profile-")
    options.add_argument(f"--user-data-dir={temp_profile}")

    driver = webdriver.Chrome(options=options)
    driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    return driver


def login(driver: webdriver.Chrome, username: str, password: str) -> bool:
    """Login to Property Control Center. Returns True on success."""
    driver.get("https://www.propertycontrolcenter.com/users/")
    time.sleep(5)

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
    time.sleep(8)

    # Verify login succeeded
    wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//li[@class='toproot']/a[text()='Search']")
        )
    )

    if driver.find_elements(By.NAME, "username"):
        raise Exception("Login form still present after login attempt")

    return True


def navigate_to_power_search(driver: webdriver.Chrome):
    """Navigate to the Power Search page."""
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


def load_saved_search(driver: webdriver.Chrome, search_name: str):
    """Load and run a saved search by name."""
    wait = WebDriverWait(driver, 20)

    # Close popup if present
    try:
        wait.until(EC.presence_of_element_located((By.ID, "panel-1012-bodyWrap")))
        close_button = wait.until(EC.element_to_be_clickable((By.ID, "tool-1013")))
        close_button.click()
        time.sleep(2)
    except Exception:
        pass

    # Click "Load a Saved Search"
    load_saved_link = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//a[contains(text(), 'Load a Saved Search')]")
        )
    )
    load_saved_link.click()
    time.sleep(2)

    # Wait for saved searches popup
    wait.until(EC.presence_of_element_located((By.ID, "savedPowerSearches-body")))

    # Find and run the saved search
    search_row = wait.until(
        EC.presence_of_element_located(
            (By.XPATH, f"//td[contains(@class, 'description-td') and contains(text(), '{search_name}')]/ancestor::tr")
        )
    )
    run_link = search_row.find_element(By.XPATH, ".//td[3]/a[contains(text(), 'Run')]")
    run_link.click()
    time.sleep(5)

    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable")))


def export_all_pages(driver: webdriver.Chrome, export_folder: str, max_pages: int = 30) -> list:
    """Export CSV from each page of search results and return list of file paths."""
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

        # Navigate to next page
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
    return {"status": "ok", "service": "listing-notifier-scraper"}


@app.post("/test-login")
async def test_login(
    req: LoginTestRequest,
    authorization: Optional[str] = Header(None),
):
    verify_auth(authorization)

    driver = None
    try:
        export_folder = tempfile.mkdtemp(prefix="scraper-exports-")
        driver = create_driver(export_folder)
        login(driver, req.username, req.password)
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
    export_folder = tempfile.mkdtemp(prefix="scraper-exports-")

    try:
        # 1. Create driver and login
        driver = create_driver(export_folder)
        login(driver, req.username, req.password)

        # 2. Navigate to Power Search
        navigate_to_power_search(driver)

        # 3. Load saved search
        load_saved_search(driver, req.saved_search_name)

        # 4. Export all pages
        csv_files = export_all_pages(driver, export_folder, req.max_pages)

        # 5. Merge CSVs
        df = merge_csvs(csv_files)

        # 6. Clean up driver
        driver.quit()
        driver = None

        if df.empty:
            return {"listings": [], "total_count": 0, "pages_scraped": 0}

        # 7. Convert to list of dicts
        listings = df.to_dict(orient="records")

        # 8. Clean up temp files
        shutil.rmtree(export_folder, ignore_errors=True)

        return {
            "listings": listings,
            "total_count": len(listings),
            "pages_scraped": len(csv_files),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        shutil.rmtree(export_folder, ignore_errors=True)

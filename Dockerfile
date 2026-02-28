FROM python:3.11-slim-bookworm

# Install Chrome
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    && wget -q -O /tmp/google-chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y /tmp/google-chrome.deb || apt-get install -fy \
    && rm /tmp/google-chrome.deb \
    && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver matching Chrome version
RUN CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+') \
    && DRIVER_URL="https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}.0/linux64/chromedriver-linux64.zip" \
    && wget -q "$DRIVER_URL" -O /tmp/chromedriver.zip || true \
    && if [ -f /tmp/chromedriver.zip ]; then \
         unzip /tmp/chromedriver.zip -d /tmp/ && \
         mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/ && \
         chmod +x /usr/local/bin/chromedriver; \
       else \
         echo "Using selenium-manager for chromedriver"; \
       fi \
    && rm -rf /tmp/chromedriver*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

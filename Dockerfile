FROM python:3.11-slim-bookworm

# Install Chrome dependencies and Chrome
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    && wget -q -O /tmp/google-chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y /tmp/google-chrome.deb || apt-get install -fy \
    && rm /tmp/google-chrome.deb \
    && rm -rf /var/lib/apt/lists/*

# Don't manually install ChromeDriver — let Selenium 4's built-in
# SeleniumManager download the correct matching version at runtime.

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

# 1. Base Image: Official lightweight Python 3.9
FROM python:3.9-slim

# 2. System Dependencies (Needed for pybaseball/lxml/scipy)
# We install git, libxml2, and build tools, then clean up to keep image small
RUN apt-get update && apt-get install -y \
    git \
    libxml2-dev \
    libxslt-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 3. Work Directory: Set the folder inside the container
WORKDIR /app

# 4. Dependencies: Copy requirements first (for caching layers)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Application Code: Copy the rest of the source code
COPY src/ ./src/
COPY tests/ ./tests/
COPY pytest.ini .

# 6. Default Command:
# When the container runs, it defaults to checking data integrity (SDET Approach)
CMD ["pytest", "tests/"]
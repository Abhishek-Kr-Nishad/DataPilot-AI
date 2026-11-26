FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DATAPILOT_EXECUTION_MODE=local

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY pyproject.toml .
COPY src/ ./src/
COPY aws/ ./aws/
COPY sql/ ./sql/
COPY scripts/ ./scripts/

ENV PYTHONPATH=/app/src

EXPOSE 8000

CMD ["uvicorn", "datapilot.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

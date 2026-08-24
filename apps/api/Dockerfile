FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends g++ unixodbc unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip

# Copie primeiro o projeto (pyproject + código) para existir o pacote "app"
COPY pyproject.toml /app/
COPY pytest.ini conftest.py /app/
COPY app /app/app

# Agora sim instala
RUN pip install --no-cache-dir .

EXPOSE 8000
# 2 workers: ingest/ETL síncrono não pode monopolizar o event loop do login.
# proxy-headers: rate-limit de login usa o IP real (X-Forwarded-For), não o gateway Docker.
CMD ["sh", "-c", "uvicorn app.main:app --host=0.0.0.0 --port=8000 --workers ${UVICORN_WORKERS:-2} --proxy-headers --forwarded-allow-ips='*'"]

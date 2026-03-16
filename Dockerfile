FROM python:3.12-slim

WORKDIR /app

# Зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Исходный код
COPY src/ ./src/
COPY main.py .

# Директория для SQLite и credentials (монтируется через volume)
RUN mkdir -p /app/data /app/credentials

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

CMD ["python", "main.py"]

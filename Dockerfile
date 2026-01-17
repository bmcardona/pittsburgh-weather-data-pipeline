
FROM Python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ ./scripts/
COPY coordinates.json .

RUN mkdir -p /app/outputs

ENV PYTHONUNBUFFERED=1

CMD ["python", "app/scripts/extract_weather_from_openmeteo.py"]

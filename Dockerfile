FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1
ENV DBT_SEND_ANONYMOUS_USAGE_STATS=false
CMD ["python", "scripts/run_e2e.py"]

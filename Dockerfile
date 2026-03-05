FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app.py scheduler.py /app/
COPY templates /app/templates
COPY static /app/static


COPY vclib.py /app/

EXPOSE 5000

CMD ["sh","-c","gunicorn -b 0.0.0.0:${FLASK_PORT:-5000} --workers 1 --threads 12 --timeout 300 --graceful-timeout 30 --keep-alive 5 app:app"]

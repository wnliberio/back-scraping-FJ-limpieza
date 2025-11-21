# backend-detectar/Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

USER root
RUN mkdir -p /tmp && chmod 1777 /tmp

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# dependencias
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# código
COPY . .

# puerto interno
ENV PORT=8000
EXPOSE 8000

# Módulo de entrada configurable (por defecto main:app)
ENV APP_MODULE=main:app
CMD ["sh","-lc","gunicorn -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8000 ${APP_MODULE}"]

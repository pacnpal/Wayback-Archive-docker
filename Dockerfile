FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    OUTPUT_DIR=/app/output

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        libxml2 \
        libxslt1.1 \
        libjpeg62-turbo \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 https://github.com/GeiserX/Wayback-Archive.git /tmp/wa \
 && pip install --no-cache-dir -r /tmp/wa/config/requirements.txt \
 && pip install --no-cache-dir /tmp/wa/config \
 && rm -rf /tmp/wa

COPY webui/requirements.txt /app/webui/requirements.txt
RUN pip install --no-cache-dir -r /app/webui/requirements.txt

COPY webui /app/webui

RUN mkdir -p /app/output
VOLUME ["/app/output"]
EXPOSE 8765

ENTRYPOINT ["uvicorn", "webui.app:app", "--host", "0.0.0.0", "--port", "8765"]

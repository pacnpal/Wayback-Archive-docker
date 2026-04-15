# syntax=docker/dockerfile:1.7
FROM python:3.14-slim

ARG APP_VERSION=dev
ENV APP_VERSION=$APP_VERSION \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    OUTPUT_DIR=/app/output \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Cached apt — the index and .deb archives persist across builds, so unchanged
# packages don't re-download. `rm -rf /var/lib/apt/lists/*` is skipped because
# the cache mount is ephemeral in the final image.
ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
        git \
        libxml2 \
        libxslt1.1 \
        libjpeg62-turbo \
        fonts-noto-color-emoji

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip

# Upstream engine — pip wheels cached between builds.
RUN --mount=type=cache,target=/root/.cache/pip \
    git clone --depth 1 https://github.com/GeiserX/Wayback-Archive.git /tmp/wa \
 && pip install -r /tmp/wa/config/requirements.txt \
 && pip install /tmp/wa/config \
 && rm -rf /tmp/wa

COPY webui/requirements.txt /app/webui/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /app/webui/requirements.txt

COPY webui /app/webui

# Render the file-cabinet emoji into PNG + ICO favicons at build time.
RUN python -c "from PIL import Image, ImageDraw, ImageFont; \
font_path='/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf'; \
f=ImageFont.truetype(font_path, 109); \
sizes=[(256,256),(128,128),(64,64),(48,48),(32,32),(16,16)]; \
im=Image.new('RGBA',(136,128),(0,0,0,0)); \
ImageDraw.Draw(im).text((4,0), '\U0001f5c4', font=f, embedded_color=True); \
im.resize((256,256),Image.LANCZOS).save('/app/webui/static/favicon.png'); \
im.save('/app/webui/static/favicon.ico', format='ICO', sizes=sizes)"

RUN mkdir -p /app/output
VOLUME ["/app/output"]
EXPOSE 8765

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8765/health', timeout=3).status==200 else 1)"

ENTRYPOINT ["uvicorn", "webui.app:app", "--host", "0.0.0.0", "--port", "8765"]

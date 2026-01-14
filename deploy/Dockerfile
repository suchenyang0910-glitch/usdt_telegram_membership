FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir -U pip setuptools wheel

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --retries 5 --timeout 60 -r /app/requirements.txt

ARG INSTALL_MEDIA=0
COPY requirements-media.txt /app/requirements-media.txt
RUN if [ "$INSTALL_MEDIA" = "1" ]; then pip install --no-cache-dir --retries 5 --timeout 60 -r /app/requirements-media.txt; fi

ARG INSTALL_USERBOT=0
COPY requirements-userbot.txt /app/requirements-userbot.txt
RUN if [ "$INSTALL_USERBOT" = "1" ]; then pip install --no-cache-dir --retries 5 --timeout 60 -r /app/requirements-userbot.txt; fi

COPY . /app

ENV PYTHONUNBUFFERED=1

CMD ["bash","-lc","python preflight.py && python main.py"]


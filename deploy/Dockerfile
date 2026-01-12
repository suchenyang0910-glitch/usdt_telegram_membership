FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

ARG INSTALL_MEDIA=0
COPY requirements-media.txt /app/requirements-media.txt
RUN if [ "$INSTALL_MEDIA" = "1" ]; then pip install --no-cache-dir -r /app/requirements-media.txt; fi

COPY . /app

ENV PYTHONUNBUFFERED=1

CMD ["bash","-lc","python preflight.py && python main.py"]


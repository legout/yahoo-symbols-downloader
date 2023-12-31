FROM python:3.11-slim-bookworm 

RUN apt-get update && apt-get install -y --no-install-recommends \
    git cron \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install pdm

WORKDIR /app

COPY yahoo_symbols_downloader yahoo_symbols_downloader
COPY pdm.lock pyproject.toml README.md ./
RUN pdm install --prod --no-lock
RUN mkdir -p logs


ENTRYPOINT [ "pdm", "run", "python", "-m", "yahoo_symbols_downloader.main" ]

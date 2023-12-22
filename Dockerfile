FROM python:3.11-slim-bookworm 

RUN apt-get update && apt-get install -y --no-install-recommends \
    git cron \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install pdm
WORKDIR /app
COPY yahoo_symbols yahoo_symbols
COPY pdm.lock pyproject.toml README.md ./
RUN pdm install --prod --no-lock


ENTRYPOINT [ "pdm", "run", "python", "-m", "yahoo_symbols.main" ]
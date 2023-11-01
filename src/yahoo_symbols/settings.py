from pathlib import Path

import msgspec


class S3Storage(msgspec.Struct):
    key: str | None
    secret: str | None
    endpoint_url: str | None
    profile: str | None
    path: str | None
    bucket: str | None
    partitioning: str | None


class LocalStorage(msgspec.Struct):
    bucket: str | None
    path: str | None
    partitioning: str | None


class SqliteStorage(msgspec.Struct):
    path: str | None


class Storage(msgspec.Struct):
    type: str
    local: LocalStorage
    s3: S3Storage
    sqlite: SqliteStorage


class Run(msgspec.Struct):
    types: list[str]
    query_length: int
    concurrency: int
    chunk_size: int
    sort_by: str | list[str]
    partitioning_columns: list[str]
    delta_subset: list[str]


class Download(msgspec.Struct):
    random_proxy: bool
    random_user_agent: bool
    max_retries: int
    random_delay_multiplier: int
    concurrency: int
    verbose: bool


class SchedLogging(msgspec.Struct):
    filename: str | None
    path: str | None
    repo: str | None


class Schedule(msgspec.Struct):
    cron: str | None
    logging: SchedLogging


class Parameters(msgspec.Struct):
    download: Download
    run: Run


class Settings(msgspec.Struct):
    storage: Storage
    parameters: Parameters
    schedule: Schedule


def load_settings(path: str):
    with open(path) as f:
        return msgspec.yaml.decode(f.read(), type=Settings)


SETTINGS = load_settings(Path(__file__).parents[2] / "config/settings.yml")

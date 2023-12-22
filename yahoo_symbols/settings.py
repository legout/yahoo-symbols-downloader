import msgspec


class S3Storage(msgspec.Struct):
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
    batch_size: int


class Download(msgspec.Struct):
    random_proxy: bool
    random_user_agent: bool
    max_retries: int
    random_delay_multiplier: int
    concurrency: int
    verbose: bool



class Parameters(msgspec.Struct):
    download: Download
    run: Run


class Settings(msgspec.Struct):
    storage: Storage
    parameters: Parameters



def load_settings(path: str):
    with open(path) as f:
        return msgspec.toml.decode(f.read(), type=Settings)

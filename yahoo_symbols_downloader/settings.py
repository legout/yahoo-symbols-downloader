import msgspec


class S3Storage(msgspec.Struct):
    profile: str | None
    path: str | None
    bucket: str | None
    partitioning: str | None

    def __post_init__(self):
        if self.profile == "":
            self.profile = None


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
    log_path: str | None


class Download(msgspec.Struct):
    random_proxy: bool
    random_user_agent: bool
    max_retries: int
    random_delay_multiplier: int
    concurrency: int
    verbose: bool
    debug: bool
    warnings: bool
    proxies: list[str] | str | None

    def __post_init__(self):
        if isinstance(self.proxies, str):
            if self.proxies != "":
                with open(self.proxies) as f:
                    self.proxies = f.read().splitlines()
            else:
                self.proxies = None


class Scheduler(msgspec.Struct):
    cron: str | list[str]


class Parameters(msgspec.Struct):
    download: Download
    run: Run
    scheduler: Scheduler


class Settings(msgspec.Struct):
    storage: Storage
    parameters: Parameters


def load_settings(path: str):
    with open(path) as f:
        return msgspec.toml.decode(f.read(), type=Settings)

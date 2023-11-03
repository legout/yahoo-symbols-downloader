import asyncio
import os
from redbird.repos import CSVFileRepo, SQLRepo
import sqlalchemy
from rocketry import Rocketry
from rocketry.log import RunRecord
from .run import run
from .settings import load_settings
from pydala.filesystem import FileSystem
import msgspec
from pathlib import Path

# load SETTINGS

SETTINGS = load_settings(Path(__file__).parent / "config/settings.yml")

if SETTINGS.storage.type.lower() == "s3":
    FS = FileSystem(
        protocol="s3",
        key=SETTINGS.storage.s3.key,
        secret=SETTINGS.storage.s3.secret,
        endpoint_url=SETTINGS.storage.s3.endpoint_url,
        profile=SETTINGS.storage.s3.profile,
        bucket=SETTINGS.storage.s3.bucket,
    )
    STORAGE_PATH = SETTINGS.storage.s3.path

elif SETTINGS.storage.type.lower() == "local":
    FS = FileSystem(
        protocol="file",
        bucket=SETTINGS.storage.local.bucket,
    )
    STORAGE_PATH = SETTINGS.storage.local.path

else:
    FS = None
    STORAGE_PATH = SETTINGS.storage.sqlite.path


DOWNLOAD_ARGS = msgspec.structs.asdict(SETTINGS.parameters.download)

# logging
if SETTINGS.schedule.logging.repo == "csv":
    repo = CSVFileRepo(
        filename=os.path.join(
            SETTINGS.schedule.logging.path, SETTINGS.schedule.logging.filename + ".csv"
        ),
        model=RunRecord,
    )

elif SETTINGS.schedule.logging.repo == "sqlite":
    engine = sqlalchemy.create_engine(
        f"sqlite:///{os.path.join(SETTINGS.schedule.logging.path, SETTINGS.schedule.logging.filename+'.db')}"
    )
    engine.execute(
        """CREATE TABLE log (
        id INTEGER PRIMARY KEY,
        created FLOAT,
        task_name TEXT,
        run_id TEXT,
        action TEXT
    )"""
    )
    repo = SQLRepo(model=RunRecord, table="log", engine=engine, id_field="id")

# init app
app = Rocketry(execution="async", logger_repo=repo)


@app.task(SETTINGS.parameters.schedule.cron)
async def update_symbol_info():
    await run(
        types=SETTINGS.parameters.run.types,
        query_length=SETTINGS.parameters.run.query_length,
        batch_size=SETTINGS.parameters.run.batch_size,
        storage_type=SETTINGS.storage.type,
        storage_path=STORAGE_PATH,
        s3_bucket=SETTINGS.storage.s3.bucket,
        s3_profile=SETTINGS.storage.s3.profile,
        **DOWNLOAD_ARGS,
    )


async def main():
    "Launch Rocketry app (and possibly something else)"
    rocketry_task = asyncio.create_task(app.serve())
    # Start possibly other async apps
    await rocketry_task


if __name__ == "__main__":
    asyncio.run(main())

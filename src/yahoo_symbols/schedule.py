import asyncio
import os
from redbird.repos import CSVFileRepo, SQLRepo
import sqlalchemy
from rocketry import Rocketry
from rocketry.log import RunRecord
from run import run
from settings import SETTINGS

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


app = Rocketry(execution="async", logger_repo=repo)


@app.task(SETTINGS.parameters.schedule.cron)
async def update_symbol_info():
    await run()


async def main():
    "Launch Rocketry app (and possibly something else)"
    rocketry_task = asyncio.create_task(app.serve())
    # Start possibly other async apps
    await rocketry_task


if __name__ == "__main__":
    asyncio.run(main())

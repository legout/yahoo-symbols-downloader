import os
from pathlib import Path
import asyncio
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from pydala.helpers.polars_ext import pl

from .constants import TYPES
from .helpers import (
    download,
    gen_lookup_queries,
    get_parquet_dataset,
    run_sqlite_query,
    save,
    settings_to_kwargs,
)
from .settings import load_settings
#from .utils import AsyncTyper
from typer import Typer
app = Typer()


@app.command()
def export(
    export_path: str,
    types: str = None,
    to: str = "csv",
    storage_path: str = "yahoo-symbols",
    storage_type: str = "s3",
    s3_profile: str = "default",
    s3_bucket: str = None,
):
    """
    Export data to a specified format.

    Args:
        export_path (str): The path where the exported data will be saved.
        types (str, optional): The types of data to export. Defaults to None.
        to (str, optional): The format to export the data in. Valid options are "csv", "excel", "xlsx" or "json".
            Defaults to "csv".
        storage_path (str, optional): The path to the storage location. Defaults to "yahoo-symbols".
        storage_type (str, optional): The type of storage. Defaults to "s3".
        s3_profile (str, optional): The S3 profile to use. Defaults to "default".
        s3_bucket (str | None, optional): The S3 bucket to use. Defaults to None.

    Returns:
        None
    """

    types = types or TYPES
    if isinstance(types, str):
        types = types.split(",")

    if os.path.isfile(storage_path):
        ds = None
    else:
        ds = get_parquet_dataset(
            storage_path=storage_path,
            storage_type=storage_type,
            s3_profile=s3_profile,
            s3_bucket=s3_bucket,
        )

    for type_ in types:
        logger.info("Exporting data of type {}".format(type_))

        if ds is not None:
            df = pl.from_arrow(ds.filter(f"type={type_}").to_table())
        else:
            df = run_sqlite_query(f"SELECT * FROM {type_}", storage_path=storage_path)

        if to == "csv":
            path_ = os.path.join(os.path.splitext(export_path)[0], f"{type_}.csv")
            df.write_csv(path_)
        elif to in ["xlsx", "excel"]:
            path_ = os.path.join(os.path.splitext(export_path)[0], "yahoo-symbols.xlsx")
            df.write_excel(path_, worksheet=type_)
        elif to == "json":
            path_ = os.path.join(os.path.splitext(export_path)[0], f"{type_}.json")
            df.write_json(path_, pretty=True, row_oriented=True)

        logger.info("Finished exporting data fo type {}".format(type_))

    logger.success("Finished exporting data to {}".format(export_path))


#@app.command()
async def run_(
    settings: str = None,
    types: str = None,
    query_length: int = "2",
    batch_size: int = 1000,
    storage_path: str = "yahoo-symbols",
    storage_type: str = "s3",
    s3_profile: str = None,
    s3_bucket: str = None,
    random_proxy: bool = False,
    random_user_agent: bool = True,
    concurrency: int = 10,
    max_retries: int = 5,
    random_delay_multiplier: int = 10,
    proxies: str = None,
    debug: bool = False,
    verbose: bool = False,
    warnings: bool = False,
    log_path: str = None,
):
    """
    Asynchronous function that runs a series of queries on a given type of data.

    Args:
        types (str): The type of data to run queries on. Defaults to None.
        query_length (int): The length of the queries. Defaults to "2".
        batch_size (int): The number of queries to run in each batch. Defaults to 1000.
        storage_path (str): The path to store the results. Defaults to "yahoo-symbols".
        storage_type (str, optional): The type of storage to use. Valid options are: "s3", "local" or "sqlite".
            Defaults to "s3".
        s3_profile (str, optional): The S3 profile to use if storage_type is "s3". It is neccessary to define
            your profile in ~/.aws/credentials. Defaults to "default".
        s3_bucket (str | None): The S3 bucket to use. Defaults to None.
        random_proxy (bool): Whether to use a random proxy. Defaults to False.
        random_user_agent (bool): Whether to use a random user agent. Defaults to True.
        concurrency (int): The number of queries to run concurrently. Defaults to 25.
        max_retries (int): The maximum number of retries. Defaults to 3.
        random_delay_multiplier (int): The multiplier for the random delay. Defaults to 5.

    Returns:
        None
    """

    if settings is not None:
        settings = load_settings(settings)
        kwargs = settings_to_kwargs(settings)

    else:
        kwargs = {
            "types": types,
            "query_length": query_length,
            "batch_size": batch_size,
            "storage_path": storage_path,
            "storage_type": storage_type,
            "s3_profile": s3_profile,
            "s3_bucket": s3_bucket,
            "random_proxy": random_proxy,
            "random_user_agent": random_user_agent,
            "concurrency": concurrency,
            "max_retries": max_retries,
            "random_delay_multiplier": random_delay_multiplier,
            "proxies": proxies,
            "debug": debug,
            "verbose": verbose,
            "warnings": warnings,
            "log_path": log_path,
        }

    if kwargs.get("log_path") is not None:
        if kwargs.get("log_path")[0] != "/":
            root_path = Path(__file__).parent.parent.resolve()
        else:
            root_path = ""
        log_path = os.path.join(root_path, kwargs.get("log_path"), "log.json")
        logger.info(f"Logging to {log_path}")

        logger.add(sink=log_path, serialize=True, rotation="1 month")

    lookup_queries = gen_lookup_queries(kwargs.get("query_length"))

    types = kwargs.get("types") or TYPES
    if isinstance(types, str):
        types = types.split(",")

    with logger.contextualize(
        types=types,
        query_length=kwargs.get("query_length"),
        batch_size=kwargs.get("batch_size"),
        concurrency=kwargs.get("concurrency"),
        random_proxy=kwargs.get("random_proxy"),
        random_user_agent=kwargs.get("random_user_agent"),
    ):
        logger.info(f"Starting with query_length: {kwargs.get('query_length')}, batch_size: {kwargs.get('batch_size')}, concurrency: {kwargs.get('concurrency')}, random_proxy: {kwargs.get('random_proxy')}, random_user_agent: {kwargs.get('random_user_agent')}")

    for type_ in types:
        batches = len(lookup_queries) // kwargs.get("batch_size") + 1
        for n in range(batches):
            with logger.contextualize(type=type_, batch=n + 1):
                logger.info(f"Processing type {type_} batch {n + 1}/{batches}")

            _lookup_queries = lookup_queries[n * batch_size : (n + 1) * batch_size]

            df = await download(
                _lookup_queries,
                type_=type_,
                storage_path=kwargs.get("storage_path"),
                storage_type=kwargs.get("storage_type"),
                s3_profile=kwargs.get("s3_profile"),
                s3_bucket=kwargs.get("s3_bucket"),
                concurrency=kwargs.get("concurrency"),
                random_proxy=kwargs.get("random_proxy"),
                random_user_agent=kwargs.get("random_user_agent"),
                max_retries=kwargs.get("max_retries"),
                random_delay_multiplier=kwargs.get("random_delay_multiplier"),
                debug=kwargs.get("debug"),
                verbose=kwargs.get("verbose"),
                proxies=kwargs.get("proxies"),
                warnings=kwargs.get("warnings"),
            )
            if df is None:
                logger.info(f"Found {df.shape[0]} new data points.")
                if df.shape[0] != 0:
                    await save(
                        df,
                        type_=type_,
                        storage_path=kwargs.get("storage_path"),
                        storage_type=kwargs.get("storage_type"),
                        s3_profile=kwargs.get("s3_profile"),
                        s3_bucket=kwargs.get("s3_bucket"),
                    )
            with logger.contextualize(
                type=type_,
                batch=n + 1,
                lookup_queries=len(_lookup_queries),
                new_data=df.shape[0] if df is not None else -1,
            ):
                logger.success(f"Finished type {type_} batch {n + 1}/{batches}")

    logger.success("Finished")

@app.command()
def run(
    settings: str = None,
    types: str = None,
    query_length: int = "2",
    batch_size: int = 1000,
    storage_path: str = "yahoo-symbols",
    storage_type: str = "s3",
    s3_profile: str = None,
    s3_bucket: str = None,
    random_proxy: bool = False,
    random_user_agent: bool = True,
    concurrency: int = 10,
    max_retries: int = 5,
    random_delay_multiplier: int = 10,
    proxies: str = None,
    debug: bool = False,
    verbose: bool = False,
    warnings: bool = False,
    log_path: str = None,
):
    """
    Function that runs a series of queries on a given type of data.

    Args:
        types (str): The type of data to run queries on. Defaults to None.
        query_length (int): The length of the queries. Defaults to "2".
        batch_size (int): The number of queries to run in each batch. Defaults to 1000.
        storage_path (str): The path to store the results. Defaults to "yahoo-symbols".
        storage_type (str, optional): The type of storage to use. Valid options are: "s3", "local" or "sqlite".
            Defaults to "s3".
        s3_profile (str, optional): The S3 profile to use if storage_type is "s3". It is neccessary to define
            your profile in ~/.aws/credentials. Defaults to "default".
        s3_bucket (str | None): The S3 bucket to use. Defaults to None.
        random_proxy (bool): Whether to use a random proxy. Defaults to False.
        random_user_agent (bool): Whether to use a random user agent. Defaults to True.
        concurrency (int): The number of queries to run concurrently. Defaults to 25.
        max_retries (int): The maximum number of retries. Defaults to 3.
        random_delay_multiplier (int): The multiplier for the random delay. Defaults to 5.

    Returns:
        None
    """
    
    asyncio.run(run_(
        settings=settings,
        types=types,
        query_length=query_length,
        batch_size=batch_size,
        storage_path=storage_path,
        storage_type=storage_type,
        s3_profile=s3_profile,
        s3_bucket=s3_bucket,
        random_proxy=random_proxy,
        random_user_agent=random_user_agent,
        concurrency=concurrency,
        max_retries=max_retries,
        random_delay_multiplier=random_delay_multiplier,
        proxies=proxies,
        debug=debug,
        verbose=verbose,
        warnings=warnings,
        log_path=log_path,
    ))

@app.command()
def start_scheduler(
    settings: str = None,
    cron: str = None,
    types: str = None,
    query_length: int = "2",
    batch_size: int = 1000,
    storage_path: str = "yahoo-symbols",
    storage_type: str = "s3",
    s3_profile: str = None,
    s3_bucket: str = None,
    random_proxy: bool = False,
    random_user_agent: bool = True,
    concurrency: int = 10,
    max_retries: int = 5,
    random_delay_multiplier: int = 10,
    proxies: str = None,
    debug: bool = False,
    verbose: bool = False,
    warnings: bool = False,
    log_path: str = None,
):
    if settings is not None:
        settings = load_settings(settings)
        kwargs = settings_to_kwargs(settings)

    else:
        kwargs = {
            "types": types,
            "query_length": query_length,
            "batch_size": batch_size,
            "storage_path": storage_path,
            "storage_type": storage_type,
            "s3_profile": s3_profile,
            "s3_bucket": s3_bucket,
            "random_proxy": random_proxy,
            "random_user_agent": random_user_agent,
            "concurrency": concurrency,
            "max_retries": max_retries,
            "random_delay_multiplier": random_delay_multiplier,
            "proxies": proxies,
            "debug": debug,
            "verbose": verbose,
            "warnings": warnings,
            "log_path": log_path,
            "cron": cron,
        }

    scheduler = BlockingScheduler()
    cron = kwargs.pop("cron")
    scheduler.add_job(
        run,
        trigger=CronTrigger.from_crontab(cron),
        kwargs=kwargs,
    )
    logger.info(f"Starting scheduler with cron {cron}")
    scheduler.start()
    
    #return scheduler


if __name__ == "__main__":
    app()

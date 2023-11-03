import datetime as dt
import os
from itertools import product
from loguru import logger
from pydala.dataset import ParquetDataset
from pydala.filesystem import FileSystem
from pydala.helpers.polars_ext import pl
from yfin.symbols import validate_async
from .constants import TYPES, SAMPLES

from .helpers import (
    get_quote_summary,
    get_quotes,
    get_lookup,
    get_new_symbols,
    run_sqlite_query,
    get_parquet_dataset,
)
from .utils import AsyncTyper

app = AsyncTyper()


@app.command()
async def download(
    lookup_queries: str,  # | list[str],
    type_: str,
    storage_path: str = "yahoo-symbols",
    storage_type: str = "s3",
    s3_profile: str = "default",
    s3_bucket: str = None,
    random_proxy: bool = False,
    random_user_agent: bool = True,
    concurrency: int = 25,
    max_retries: int = 3,
    random_delay_multiplier: int = 5,
):
    """
    Downloads data from a specified source based on the given lookup queries and type.

    Args:
        lookup_queries (str | list[str]): The lookup queries used to retrieve the data.
        type_ (str): The type of data to download.
        storage_path (str, optional): The path where the downloaded data will be stored. Defaults to "yahoo-symbols".
        storage_type (str, optional): The type of storage to use. Valid options are: "s3", "local" or "sqlite".
            Defaults to "s3".
        s3_profile (str, optional): The S3 profile to use if storage_type is "s3". It is neccessary to define
            your profile in ~/.aws/credentials. Defaults to "default".
        s3_bucket (str | None, optional): The S3 bucket to use if storage_type is "s3". Defaults to None.


    Returns:
        None
    """

    if isinstance(lookup_queries, str):
        lookup_queries = lookup_queries.split(",")
    
    logger.info(f"Processing queries: {lookup_queries[0]} - {lookup_queries[-1]}")
    lu_res = await get_lookup(
        lookup_query=lookup_queries,
        type_=type_,
        random_proxy=random_proxy,
        random_user_agent=random_user_agent,
        random_delay_multiplier=random_delay_multiplier,
        concurrency=concurrency,
        max_retries=max_retries,
    )

    symbols = sorted(set(lu_res["symbol"]))
    new_symbols = get_new_symbols(
        symbols=symbols,
        type_=type_,
        storage_path=storage_path,
        storage_type=storage_type,
        s3_profile=s3_profile,
        s3_bucket=s3_bucket,
    )
    if len(new_symbols) == 0:
        logger.success("No new symbols found. Finished")
        return
    valid_symbols = await validate_async(new_symbols)
    valid_symbols = sorted(set(valid_symbols.query("valid")["symbol"]))
    
    if len(valid_symbols) == 0:
        logger.success("No new valid symbols found. Finished")
        return
    
    logger.info(f"Found {len(valid_symbols)} new valid symbols")
    
    summary_profile, quote_type = await get_quote_summary(
        symbols=valid_symbols,
        random_proxy=random_proxy,
        random_user_agent=random_user_agent,
        random_delay_multiplier=random_delay_multiplier,
        concurrency=concurrency,
        max_retries=max_retries,
    )

    quotes = await get_quotes(
        valid_symbols,
        random_proxy=random_proxy,
        random_user_agent=random_user_agent,
        random_delay_multiplier=random_delay_multiplier,
        concurrency=concurrency,
        max_retries=max_retries,
    )

    df = (
        (
            quote_type.join(quotes, on=["symbol", "exchange", "type"], how="outer")
            .join(summary_profile, on="symbol", how="outer")
            .unique()
            .sort("symbol")
        )
        .filter(pl.col("type") == type_)
        .with_columns(pl.lit(dt.date.today()).alias("added"))
    )
    logger.info(f"Downloaded {df.shape[0]} new {type_} symbols")
    
    if "sqlite" in str(storage_type):
        df_existing = run_sqlite_query(
            f"SELECT DISTINCT symbol FROM {type_}", storage_path=storage_path
        )
        delta_df = df.delta(
            df_existing,
            subset=[
                "symbol",
                "exchange",
                "type",
                "short_name",
                "long_name",
                "market",
                "underlying_symbol",
            ],
        )
        if delta_df.shape[0]:
            delta_df.write_database(
                name=type_,
                connection=f"sqlite3:///{storage_path}",
                if_exists="append",
            )
        
    else:
        if os.path.isfile(storage_path):
            storage_path = os.path.splitext(storage_path)[0]
        ds = get_parquet_dataset(
            storage_path=storage_path,
            storage_type=storage_type,
            s3_profile=s3_profile,
            s3_bucket=s3_bucket,
        )
        ds.write_to_dataset(
            df=df,
            mode="delta",
            num_rows=1_000_000,
            row_group_size=100_000,
            compression="zstd",
            sort_by=["exchange", "symbol"],
            partitioning_columns=["type", "market", "exchange"],
            unique=True,
            delta_subset=[
                "symbol",
                "exchange",
                "type",
                "short_name",
                "long_name",
                "market",
                "underlying_symbol",
            ],
            delta_other_df_filter_columns=[
                "symbol",
                "exchange",
                "type",
                "short_name",
                "long_name",
                "market",
                "underlying_symbol",
            ],
            on="parquet_dataset",
            use="pyarrow",
        )
    logger.success(f"Finished processing query: {lookup_queries[0]} - {lookup_queries[-1]}")

    # return df, ds


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
        logger.info(f"Exporting {type_} data")
        if ds is not None:
            df = pl.from_arrow(ds.filter(f"type={type_}").to_table())
        else:
            df = run_sqlite_query(
                f"SELECT * FROM {type_}", storage_path=storage_path
            )

        if to == "csv":
            path_ = os.path.join(os.path.splitext(export_path)[0], f"{type_}.csv")
            df.write_csv(path_)
        elif to in ["xlsx", "excel"]:
            path_ = os.path.join(os.path.splitext(export_path)[0], "yahoo-symbols.xlsx")
            df.write_excel(path_, worksheet=type_)
        elif to == "json":
            path_ = os.path.join(os.path.splitext(export_path)[0], f"{type_}.json")
            df.write_json(path_, pretty=True, row_oriented=True)
        logger.info(f"Finished exporting {type_} data")
    logger.success(f"Finished exporting data to {export_path}")

@app.command()
async def run(
    types: str = None,
    query_length: int = "2",
    batch_size: int = 1000,
    storage_path: str = "yahoo-symbols",
    storage_type: str = "s3",
    s3_profile: str = "default",
    s3_bucket: str = None,
    random_proxy: bool = False,
    random_user_agent: bool = True,
    concurrency: int = 25,
    max_retries: int = 3,
    random_delay_multiplier: int = 5,
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

    lookup_queries = [
        "".join(q)
        for ql in range(1, query_length + 1)
        for q in list(product(*[SAMPLES for n in range(ql)]))
    ]

    types = types or TYPES
    if isinstance(types, str):
        types = types.split(",")

    logger.info(
        f"Starting: types={types}, query_length={query_length}, batch_size={batch_size}, concurrency={concurrency}, "
        + f"random_proxy={random_proxy}, random_user_agent={random_user_agent}"
    )
    for type_ in types:
        logger.info(f"Starting type: {type_}")
        for n in range(len(lookup_queries) // batch_size + 1):
            logger.info(f"Starting batch: {n}")
            _lookup_queries = lookup_queries[n * batch_size : (n + 1) * batch_size]

            await download(
                _lookup_queries,
                type_,
                storage_path=storage_path,
                storage_type=storage_type,
                s3_profile=s3_profile,
                s3_bucket=s3_bucket,
                concurrency=concurrency,
                random_proxy=random_proxy,
                random_user_agent=random_user_agent,
                max_retries=max_retries,
                random_delay_multiplier=random_delay_multiplier,
            )
            logger.success(f"Batch {n} completed")
        logger.success(f"Completed type: {type_}")
    logger.success("Completed")


if __name__ == "__main__":
    app()

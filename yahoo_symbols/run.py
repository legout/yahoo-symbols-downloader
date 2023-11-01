import datetime as dt
import os
from itertools import product

from loguru import logger
from pydala.dataset import ParquetDataset
from pydala.filesystem import FileSystem
from pydala.helpers.polars_ext import pl
from yfin.quote_summary import quote_summary_async
from yfin.quotes import quotes_async
from yfin.symbols import lookup_search_async, validate_async
from .constants import TYPES, SAMPLES

from .utils import AsyncTyper, repeat_until_completed

app = AsyncTyper()


async def get_lookup(
    lookup_query: list[str],
    type_: str,
    *args,
    **kwargs,
) -> pl.DataFrame:
    """
    This function performs a lookup search using the given query and type, and returns a DataFrame
    containing the symbol, name, exchange, and type of the results.

    Args:
        lookup_query (list[str]): A list of strings representing the search query.
        type_ (str): The type of search to perform.
        *args: Additional positional arguments to pass to the lookup_search_async function.
        **kwargs: Additional keyword arguments to pass to the lookup_search_async function.

    Returns:
        pl.DataFrame: A DataFrame containing the symbol, name, exchange, and type of the results.
    """
    res = await lookup_search_async(
        query=lookup_query,
        type_=type_,
        *args,
        **kwargs,
    )

    if res.shape[0] > 0:
        res = pl.from_pandas(res)
        renames = {
            k: v
            for k, v in {
                "shortName": "name",
                "quoteType": "type",
            }.items()
            if k in res.columns
        }

        res = res.rename(renames).group_by(["symbol", "exchange"]).agg(pl.all().first())
        res = res[
            [
                col
                for col in res.columns
                if col in ["symbol", "name", "exchange", "type"]
            ]
        ]
        return res


async def get_quote_summary(symbols: list[str], *args, **kwargs)->tuple[pl.DataFrame, pl.DataFrame]:
    """
    Retrieves summary profile and quote type information for a list of symbols.

    Args:
        symbols (list[str]): A list of symbols to retrieve information for.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.

    Returns:
        Tuple[pl.DataFrame, pl.DataFrame]: A tuple containing two pandas.DataFrame objects, one for summary profile and one for quote type information.
    """
    res = await repeat_until_completed(
        func=quote_summary_async,
        symbols=symbols,
        modules=["summary_profile", "quote_type"],
        *args,
        **kwargs,
    )

    summary_profile = res["summary_profile"]
    if summary_profile.shape[0]:
        summary_profile = pl.from_pandas(summary_profile).drop(
            [
                col
                for col in [
                    "company_officers",
                    "industry_dips",
                    "first_trade_date_epoch_utc",
                ]
                if col in summary_profile.columns
            ]
        )
    else:
        summary_profile = pl.DataFrame({"symbol": []}).with_columns(
            pl.col("symbol").cast(pl.Utf8())
        )

    quote_type = res["quote_type"]
    if quote_type.shape[0]:
        quote_type = (
            pl.from_pandas(quote_type)
            .rename(
                {
                    "quote_type": "type",
                    "time_zone_full_name": "timezone",
                    "first_trade_date_epoch_utc": "first_trade_date",
                }
            )
            .with_columns(pl.col("type").str.to_lowercase())
            .drop(
                [
                    col
                    for col in [
                        "uuid",
                        "message_board_id",
                        "time_zone_short_name",
                    ]
                    if col in quote_type.columns
                ]
            )
        )
    else:
        quote_type = pl.DataFrame({"symbol": [], "exchange": []}).with_columns(
            pl.col(["symbol", "exchange"]).cast(pl.Utf8())
        )

    return summary_profile, quote_type


async def get_quotes(symbols: list[str], *args, **kwargs)->pl.DataFrame:
    """
    Retrieves quotes for a list of symbols from Yahoo Finance.

    Args:
        symbols (list[str]): A list of symbols to retrieve quotes for.
        *args: Additional arguments to pass to the `quotes_async` function.
        **kwargs: Additional keyword arguments to pass to the `quotes_async` function.

    Returns:
        pl.DataFrame: A polars DataFrame containing the retrieved quotes.
    """
    quotes = await repeat_until_completed(
        func=quotes_async,
        symbols=symbols,
        chunk_size=500,
        fields=["twoHundredDayAverageChangePercent", "currency"],
        *args,
        **kwargs,
    )

    quotes = pl.from_pandas(quotes)

    if quotes.shape[0]:
        quotes = (
            quotes.select(
                [
                    col
                    for col in [
                        "currency",
                        "exchange",
                        "market",
                        "exchange_data_delayed_by",
                        "full_exchange_name",
                        "symbol",
                        "quote_type",
                    ]
                    if col in quotes.columns
                ]
            )
            .rename(
                {
                    "quote_type": "type",
                    "full_exchange_name": "exchange_name",
                    "exchange_data_delayed_by": "exchange_data_delay",
                }
            )
            .with_columns(pl.col("type").str.to_lowercase())
        )
    else:
        quotes = pl.DataFrame({"symbol": [], "exchange": []}).with_columns(
            pl.col(["symbol", "exchange"]).cast(pl.Utf8())
        )

    return quotes


@app.command()
async def download(
    lookup_queries: str,  # | list[str],
    type_: str,
    storage_path: str = "yahoo-symbols",
    storage_type: str = "s3",
    s3_profile: str = "default",
    s3_bucket: str  = None,
    **download_args,
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
        **download_args: Additional arguments to be passed to the download function.

    Returns:
        None
    """

    if isinstance(lookup_queries, str):
        lookup_queries = lookup_queries.split(",")
    lu_res = await get_lookup(lookup_query=lookup_queries, type_=type_, **download_args)

    symbols = sorted(set(lu_res["symbol"]))

    valid_symbols = await validate_async(symbols)
    valid_symbols = sorted(set(valid_symbols.query("valid")["symbol"]))

    summary_profile, quote_type = await get_quote_summary(
        symbols=valid_symbols, **download_args
    )

    quotes = await get_quotes(valid_symbols, **download_args)

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

    if "sqlite" not in storage_type.lower():
        if os.path.isfile(storage_path):
            storage_path = os.path.splitext(storage_path)[0]
        ds = ParquetDataset(
            path=storage_path,
            partitioning="hive",
            filesystem=FileSystem(
                protocol="s3" if storage_type == "s3" else "file",
                profile=s3_profile if storage_type == "s3" else None,
                bucket=s3_bucket if storage_type == "s3" else None,
            ),
        )
        ds.load(update_metadata=True)
        ds.write_to_dataset(
            df=df,
            mode="delta",
            num_rows=1_000_000,
            row_group_size=250_000,
            compression="zstd",
            sort_by=["exchange", "symbol"],
            partitioning_columns=["type"],
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
    else:
        if os.path.exists(storage_path):
            if not os.path.isfile(storage_path):
                storage_path = os.path.join(storage_path, "yahoo-symbols.sqlite")
        else:
            if "." in storage_path:
                os.makedirs(os.path.dirname(storage_path), exist_ok=True)
            else:
                os.makedirs(storage_path, exist_ok=True)
                storage_path = os.path.join(storage_path, "yahoo-symbols.sqlite")

        delta_df = df.delta(
            pl.read_database_uri(
                f"SELECT * FROM {type_}",
                uri=f"sqlite3:///{storage_path}",
            ),
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

    #return df, ds


@app.command()
def export(
    export_path: str,
    types: str = None,
    to: str = "csv",
    storage_path: str = "yahoo-symbols",
    storage_type: str = "s3",
    s3_profile: str = "default",
    s3_bucket: str  = None,
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
        ds = ParquetDataset(
            path=storage_path,
            partitioning="hive",
            filesystem=FileSystem(
                protocol="s3" if storage_type == "s3" else "file",
                profile=s3_profile if storage_type == "s3" else None,
                bucket=s3_bucket if storage_type == "s3" else None,
            ),
        )
        ds.load(update_metadata=True)

    for type_ in types:
        if ds is not None:
            df = pl.from_arrow(ds.filter(f"type={type_}").to_table())
        else:
            df = pl.read_database_uri(
                f"SELECT * FROM {type_}",
                uri=f"sqlite3:///{storage_path}",
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

            df, ds = await download(
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

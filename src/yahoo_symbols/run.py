import datetime as dt
import os
from itertools import product
from string import ascii_lowercase, digits

import msgspec
from loguru import logger
from pydala.dataset import ParquetDataset
from pydala.filesystem import FileSystem
from pydala.helpers.polars_ext import pl
from yfin.quote_summary import quote_summary_async
from yfin.quotes import quotes_async
from yfin.symbols import lookup_search_async, validate_async

from .settings import SETTINGS
from .utils import AsyncTyper, repeat_until_completed

app = AsyncTyper()

if SETTINGS.storage.type.lower() == "s3":
    FS = FileSystem(
        protocol="s3",
        key=SETTINGS.storage.s3.key,
        secret=SETTINGS.storage.s3.secret,
        endpoint_url=SETTINGS.storage.s3.endpoint_url,
        profile=SETTINGS.storage.s3.profile,
        bucket=SETTINGS.storage.s3.bucket,
    )

elif SETTINGS.storage.type.lower() == "local":
    FS = FileSystem(
        protocol="file",
        bucket=SETTINGS.storage.local.bucket,
    )

else:
    FS = None

DOWNLOAD_ARGS = msgspec.structs.asdict(SETTINGS.parameters.download)


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


async def get_quote_summary(symbols: list[str], *args, **kwargs):
    """
    Retrieves summary profile and quote type information for a list of symbols.

    Args:
        symbols (list[str]): A list of symbols to retrieve information for.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.

    Returns:
        Tuple: A tuple containing two pandas.DataFrame objects, one for summary profile and one for quote type information.
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


async def get_quotes(symbols: list[str], chunk_size: int = 750, *args, **kwargs):
    """
    Retrieves quotes for a list of symbols from Yahoo Finance.

    Args:
        symbols (list[str]): A list of symbols to retrieve quotes for.
        chunk_size (int, optional): The number of symbols to retrieve quotes for at a time. Defaults to 750.
        *args: Additional arguments to pass to the `quotes_async` function.
        **kwargs: Additional keyword arguments to pass to the `quotes_async` function.

    Returns:
        A polars DataFrame containing the retrieved quotes.
    """
    quotes = await repeat_until_completed(
        func=quotes_async,
        symbols=symbols,
        chunk_size=chunk_size,
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
async def get_all(
    lookup_queries: str,  # | list[str],
    type_: str,
):
    """
    Retrieves data for a given set of lookup queries and type.

    Args:
        lookup_queries (str | list[str]): The lookup queries to retrieve data for.
        type_ (str): The type of data to retrieve.

    Returns:
        Tuple[DataFrame, ParquetDataset]: A tuple containing the retrieved data as a DataFrame and the ParquetDataset.
    """
    if isinstance(lookup_queries, str):
        lookup_queries = lookup_queries.split(",")
    lu_res = await get_lookup(lookup_query=lookup_queries, type_=type_, **DOWNLOAD_ARGS)

    symbols = sorted(set(lu_res["symbol"]))

    valid_symbols = await validate_async(symbols)
    valid_symbols = sorted(set(valid_symbols.query("valid")["symbol"]))

    summary_profile, quote_type = await get_quote_summary(
        symbols=valid_symbols, **DOWNLOAD_ARGS
    )

    quotes = await get_quotes(
        valid_symbols, SETTINGS.parameters.run.chunk_size, **DOWNLOAD_ARGS
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

    if FS is not None:
        ds = ParquetDataset(
            path=SETTINGS.storage.s3.path
            if SETTINGS.storage.type.lower() == "s3"
            else SETTINGS.storage.local.path,
            partitioning=SETTINGS.storage.s3.partitioning
            if SETTINGS.storage.type.lower() == "s3"
            else SETTINGS.storage.local.partitioning,
            filesystem=FS,
        )
        ds.load(update_metadata=True)
        ds.write_to_dataset(
            df=df,
            mode="delta",
            num_rows=1_000_000,
            row_group_size=250_000,
            compression="zstd",
            sort_by=SETTINGS.parameters.run.sort_by,
            partitioning_columns=SETTINGS.parameters.run.partitioning_columns,
            unique=True,
            delta_subset=SETTINGS.parameters.run.delta_subset,
            delta_other_df_filter_columns=SETTINGS.parameters.run.delta_subset,
            on="parquet_dataset",
            use="pyarrow",
        )
    else:
        delta_df = df.delta(
            pl.read_database_uri(
                f"SELECT * FROM {type_}",
                uri=f"sqlite3:///{SETTINGS.storage.sqlite.path}",
            ),
            subset=SETTINGS.parameters.run.delta_subset,
        )
        if delta_df.shape[0]:
            delta_df.write_database(
                name=type_,
                connection=f"sqlite3:///{SETTINGS.storage.sqlite.path}",
                if_exists="append",
            )

    return df, ds


@app.command()
def export(path: str, types: str = None, to: str = "csv"):
    """
    Export data to a specified file format.

    Parameters:
        path (str): The path to the directory where the exported file will be saved.
        types (str, optional): The types of data to export. Defaults to None.
        to (str, optional): The file format to export the data to. Defaults to "csv".

    Returns:
        None
    """
    types = types or SETTINGS.parameters.run.types
    if isinstance(types, str):
        types = types.split(",")

    if FS is not None:
        ds = ParquetDataset(
            path=SETTINGS.storage.s3.path
            if SETTINGS.storage.type.lower() == "s3"
            else SETTINGS.storage.local.path,
            partitioning=SETTINGS.storage.s3.partitioning
            if SETTINGS.storage.type.lower() == "s3"
            else SETTINGS.storage.local.partitioning,
            filesystem=FS,
        )
        ds.load(update_metadata=True)

    else:
        ds = None
    for type_ in types:
        if ds is not None:
            df = pl.from_arrow(ds.filter(f"type={type_}").to_table())
        else:
            df = pl.read_database_uri(
                f"SELECT * FROM {type_}",
                uri=f"sqlite3:///{SETTINGS.storage.sqlite.path}",
            )

        if to == "csv":
            path_ = os.path.join(path, f"{type_}.csv")
            df.write_csv(path_)
        elif to == "excel":
            path_ = os.path.join(path, "yahoo-symbols.xlsx")
            df.write_excel(path_, worksheet=type_)


@app.command()
async def run(
    types: str = None,
    query_length: int = None,
    concurrency: int = None,
    chunk_size: int = None,
    sort_by: str = None,
    partitioning_columns: list[str] = None,
    random_proxy: bool = None,
    random_user_agent: bool = None,
    max_retries: int = None,
    random_delay_multiplier: int = None,
):
    """
    Run the specified types of queries with the given parameters.

    Parameters:
        types (str): The types of queries to run. Default is None.
        query_length (int): The length of each query. Default is None.
        concurrency (int): The number of queries to run concurrently. Default is None.
        chunk_size (int): The size of each chunk. Default is None.
        sort_by (str): The field to sort the results by. Default is None.
        partitioning_columns (list[str]): The columns to partition the results by. Default is None.
        random_proxy (bool): Whether to use a random proxy. Default is None.
        random_user_agent (bool): Whether to use a random user agent. Default is None.
        max_retries (int): The maximum number of retries. Default is None.
        random_delay_multiplier (int): The multiplier for random delay. Default is None.

    Returns:
        None
    """
    SETTINGS.parameters.run.types = types or SETTINGS.parameters.run.types
    SETTINGS.parameters.run.query_length = (
        query_length or SETTINGS.parameters.run.query_length
    )
    SETTINGS.parameters.run.concurrency = (
        concurrency or SETTINGS.parameters.run.concurrency
    )
    SETTINGS.parameters.run.chunk_size = (
        chunk_size or SETTINGS.parameters.run.chunk_size
    )
    SETTINGS.parameters.run.sort_by = sort_by or SETTINGS.parameters.run.sort_by
    SETTINGS.parameters.run.partitioning_columns = (
        partitioning_columns or SETTINGS.parameters.run.partitioning_columns
    )
    SETTINGS.parameters.download.random_proxy = (
        random_proxy or SETTINGS.parameters.download.random_proxy
    )
    SETTINGS.parameters.download.random_user_agent = (
        random_user_agent or SETTINGS.parameters.download.random_user_agent
    )
    SETTINGS.parameters.download.max_retries = (
        max_retries or SETTINGS.parameters.download.max_retries
    )
    SETTINGS.parameters.download.random_delay_multiplier = (
        random_delay_multiplier or SETTINGS.parameters.download.random_delay_multiplier
    )

    letters = list(ascii_lowercase)
    numbers = list(digits)
    samples = letters + numbers + [".", "-"]

    lookup_queries = [
        "".join(q)
        for ql in range(1, query_length + 1)
        for q in list(product(*[samples for n in range(ql)]))
    ]

    if isinstance(SETTINGS.parameters.run.types, str):
        types = SETTINGS.parameters.run.types.split(",")
    else:
        types = SETTINGS.parameters.run.types

    logger.info(
        f"Starting: types={types}, query_length={query_length}, concurrency={concurrency}, chunk_size={chunk_size}, sort_by={sort_by}, partitioning_columns={partitioning_columns}"
    )
    for type_ in types:
        logger.info(f"Starting type: {type_}")
        for n in range(len(lookup_queries) // SETTINGS.parameters.run.concurrency + 1):
            logger.info(f"Starting batch: {n}")
            _lookup_queries = lookup_queries[
                n * SETTINGS.parameters.run.concurrency : (n + 1)
                * SETTINGS.parameters.run.concurrency
            ]

            df, ds = await get_all(_lookup_queries, type_)
            logger.success(f"Batch {n} completed")
        logger.success(f"Completed type: {type_}")
    logger.success("Completed")


if __name__ == "__main__":
    app()

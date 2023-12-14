from yfin.quote_summary import quote_summary_async
from yfin.quotes import quotes_async
from yfin.symbols import lookup_search_async
from yfin.base import Session
import polars as pl
from itertools import product

from pydala.dataset import ParquetDataset
from pydala.filesystem import FileSystem
import os
import re
import sqlite3
from .utils import  repeat_until_completed
from .constants import TYPES, SAMPLES

def gen_lookup_queries(query_length: int=2, ):
    lookup_queries = [
        "".join(q)
        for ql in range(1, query_length + 1)
        for q in list(product(*[SAMPLES for n in range(ql)]))
    ]
    return lookup_queries

def get_parquet_dataset(
    storage_path: str, storage_type: str, s3_profile: str, s3_bucket: str
):
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

    return ds


def run_sqlite_query(sql: str, storage_path: str):
    if os.path.exists(storage_path):
        if not os.path.isfile(storage_path):
            storage_path = os.path.join(storage_path, "yahoo-symbols.sqlite")
    else:
        if "." in storage_path:
            os.makedirs(os.path.dirname(storage_path), exist_ok=True)
        else:
            os.makedirs(storage_path, exist_ok=True)
            storage_path = os.path.join(storage_path, "yahoo-symbols.sqlite")

    con = sqlite3.connect(storage_path)

    # check table exists
    table_name = re.findall(r"(?i)FROM\s+([\w]+\b)", sql)[0]
    if pl.read_database(query=f"PRAGMA table_info({table_name})", connection=con).shape[0] != 0:
        df= pl.read_database(query=sql, connection=con)
        con.close()
        return df
    else:
        con.close()


def get_new_symbols(
    symbols: list[str],
    type_: str,
    storage_path: str,
    storage_type: str,
    s3_profile: str,
    s3_bucket: str,
):
    if "sqlite" in storage_type:
        existing_symbols = run_sqlite_query(
            f"SELECT DISTINCT symbol FROM {type_}", storage_path=storage_path
        )
        if existing_symbols is not None:
            return sorted(set(symbols) - set(existing_symbols["symbol"]))

    else:
        ds = get_parquet_dataset(
            storage_path=storage_path,
            storage_type=storage_type,
            s3_profile=s3_profile,
            s3_bucket=s3_bucket,
        )
        return sorted(
            set(symbols)
            - set(ds.filter(f"type={type_}").to_table(columns=["symbol"])["symbol"].to_pylist())
        )

    return symbols


async def get_lookup(
    lookup_query: list[str],
    type_: str,
    session: Session|None = None,
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
        session=session,
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


async def get_quote_summary(
    symbols: list[str], session: Session|None = None, *args, **kwargs
) -> tuple[pl.DataFrame, pl.DataFrame]:
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
        session=session,
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


async def get_quotes(symbols: list[str], session: Session|None = None, *args, **kwargs) -> pl.DataFrame:
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
        session=session,
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

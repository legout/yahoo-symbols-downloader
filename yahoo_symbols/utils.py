import inspect
from functools import partial, wraps

# import anyio
import asyncer
import pandas as pd
import polars as pl
#import typer
from typer import Typer


async def repeat_until_completed(
    func: callable, symbols: list[str], max_retries: int = 5, *args, **kwargs
):
    """
    Asynchronously repeats a function until it returns a non-None value or the maximum number of retries is reached.

    Args:
        func (callable): A callable function to be repeated.
        symbols (list[str]): A list of symbols to be passed to the function.
        max_retries (int, optional): The maximum number of times to repeat the function, defaults to 5.
        *args: Additional positional arguments to be passed to the function.
        **kwargs: Additional keyword arguments to be passed to the function.

    Returns:
        Any: The result of the function after it returns a non-None value or the maximum number of retries is reached.
    """
    retries = 1
    res = None
    while len(symbols) and retries < max_retries:
        res_ = await func(symbols=symbols, *args, **kwargs)
        if res_ is not None:
            if isinstance(res_, dict):
                if res is None:
                    res = res_
                else:
                    for k in res_:
                        if isinstance(res[k], pl.DataFrame):
                            res[k] = pl.concat([res[k], res_[k]])
                        elif isinstance(res[k], pd.DataFrame):
                            res[k] = pd.concat([res[k], res_[k]])
                        else:
                            res[k].append(res_[k])
            else:
                if res is None:
                    res = res_
                else:
                    if isinstance(res, pl.DataFrame):
                        res = pl.concat([res, res_])
                    if isinstance(res, pd.DataFrame):
                        res = pd.concat([res, res_])
                    else:
                        res.append(res_)
            if "symbol" in res_:
                symbols = list(set(symbols) - set(res_["symbol"]))
            else:
                symbols = []
            retries += 1
        else:
            break
    return res


class AsyncTyper(Typer):
    """
    Decorates a function with the given decorator, but only if the function is not already a coroutine function.

    Args:
        decorator (function): The decorator to apply to the function.
        f (function): The function to decorate.

    Returns:
        function: The decorated function.
    """

    @staticmethod
    def maybe_run_async(decorator, f):
        if inspect.iscoroutinefunction(f):

            @wraps(f)
            def runner(*args, **kwargs):
                return asyncer.runnify(f)(*args, **kwargs)

            decorator(runner)
        else:
            decorator(f)
        return f

    def callback(self, *args, **kwargs):
        decorator = super().callback(*args, **kwargs)
        return partial(self.maybe_run_async, decorator)

    def command(self, *args, **kwargs):
        decorator = super().command(*args, **kwargs)
        return partial(self.maybe_run_async, decorator)



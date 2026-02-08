from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd


def make_parallel(single_func, THREAD_COUNT=5):
    """Wraps a function to execute it in parallel over an iterable.

    The wrapped function's first parameter is multiplexed: the returned
    parallel version accepts an iterable in its place and fans out calls
    across threads.

    Args:
        single_func: A function whose first argument will be parallelized.
        THREAD_COUNT: Maximum number of concurrent threads. Defaults to 5.

    Returns:
        A function that takes an iterable as its first argument and returns
        a flat list of all results.
    """
    def parallel_func(iterable, *args, **kwargs):
        futures = []
        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            for i in iterable:
                futures.append(executor.submit(single_func, i, *args, **kwargs))
            as_completed(futures)
        results = [val for future in futures for val in future.result()]
        return results

    return parallel_func


def date_range(start, end):
    """Generates a daily date range between two dates.

    Args:
        start: The start date.
        end: The end date.

    Returns:
        A pandas DatetimeIndex of daily dates.
    """
    return pd.date_range(start, end)


def month_range(start, end):
    """Generates a month-end date range that includes both start and end months.

    Args:
        start: The start date.
        end: The end date.

    Returns:
        A pandas DatetimeIndex of month-end dates covering the full range.
    """
    # This is kinda funky, but is stil simpler than other approaches
    # pandas won't generate a monthly date_range that includes Jan and Feb for Jan31-Feb1 e.g.
    # So we generate a daily date_range and then resample it down (summing 0s as a no-op in the process) so it aligns.
    dates = pd.date_range(start, end, freq="1D", inclusive="both")
    series = pd.Series(0, index=dates)
    months = series.resample("ME").sum().index
    return months

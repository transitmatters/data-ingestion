from datetime import date, datetime, timedelta
from typing import Tuple


def date_from_string(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def date_to_string(date):
    return date.strftime("%Y-%m-%d")


def index_by(items, key_getter):
    res = {}
    if isinstance(key_getter, str):
        key_getter_as_str = key_getter
        key_getter = lambda dict: dict[key_getter_as_str]  # noqa: E731
    for item in items:
        res[key_getter(item)] = item
    return res


def bucket_by(items, key_getter):
    res = {}
    if isinstance(key_getter, str):
        key_getter_as_str = key_getter
        key_getter = lambda dict: dict[key_getter_as_str]  # noqa: E731
    for item in items:
        key = key_getter(item)
        key_items = res.setdefault(key, [])
        key_items.append(item)
    return res


def get_ranges_of_same_value(items_dict):
    current_value = None
    current_keys = None
    sorted_items = sorted(items_dict.items(), key=lambda item: item[0])
    for key, value in sorted_items:
        if value == current_value:
            current_keys.append(key)
        else:
            if current_keys:
                yield current_keys, current_value
            current_keys = [key]
            current_value = value
    if current_keys:
        yield current_keys, current_value


def get_date_ranges_of_same_value(items_dict):
    for dates, value in get_ranges_of_same_value(items_dict):
        min_date = min(dates)
        max_date = max(dates)
        yield (min_date, max_date), value


def date_range(start_date: date, end_date: date):
    assert start_date <= end_date
    now = start_date
    while now <= end_date:
        yield now
        now = now + timedelta(days=1)


def date_range_contains(containing: Tuple[date, date], contained: Tuple[date, date]):
    (containing_from, containing_to) = containing
    (contained_from, contained_to) = contained
    return contained_from >= containing_from and contained_to <= containing_to

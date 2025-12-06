from typing import Dict, Union
import pandas as pd
import numpy as np
from pandas.tseries.holiday import USFederalHolidayCalendar
from tempfile import NamedTemporaryFile


unofficial_labels_map = {
    "SL1": "741",
    "SL2": "742",
    "SL3": "743",
    "SL4": "751",
    "SL5": "749",
    "SLW": "746",
    "Red Line": "Red",
    "Orange Line": "Orange",
    "Green Line": "Green",
    "Blue Line": "Blue",
}

unofficial_cr_labels_map = {
    # Commuter Rail
    "Fitchburg": "CR-Fitchburg",
    "Needham": "CR-Needham",
    "Greenbush": "CR-Greenbush",
    "Fairmount": "CR-Fairmount",
    "Providence/Stoughton": "CR-Providence",
    "Newburyport/Rockport": "CR-Newburyport",
    "Framingham/Worcester": "CR-Worcester",
    "Franklin/Foxboro": "CR-Franklin",
    "Middleborough/Lakeville": "CR-Middleborough",
    "Fall.River/New.Bedford": "CR-NewBedford",
    "Lowell": "CR-Lowell",
    "Haverhill": "CR-Haverhill",
    "Kingston": "CR-Kingston",
}

unofficial_ferry_labels_map = {
    # Ferry
    "F1": "Boat-F1",
    "F2H": "Boat-F1",
    "F3": "Boat-EastBoston",
    "F4": "Boat-F4",
    "F5": "Boat-Lynn",
    "F6": "Boat-F6",
    "F7": "Boat-F7",
    "F8": "Boat-F8",
    "Charlestown Ferry": "Boat-F4",
    "Hingham/Hull Ferry": "Boat-F1",
    "East Boston Ferry": "Boat-EastBoston",
    "Lynn Ferry": "Boat-Lynn",
    "Winthrop Ferry": "Boat-F6",
    "Quincy Ferry": "Boat-F7",
    "Winthrop/Quincy Ferry": "Boat-F8",
}


def pre_process_csv(
    path_to_csv_file: str,
    date_key: str,
    route_key: str | None,
    count_key: str,
    route_name: str | None = None,
):
    if route_key is None and route_name is not None:
        route_key = "Route"
        df = pd.read_csv(path_to_csv_file, usecols=[date_key, count_key])
        df[route_key] = route_name
    else:
        df = pd.read_csv(path_to_csv_file, usecols=[date_key, route_key, count_key])

    df[date_key] = pd.to_datetime(df[date_key], format="mixed", errors="coerce")
    df = df.dropna(subset=[date_key])
    df["Year"] = df[date_key].dt.year
    df["Week"] = df[date_key].dt.isocalendar().week
    df[date_key] = df[date_key].dt.strftime("%Y-%m-%d")

    grouped_df = df.groupby(["Year", "Week", route_key])[count_key].agg("sum").reset_index()
    grouped_df[date_key] = pd.to_datetime(
        grouped_df["Year"].astype(str) + grouped_df["Week"].astype(str) + "1", format="%Y%W%w"
    )
    tmp_path = NamedTemporaryFile().name
    grouped_df.to_csv(tmp_path, index=False)
    return tmp_path


def format_ridership_csv(
    path_to_csv_file: str,
    date_key: str,
    route_key: str,
    count_key: str,
    route_ids_map: Union[None, Dict[str, str]] = None,
):
    # read data, convert to datetime
    df = pd.read_csv(path_to_csv_file)
    df[date_key] = pd.to_datetime(df[date_key])

    # add holidays
    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(start=df[date_key].min(), end=df[date_key].max())

    # mark as holiday and weekday
    df["holiday"] = df[date_key].dt.date.astype("datetime64[ns]").isin(holidays.date)
    df["weekday"] = df[date_key].dt.dayofweek

    # define peak, mark weekdays, convert service date back
    conditions = [(df["holiday"] == False) & (df["weekday"] < 5)]
    choices = ["peak"]
    df["peak"] = np.select(conditions, choices, default="offpeak")
    df["week"] = df[date_key].dt.isocalendar().week
    df["year"] = df[date_key].dt.isocalendar().year
    df[date_key] = df[date_key].dt.date.astype(str)

    # select date of the week
    dates = df[df["weekday"] == 0]
    dates = dates[[date_key, "week", "year"]].drop_duplicates()

    # limit data to just peak, merge back dates
    final = df[df["peak"] == "peak"]

    final = final.groupby(["year", "week", route_key])[count_key].mean().round().reset_index()

    final = final.merge(dates, on=["week", "year"], how="left")

    # get list of routes
    routelist = list(set(final[route_key].tolist()))

    # create dict
    output = {}

    # write out each set of routes to dict
    for route in routelist:
        for_route = final[final[route_key] == route]
        only_date_and_count = for_route[[date_key, count_key]].dropna()
        dictdata = only_date_and_count.rename(columns={date_key: "date", count_key: "count"}).to_dict(orient="records")
        route_id = route_ids_map[route] if route_ids_map else route
        output[route_id] = dictdata
    return output


def format_subway_data(path_to_csv_file: str):
    # read data, convert to datetime
    df = pd.read_csv(path_to_csv_file)
    df["servicedate"] = pd.to_datetime(df["servicedate"])

    # add holidays
    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(start=df["servicedate"].min(), end=df["servicedate"].max())

    # mark as holiday and weekday
    df["holiday"] = df["servicedate"].dt.date.astype("datetime64[ns]").isin(holidays.date)
    df["weekday"] = df["servicedate"].dt.dayofweek

    # define peak, mark weekdays, convert service date back
    conditions = [(df["holiday"] == False) & (df["weekday"] < 5)]  # noqa: E712
    choices = ["peak"]
    df["peak"] = np.select(conditions, choices, default="offpeak")
    df["week"] = df["servicedate"].dt.isocalendar().week
    df["year"] = df["servicedate"].dt.isocalendar().year
    df["servicedate"] = df["servicedate"].dt.date.astype(str)

    # select date of the week
    dates = df[df["weekday"] == 0]
    dates = dates[["servicedate", "week", "year"]].drop_duplicates()

    # limit data to just peak, merge back dates
    final = df[df["peak"] == "peak"]
    final = final.groupby(["year", "week", "route_or_line"])["validations"].mean().round().reset_index()

    final = final.merge(dates, on=["week", "year"], how="left")

    # get list of bus routes
    routelist = list(set(final["route_or_line"].tolist()))

    # create dict
    output = {}

    # write out each set of routes to dict
    for route in routelist:
        dftemp = final[final["route_or_line"] == route].fillna(0).astype({"validations": int})
        dictdata = (
            dftemp[["servicedate", "validations"]]
            .rename(columns={"servicedate": "date", "validations": "count"})
            .to_dict(orient="records")
        )
        rewritten_route_id = unofficial_labels_map.get(route) or route
        output[rewritten_route_id] = dictdata

    return output


def format_bus_data(path_to_excel_file: str):
    # read data - new format doesn't need skiprows
    df = pd.read_excel(
        path_to_excel_file,
        sheet_name="Weekly by Route",
        keep_default_na=False,
        na_values=["N/A", "999999", "NULL"],
    )

    # Check if this is the new format (has WeekStartDay, Route, TotalRiders columns)
    if "WeekStartDay" in df.columns and "Route" in df.columns and "TotalRiders" in df.columns:
        # New format - data is already in the right structure
        df = df.rename(columns={"Route": "route", "WeekStartDay": "date", "TotalRiders": "count"})
        # cast empty/NULL values to 0
        df = df.replace(to_replace=["", "NULL"], value=0)
        # Convert route numbers to strings to match GTFS format
        df["route"] = df["route"].astype(str)
        # change datetime to date
        df["date"] = pd.to_datetime(
            df["date"],
            format="mixed",
        ).dt.date.astype(str)

    # get list of bus routes
    routelist = list(set(df["route"].tolist()))

    # create dict
    output = {}

    # write out each set of routes to dict
    for route in routelist:
        dftemp = df[df["route"] == route].fillna(0).astype({"count": int})
        dictdata = dftemp[["date", "count"]].to_dict(orient="records")
        rewritten_route_id = unofficial_labels_map.get(route) or route
        output[rewritten_route_id] = dictdata

    return output


def format_cr_data(path_to_ridershp_file: str):
    ridership_by_route = format_ridership_csv(
        path_to_csv_file=path_to_ridershp_file,
        date_key="service_date",
        route_key="line",
        count_key="estimated_boardings",
        route_ids_map=unofficial_cr_labels_map,
    )
    return ridership_by_route


def format_ferry_data(path_to_ridership_file: str):
    preprocess = pre_process_csv(
        path_to_csv_file=path_to_ridership_file,
        date_key="actual_departure",
        route_key="route_id",
        count_key="pax_on",
    )
    ridership_by_route = format_ridership_csv(
        path_to_csv_file=preprocess,
        date_key="actual_departure",
        route_key="route_id",
        count_key="pax_on",
        route_ids_map=unofficial_ferry_labels_map,
    )
    return ridership_by_route


def format_the_ride_data(path_to_ridership_file: str):
    preprocess = pre_process_csv(
        path_to_csv_file=path_to_ridership_file,
        date_key="Date",
        route_key=None,
        route_name="RIDE",
        count_key="Completed_Trips",
    )
    ridership_by_route = format_ridership_csv(
        path_to_csv_file=preprocess,
        date_key="Date",
        route_key="Route",
        count_key="Completed_Trips",
    )
    return ridership_by_route


def get_ridership_by_route_id(
    path_to_subway_file: str | None,
    path_to_bus_file: str | None,
    path_to_cr_file: str | None,
    path_to_ferry_file: str | None,
    path_to_ride_file: str | None,
):
    subway = format_subway_data(path_to_subway_file) if path_to_subway_file else {}
    bus = format_bus_data(path_to_bus_file) if path_to_bus_file else {}
    cr = format_cr_data(path_to_cr_file) if path_to_cr_file else {}
    ferry = format_ferry_data(path_to_ferry_file) if path_to_ferry_file else {}
    ride = format_the_ride_data(path_to_ride_file) if path_to_ride_file else {}

    return {**subway, **bus, **cr, **ferry, **ride}

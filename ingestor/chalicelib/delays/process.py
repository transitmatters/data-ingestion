from datetime import date, timedelta
from decimal import Decimal
import json
import re
from typing import List
from urllib.parse import urlencode

import pandas as pd
import requests

from chalicelib import constants, dynamo
from chalicelib.delays.aggregate import group_weekly_data
from chalicelib.delays.types import Alert, AlertsRequest

import spacy
from spacy.matcher import PhraseMatcher
from rapidfuzz import fuzz

TABLE_NAME = "AlertDelaysWeekly"

# Load SpaCy model and initialize PhraseMatcher
nlp = spacy.load("en_core_web_sm")
matcher = PhraseMatcher(nlp.vocab, attr="LOWER")


# Add patterns to matcher
for alert_type_label, patterns in constants.ALERT_PATTERNS.items():
    patterns_docs = [nlp.make_doc(text) for text in patterns]
    matcher.add(alert_type_label, patterns_docs)


def generate_requests(start_date: date, end_date: date, lines=constants.ALL_LINES) -> List[AlertsRequest]:
    reqs = []
    date_ranges = []
    current_date = start_date
    while current_date <= end_date:
        date_ranges.append(current_date)
        current_date += timedelta(days=1)
    for current_date in date_ranges:
        for line in lines:
            request = AlertsRequest(
                route=line,
                date=current_date,
            )
            reqs.append(request)
    return reqs


def process_single_day(request: AlertsRequest):
    params = {
        "route": request.route,
    }
    # process a single day of alerts
    request_url = constants.DD_URL_ALERTS.format(
        date=request.date.strftime(constants.DATE_FORMAT_BACKEND), parameters=urlencode(params, doseq=True)
    )
    response = requests.get(request_url)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        print(response.content.decode("utf-8"))
        raise
    return json.loads(response.content.decode("utf-8"))


def alert_is_delay(alert: Alert):
    return "delays" in alert["text"].lower() and "minutes" in alert["text"].lower()


def alert_type(alert: Alert):
    text_lower = alert["text"].lower()
    doc = nlp(text_lower)

    # First try exact phrase matching
    matches = matcher(doc)
    if matches:
        match_id, _, _ = matches[0]
        return nlp.vocab.strings[match_id]

    # If no exact match, use lemmatization and fuzzy matching for misspellings
    # Create lemmatized version of the text
    lemmatized_text = " ".join([token.lemma_ for token in doc])

    # Try fuzzy matching against patterns with similarity threshold
    best_match_score = 0
    best_match_type = None
    threshold = 85  # Adjust this threshold as needed (0-100)

    for alert_type_label, patterns in constants.ALERT_PATTERNS.items():
        for pattern in patterns:
            # Check fuzzy match on original text
            score = fuzz.partial_ratio(pattern, text_lower)
            if score > best_match_score and score >= threshold:
                best_match_score = score
                best_match_type = alert_type_label

            # Also check fuzzy match on lemmatized text
            pattern_doc = nlp(pattern)
            lemmatized_pattern = " ".join([token.lemma_ for token in pattern_doc])
            score = fuzz.partial_ratio(lemmatized_pattern, lemmatized_text)
            # Update best match if this score is better than current best and meets threshold
            if score > best_match_score and score >= threshold:
                best_match_score = score
                best_match_type = alert_type_label

    if best_match_type:
        return best_match_type

    print(alert["valid_from"], alert["text"].lower())
    return "other"


def process_delay_time(alerts: List[Alert]):
    delays = []
    for alert in alerts:
        if not alert_is_delay(alert):
            continue
        delay_time = re.findall(r"delays of about \d+ minutes", alert["text"].lower())
        if (delay_time is None) or (len(delay_time) == 0):
            # try another pattern, since the first one didn't match
            # less accurate, but better than nothing
            delay_time = re.findall(r"delays of up to \d+ minutes", alert["text"].lower())
        if (delay_time is not None) and (len(delay_time) != 0):
            delays.append(
                {
                    "delay_time": delay_time[0],
                    "alert_type": alert_type(alert),
                }
            )
    total_delay = 0
    delay_by_type = {
        "disabled_vehicle": 0,
        "signal_problem": 0,
        "power_problem": 0,
        "door_problem": 0,
        "brake_problem": 0,
        "switch_problem": 0,
        "track_issue": 0,
        "mechanical_problem": 0,
        "track_work": 0,
        "car_traffic": 0,
        "police_activity": 0,
        "medical_emergency": 0,
        "fire": 0,
        "flooding": 0,
        "other": 0,
    }
    for delay in delays:
        if (delay is None) or (len(delay) == 0):
            continue
        res = list(map(int, re.findall(r"\d+", delay["delay_time"])))
        total_delay += res[0]
        delay_by_type[delay["alert_type"]] += res[0]
    return total_delay, delay_by_type


def process_requests(requests: List[AlertsRequest], lines=constants.ALL_LINES):
    # process all requests
    all_data = {}
    for line in lines:
        all_data[line] = []

    for request in requests:
        data = process_single_day(request)
        if data is not None and len(data) != 0:
            total_delay, delay_by_type = process_delay_time(data)
            all_data[request.route].append(
                {
                    "date": request.date.isoformat(),
                    "line": request.route,
                    "total_delay_time": total_delay,
                    "delay_by_type": delay_by_type,
                }
            )

    df_data = {}
    for line in lines:
        df = pd.DataFrame(all_data[line])
        df = df.join(pd.json_normalize(df["delay_by_type"]))
        df.drop(columns=["delay_by_type"], inplace=True)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df_data[line] = df
    return df_data


def update_table(start_date: date, end_date: date, lines=constants.ALL_LINES):
    """
    Update the table with rapid transit data
    """
    alert_requests = generate_requests(start_date, end_date, lines)
    all_data = process_requests(alert_requests, lines)

    grouped_data = []
    for line, df in all_data.items():
        grouped_data.extend(group_weekly_data(df, start_date.isoformat()))

    dynamo.dynamo_batch_write(json.loads(json.dumps(grouped_data), parse_float=Decimal), TABLE_NAME)


if __name__ == "__main__":
    start_date = date(2025, 6, 1)
    end_date = date(2025, 8, 10)
    update_table(start_date, end_date, constants.ALL_LINES)

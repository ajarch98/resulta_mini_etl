"""Mini-ETL pipeline.

Extract from NFL scoreboard and events endpoints.
Transform data into the required formats.
Load data into a json file.
"""

import json
import requests
import datetime
import logging

logging.basicConfig(level=logging.INFO)

BASEURL = "https://delivery.chalk247.com/"
PARAMS = {"api_key": "74db8efa2a6db279393b433d97c2bc843f8e32b0"}
EXPORT_FILE = "result.json"


def get_event_data(start_date: datetime, end_date: datetime):
    """Call events endpoint and return data."""
    url = f"{BASEURL}scoreboard/NFL/{str(start_date)}/{str(end_date)}.json"

    try:
        response = requests.get(url, params=PARAMS)
        return response.status_code, response.json()
    except:
        return response.status_code, {}


def get_rankings():
    """Call scoreboard endpoint and return data."""
    url = f"{BASEURL}team_rankings/NFL.json"

    try:
        response = requests.get(url, params=PARAMS)
        response.raise_for_status()
        return response.status_code, response.json()
    except:
        return response.status_code, {}


def transform_data(rankings, event):
    """Merge data from scoreboard and rankings endpoints."""

    def get_event_date_and_time(event_datetime):
        """Get event_date and event_time from response.event_date (is a datetime object)."""
        event_datetime = datetime.datetime.strptime(event_datetime, "%Y-%m-%d %H:%M")
        event_date = event_datetime.strftime("%d-%m-%Y")
        event_time = event_datetime.strftime("%H:%M")
        return event_date, event_time

    def get_team_rankings(rankings, team_id):
        """Get team rankings data given scoreboard and team_id."""
        for team in rankings:
            if team_id == team["team_id"]:
                return (team["rank"], f'{float(team["adjusted_points"]):.2f}')

    event_date, event_time = get_event_date_and_time(event["event_date"])
    away_rank, away_rank_points = get_team_rankings(rankings, event["away_team_id"])
    home_rank, home_rank_points = get_team_rankings(rankings, event["home_team_id"])

    return {
        "event_id": event["event_id"],
        "event_date": event_date,
        "event_time": event_time,
        "away_team_id": event["away_team_id"],
        "away_nick_name": event["away_nick_name"],
        "away_city": event["away_city"],
        "away_rank": away_rank,
        "away_rank_points": away_rank_points,
        "home_team_id": event["home_team_id"],
        "home_nick_name": event["home_nick_name"],
        "home_city": event["home_city"],
        "home_rank": home_rank,
        "home_rank_points": home_rank_points,
    }


def main():
    """Main function used for convenience."""
    status, resp = get_rankings()

    if status != 200:
        logging.warning("Error in calling events endpoint. Exiting program...")
        return

    rankings = resp.get("results", {}).get("data", {})

    end_date = datetime.date.today()  # today
    start_date = end_date - datetime.timedelta(days=7)
    status, resp = get_event_data(start_date=start_date, end_date=end_date)

    if status != 200:
        logging.warning("Error in calling events endpoint. Exiting program...")
        return

    events_data = resp.get("results")

    result_items = []
    for _, event_data in events_data.items():
        if not event_data:
            continue

        events = event_data.get("data", {})
        for _, event_data in events.items():
            result_items.append(transform_data(rankings, event_data))

    with open(EXPORT_FILE, "w") as f:
        json.dump(result_items, f, indent=4)

    logging.info(f"Program generated at {EXPORT_FILE}.")


if __name__ == "__main__":
    main()

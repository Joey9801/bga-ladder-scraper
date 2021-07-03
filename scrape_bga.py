#!/usr/bin/env python

import argparse
import sqlite3
from datetime import date, datetime, timedelta
from time import sleep
from typing import Dict, Optional, Tuple, Callable
import requests
import uuid
import hashlib
from pathlib import Path
import logging
import itertools

from ratelimit import limits, sleep_and_retry, RateLimitException
import backoff

logging.basicConfig(
    format='[%(levelname)s] %(asctime)s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)


class NotFound(Exception):
    pass

# NB not actually an exponential backoff, just waits 1 second each time
# @backoff.on_exception(backoff.expo(base=1, factor=1), RateLimitException)
@sleep_and_retry
@limits(calls=2, period=3)
def requests_get(url, params: Dict = dict()):
    logging.info("Requesting %s params=%s", url, params)
    
    headers = {
        "User-Agent": "Joe Roberts' ladder scraper (joe@jwjr.co.uk)",
        "From": "joe@jwjr.co.uk",
    }

    r = requests.get(url, params, headers=headers)
    if r.status_code != 200:
        logging.error("Request to %s returned non-200 status code: %s", url, r.status_code)
        raise NotFound
    
    return r


def prefill_glider_models(cur: sqlite3.Cursor):
    url = "https://www.bgaladder.net/api/Gliders"
    data = requests_get(url).json()

    cur.executemany("""
        insert into glider_model (
            model_name,
            seats,
            vintage,
            turbo,
            handicap,
            ladder_id
        ) values (
            ?, ?, ?, ?, ?, ?
        )
        """, ((
            g["GliderType"],
            g["Seats"],
            g["Vintage"],
            g["Turbo"],
            g["Handicap"],
            g["GliderID"],
        ) for g in data)
    )
    

def prefill_launch_points(cur: sqlite3.Cursor):
    url = "https://www.bgaladder.net/api/LaunchPoints"
    data = requests_get(url).json()

    cur.executemany("""
        insert into launch_point (
            site_name,
            lat,
            lon,
            height_amsl,
            ladder_id,
            club_ladder_code
        ) values (?, ?, ?, ?, ?, ?)
        """, ((
            l["Site"],
            l["Latitude"],
            l["Longitude"],
            l["Altitude"] * 0.3048,
            l["LPCode"],
            l["ClubID"] if l["ClubID"] != "" else None
        ) for l in data)
    )

    
def prefill_clubs(cur: sqlite3.Cursor):
    url = "https://www.bgaladder.net/api/Clubs"
    data = requests_get(url).json()

    cur.executemany(
        "insert into club (club_name, is_university, ladder_code) values ( ?, ?, ?)",
        ((c["Name"], c["University"], c["ID"]) for c in data)
    )
    
def prefill_pilots(cur: sqlite3.Cursor):
    url = "https://www.bgaladder.net/api/ActivePilots"
    data = requests_get(url).json()

    cur.executemany(
        "insert into pilot (forename, surname, ladder_id) values (?, ?, ?)",
        ((p["ForeName"], p["Surname"], p["ID"]) for p in data)
    )

def init_database(db: sqlite3.Connection):
    cur = db.cursor()
    with open("schema.sql") as f:
        schema = f.read()
        cur.executescript(schema)

    prefill_glider_models(cur)
    prefill_launch_points(cur)
    prefill_clubs(cur)
    prefill_pilots(cur)
    
    db.commit()


def get_or_create_pilot(cur: sqlite3.Cursor, forename: str, surname: str, ladder_id: int) -> int:
    cur.execute("""
        select (id)
        from pilot
        where
            forename = ?
            and surname = ?
            and ladder_id = ?
        """, (forename, surname, ladder_id)
    )
    existing = cur.fetchone()
    if existing is not None:
        return existing[0]
    
    cur.execute(
        "insert into pilot (forename, surname, ladder_id) values (?, ?, ?)",
        (forename, surname, ladder_id)
    )

    # NB there's a uniqueness constraint on ladder_id, and we know that we just
    # inserted a non-null value
    cur.execute("select (id) from pilot where ladder_id = ?", (ladder_id,))
    return cur.fetchone()[0]


def get_or_create_club(cur: sqlite3.Cursor, bgal_club_code: str) -> int:
    cur.execute("select (id) from club where ladder_code = ?", (bgal_club_code,))
    existing = cur.fetchone()
    if existing is not None:
        return existing[0]
    
    cur.execute("insert into club (ladder_code) values (?)", (bgal_club_code,))
    cur.execute("select (id) from club where ladder_code = ?", (bgal_club_code,))
    return cur.fetchone()[0]


def get_or_create_glider_model(cur: sqlite3.Cursor, model_name: str, bgal_model_id: int) -> int:
    cur.execute("select (id) from glider_model where model_name = ?", (model_name,))
    existing = cur.fetchone()
    if existing is not None:
        return existing[0]
    
    cur.execute("insert into glider_model (model_name, ladder_id) values (?, ?)", (model_name, bgal_model_id))
    cur.execute("select (id) from glider_model where ladder_id = ?", (bgal_model_id,))
    return cur.fetchone()[0]


def get_or_create_glider(cur: sqlite3.Cursor, reg: str, model_id: int) -> int:
    cur.execute("select (id) from glider where reg = ?", (reg,))
    existing = cur.fetchone()
    if existing is not None:
        return existing[0]
    
    cur.execute("insert into glider (reg, model) values (?, ?)", (reg, model_id))
    cur.execute("select (id) from glider where reg = ?", (reg,))
    return cur.fetchone()[0]


def insert_task(cur: sqlite3.Cursor, flight_details: Dict) -> int:
    cur.execute("select count(distinct id) from task")
    task_id = cur.fetchone()[0]

    turnpoint_codes = list()
    def maybe_append(code: Optional[str]) -> bool:
        if code is None or code == "":
            return False
        else:
            turnpoint_codes.append(code)
            return True
    
    maybe_append(flight_details.get("StartPoint"))
    for i in itertools.count(1):
        if not maybe_append(flight_details.get(f"TP{i}")):
            break
    maybe_append(flight_details.get("FinishPoint"))

    cur.executemany(
        "insert into task (id, turnpoint_index, turnpoint_code) values (?, ?, ?)",
        ((task_id, i, code) for (i, code) in enumerate(turnpoint_codes))
    )

    return task_id

def download_and_archive_trace(cur: sqlite3.Cursor, archive_root: Path, flight_details: Dict) -> Optional[int]:
    flight_id = flight_details["FlightID"]
    url = f"https://www.bgaladder.net/FlightIGC/{flight_id}"

    original_filename = flight_details["LoggerFile"]

    logging.debug("Downloading %s from %s", original_filename, url)
    downloaded_at = datetime.now()

    try:
        r = requests_get(url)
    except NotFound:
        return None

    sha256_hash = hashlib.sha256(r.content).hexdigest()

    cur.execute("select (id) from trace where sha256_hash = ?", (sha256_hash,))
    existing = cur.fetchone()
    if existing is not None:
        logging.warning("Found existing trace in DB with same hash: %s", sha256_hash)
        return existing[0]

    # Simple path strategy to ensure that we don't store too many files in one directory
    archive_path = Path(sha256_hash[0], sha256_hash[1], sha256_hash)

    full_path = archive_root.joinpath(archive_path)
    full_path.parent.mkdir(parents=True, exist_ok=True)
    with open(full_path, "wb") as f:
        f.write(r.content)
        
    cur.execute(
        "insert into trace (downloaded_at, original_filename, sha256_hash) values (?, ?, ?)",
        (downloaded_at, original_filename, sha256_hash)
    )

    cur.execute("select (id) from trace where sha256_hash = ?", (sha256_hash,))
    return cur.fetchone()[0]


class ExistingFlight(Exception):
    pass


class MissingTrace(Exception):
    pass


def insert_bga_flight(db: sqlite3.Connection, archive_root: Path, flight_details: Dict, scraped_at: datetime):
    fd = flight_details
    cur = db.cursor()
    
    ladder_id = fd["FlightID"]

    cur.execute("select (id) from flight where ladder_id = ?", (ladder_id,))
    if cur.fetchone() is not None:
        logging.debug("Flight with ID %s was already in database", ladder_id)
        raise ExistingFlight
    
    logging.debug("Inserting BGA Ladder flight with ID %s", ladder_id)

    pilot_id = get_or_create_pilot(cur, fd["Forename"], fd["Surname"], fd["PilotID"])
    club_id = get_or_create_club(cur, fd["ClubID"])
    glider_model_id = get_or_create_glider_model(cur, fd["Glider"], fd["GliderCode"])
    glider_id = get_or_create_glider(cur, fd["Registration"], glider_model_id)
    trace_id = download_and_archive_trace(cur, archive_root, flight_details)
    task_id = insert_task(cur, fd)
    
    flight_date = datetime.strptime(fd["FlightDate"], "%Y-%m-%dT%H:%M:%S")
    
    cur.execute("""
        insert into flight (
            pilot,
            club,
            glider,
            trace,
            flight_date,
            scraped_at,
            is_weekend,
            is_junior,
            is_height,
            is_two_seater,
            is_wooden,
            has_engine,
            penalty,
            task,
            speed,
            handicap_speed,
            scoring_distance,
            speed_points,
            height_gain,
            height_points,
            total_points,
            ladder_id
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        pilot_id,
        club_id,
        glider_id,
        trace_id,
        flight_date,
        scraped_at,
        fd["Weekend"],
        fd["Junior"],
        fd["Height"],
        fd["TwoSeater"],
        fd["Wood"] or fd["Wooden"],
        fd["Engine"],
        fd["Penalty"],
        task_id,
        fd["Speed"],
        fd["HandicapSpeed"],
        fd["ScoringDistance"],
        fd["SpeedPoints"],
        fd["HeightGain"],
        fd["HeightPoints"],
        fd["TotalPoints"],
        ladder_id,
    ))

    db.commit()
    

def get_daily_flights(
        process: Callable[[datetime, Dict], None],
        query_season: Optional[int],
        query_month: Optional[int]=None,
        query_day: Optional[int]=None,
        page_size=100
    ) -> int:
    base_url = "https://www.bgaladder.net/API/DailyScores"

    params = { "rows": page_size }
    if query_season is not None:
        params["Season"] = query_season
    if query_month is not None:
        params["Month"] = query_month
    if query_day is not None:
        params["Day"] = query_day
    
    total_found = 0
    for page in itertools.count(1):
        params["page"] = page
        r = requests_get(base_url, params)
        scraped_at = datetime.now()
        flights = r.json()["rows"]

        for flight in flights:
            process(scraped_at, flight)

        total_found = total_found + len(flights)
        if len(flights) < page_size:
            break
            
    return total_found

def scrape_day(db: sqlite3.Connection, archive_root: Path, query_date: date) -> Tuple[int, int]:
    logging.info("Scraping flights for %s", query_date)
    
    new_flights = 0
    def process(scraped_at, flight_details):
        nonlocal new_flights
        try:
            insert_bga_flight(db, archive_root, flight_details, scraped_at)
            new_flights = new_flights + 1
        except ExistingFlight:
            pass

    total_found = get_daily_flights(
        process=process,
        query_season=query_date.year,
        query_month=query_date.day,
        query_day=query_date.day
    )

    logging.info("Finished scraping flights for %s. %s flights / %s new",
        query_date, total_found, new_flights)
    
    return (total_found, new_flights)
        
def scrape_last_n_days(db: sqlite3.Connection, archive_root: Path, lookback_days: int):
    logging.info("Scraping all flights from the last %s days", lookback_days) 

    today = date.today()
    found_flights = 0
    new_flights = 0
    for lookback in range(lookback_days):
        a, b = scrape_day(db, archive_root, today + timedelta(days=-lookback))
        found_flights = found_flights + a
        new_flights = new_flights + b
        
    logging.info(
        "Finished scraping flights for the last %s days. %s flights / %s new",
        lookback_days, found_flights, new_flights
    )

def scrape_season(db: sqlite3.Connection, archive_root: Path, season: int):
    logging.info("Scraping all flights for the %s season", season)

    new_flights = 0
    def process(scraped_at, flight_details):
        nonlocal new_flights
        try:
            insert_bga_flight(db, archive_root, flight_details, scraped_at)
            new_flights = new_flights + 1
        except ExistingFlight:
            pass

    total_found = get_daily_flights(process, season)


    logging.info(
        "Finished scraping flights for the %s season. %s flights / %s new",
        season, total_found, new_flights
    )

def main(args):
    db = sqlite3.connect(args.db)
    
    if args.init_db:
        init_database(db)
        
    if args.scrape_last_n_days is not None:
        scrape_last_n_days(db, args.archive_root, args.scrape_last_n_days)

    if args.scrape_entire_season is not None:
        scrape_season(db, args.archive_root, args.scrape_entire_season)
    
    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, help="Sqlite database to operate on", required=True)
    parser.add_argument("--archive-root", type=Path, help="Sqlite database to operate on", default="./traces")
    parser.add_argument("--init-db", action="store_true", help="Perform first time initialization on the database")
    parser.add_argument("--scrape-last-n-days", type=int, help="Scrape each of the last N days one by one", metavar="N")
    parser.add_argument("--scrape-entire-season", type=int, help="Bulk scrape an entire season", metavar="SEASON")
    
    args = parser.parse_args()
    main(args)
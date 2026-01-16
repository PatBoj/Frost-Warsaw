import sqlite3
import time
from pathlib import Path
import pandas as pd

import requests
from loguru import logger
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")

class GetDataFromAPI:
    def __init__(self):
        self.url_bus = self._get_url("bus")
        self.url_tram = self._get_url("tram")

        self.data_path = "data"
        self.table_name = "vehicles"
        self.database_name = f"{self.table_name}.db"

    @staticmethod
    def _get_url(vehicle_type: str):
        """Gets API URL request for either buses or trams. There is a small difference between them, buses uses '1'
        and trams uses '2'."""
        match vehicle_type:
            case "bus":
                vehicle_type_id = "1"
            case "tram":
                vehicle_type_id = "2"
            case _:
                msg = f"Invalid vehicle type: {vehicle_type}, possible values are: 'bus' or 'tram'."
                raise ValueError(msg)

        return (
            f"https://api.um.warszawa.pl/api/action/busestrams_get/"
            f"?resource_id=f2e5503e927d-4ad3-9500-4ab9e55deb59"
            f"&apikey={API_KEY}"
            f"&type={vehicle_type_id}"
        )

    def create_database(self) -> None:
        """Creates database in location given in init definition."""
        logger.info(f"Creating a database: {Path(self.data_path) / self.database_name}...")
        conn = sqlite3.connect(Path(self.data_path) / self.database_name)
        cursor = conn.cursor()

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time TEXT,
                lon REAL,
                lat REAL,
                line TEXT,
                vehicle_number TEXT,
                brigade TEXT)
        """)

        conn.commit()
        conn.close()
        logger.info("Created database.")

    def get_raw_data(self, url: str, timeout: int = 5) -> dict:
        """Based on the given URL function gets raw data from the API. There might be a few problems, like Internet
        connection or just busy server (idk). So if there is an any error, function waits 5 second and again tries
        to connect until succed."""
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            if "Błędna" in data.get("result"):
                logger.warning(f"Answer from an api for {url} is incorrect, waiting 5 second and trying again...")
                time.sleep(5)
                return self.get_raw_data(url, timeout)
            # logger.info(f"Successfully got the data for {url}.")
            return data
        except Exception:
            logger.warning(f"There is an issue with Internet connection to {url}, trying again...")
            time.sleep(5)
            return self.get_raw_data(url, timeout)

    def insert_data_to_database(self, data):
        """Function inserts data to the database like longitude, latitude, time, line number, vehicle number and
        brigade. It also creates an artificial id for that entry."""
        conn = sqlite3.connect(Path(self.data_path) / self.database_name)
        cursor = conn.cursor()
        try:
            data_to_database = [
                (
                    item.get("Lines"),
                    item.get("Lon"),
                    item.get("Lat"),
                    item.get("Time"),
                    item.get("VehicleNumber"),
                    item.get("Brigade"),
                )
                for item in data.get("result", [])
            ]

            cursor.executemany(f"""
                INSERT OR IGNORE INTO {self.table_name} (line, lon, lat, time, vehicle_number, brigade)
                VALUES (?, ?, ?, ?, ?, ?)
            """, data_to_database)
            conn.commit()
        except Exception as e:
            logger.info(f"Exception {e}")
        finally:    
            conn.close()

    def collect_data(self, seconds_between_requests: int = 30) -> None:
        """Function collects all data in the infinite loop. But first you need to ensure that there is no file with
        database already created. It runs in the infinite loop, gets data and saves it to the database. After getting
        data it waits given second (default is 30 seconds) and colects it again."""
        if (Path(self.data_path) / self.database_name).exists():
            msg = "Database already exists."
            raise FileExistsError(msg)

        self.create_database()
        logger.info("Collecting the data...")
        counter = 0
        try:
            while True:
                bus_data = self.get_raw_data(self.url_bus, timeout=2)
                self.insert_data_to_database(bus_data)

                tram_data = self.get_raw_data(self.url_tram, timeout=2)
                self.insert_data_to_database(tram_data)

                counter += 1
                logger.info(f"Successfully got the data for {counter} batch.")
                time.sleep(seconds_between_requests)
        except KeyboardInterrupt:
            df = self.read_database()
            df["time"] = pd.to_datetime(df["time"])

            start_time = df["time"].min().strftime("%Y-%m-%d %H:%M:%S")
            end_time = df["time"].max().strftime("%Y-%m-%d %H:%M:%S")

            logger.info(
                f"Successfully retrieved {len(df)} rows "
                f"and collected data between {start_time} and {end_time}."
            )

    def read_database(self):
        """Function just for testing, reading database only."""
        conn = sqlite3.connect(f"{self.data_path}/{self.database_name}")
        df = pd.read_sql(f"SELECT * FROM {self.table_name}", conn)
        conn.close()
        return df


if __name__ == "__main__":
    warsaw_data = GetDataFromAPI()
    warsaw_data.collect_data(25)
    

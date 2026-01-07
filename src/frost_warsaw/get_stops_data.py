import time
import pandas as pd

import requests
from loguru import logger
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")

class GetStopsDataFromAPI:
    data_path = "data"
    output_file_path = f"{data_path}/stops.csv"
    url_routes = f"https://api.um.warszawa.pl/api/action/public_transport_routes/?apikey={API_KEY}"
    url_stops = f"https://api.um.warszawa.pl/api/action/dbtimetable_get?id=ab75c33d-3a26-4342-b36a-6e5fef0a3ac3&apikey={API_KEY}"
    stops_dict = {
        "0": "przelotowy",
        "1": "stały",
        "2": "na żądanie",
        "3": "krańcowy",
        "4": "dla wysiadających",
        "5": "dla wsiadających",
        "6": "zajezdnia",
        "7": "techniczny",
        "8": "postojowy"
    }
    
    def __init__(self):
        self.routes = self.get_raw_data(self.url_routes)
        self.stops = self.get_raw_data(self.url_stops)
    
    def get_raw_data(self, url: str, timeout: int = 5) -> dict:
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            if "Błędna" in data.get("result"):
                logger.warning(f"Answer from an api for {url} is incorrect, waiting 5 second and trying again...")
                time.sleep(5)
                return self.get_raw_data(url, timeout)
            logger.info(f"Successfully got the data for {url}.")
            return data
        except Exception:
            logger.warning(f"There is an issue with Internet connection to {url}, trying again...")
            time.sleep(5)
            return self.get_raw_data(url, timeout)
        
    def prepare_routes_data(self):
        data = [
        (
            line,
            route_id,
            stop_ordinal_number,
            stop_dict.get("ulica_id"),
            stop_dict.get("nr_zespolu"),
            stop_dict.get("typ"),
            stop_dict.get("nr_przystanku"),
            stop_dict.get("odleglosc"),
        )
            for line, line_dict in self.routes.get("result", {}).items()
            for route_id, route_dict in line_dict.items()
            for stop_ordinal_number, stop_dict in route_dict.items()
        ]

        return pd.DataFrame(data, columns=[
            "line", "route_id", "stop_ordinal_number", "street_id", "complex_id", "type_id", "stop_id", "distance"])
        
    def prepare_stops_data(self):
        def get_one_row(data_vector):
            for single_entry in data_vector:
                yield single_entry.get("value")

        my_result = [get_one_row(temp.get("values")) for temp in self.stops.get("result")]

        stops = pd.DataFrame(my_result, columns=["complex_id", "stop_id", "complex_name", "street_id", "lat", "lon"])
        stops = stops.drop(columns=["street_id"])
        return stops
    
    def merge_data(self):
        routes_df = self.prepare_routes_data()
        stops_df = self.prepare_stops_data()
        
        df = routes_df.merge(
            stops_df,
            on=["complex_id", "stop_id"],
            how="left",
            validate="many_to_one"
        )
        
        df["type_name"] = df["type_id"].map(self.stops_dict)
        return df
    
    def save_data(self):
        self.merge_data().to_csv(self.output_file_path, index=False)
        
if __name__ == "__main__":
    warsaw_stops = GetStopsDataFromAPI()
    warsaw_stops.save_data()
        
    

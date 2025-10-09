#%%
from datetime import datetime, timedelta
import pandas as pd
import time
import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry

#%%
def get_weather_data(start_date: str, end_date: str) -> pd.DataFrame:
	"""
    Fetch daily weather data for a set of cities from Open-Meteo. Start date must be before end date.
	Large pulls might fail. Best used to grab small increments of time. 1 day to 1 week.

    Args:
        start_date: Start date of the weather data as a string in the "YYYY-MM-DD" format 
            👉 Example: "2021-09-10".
        end_date (datetime): End date of the weather data.
            👉 Example: "2022-09-10".

    	Returns:
        pd.DataFrame: A dataframe with daily weather observations.
    """


	city_coords = {
    "New York": (40.7127837, -74.0059413),
    "Los Angeles": (34.0522342, -118.2436849),
    "Chicago": (41.8781136, -87.6297982),
    "Houston": (29.7604267, -95.3698028),
    "Phoenix": (33.4483771, -112.0740373),
    "Philadelphia": (39.9525839, -75.1652215),
    "San Antonio": (29.4241219, -98.4936282),
    "San Diego": (32.715738, -117.1610838),
    "Dallas": (32.7766642, -96.7969879),
    "San Jose": (37.3382072, -121.8863286),
    "Austin": (30.267153, -97.7430608),
    "Jacksonville": (30.3321838, -81.655651),
    "Fort Worth": (32.7554883, -97.3307658),
    "Columbus": (39.9611755, -82.9987942),
    "Indianapolis": (39.768403, -86.158068),
    "Charlotte": (35.2270869, -80.8431267),
    "San Francisco": (37.7749295, -122.4194155),
    "Seattle": (47.6062095, -122.3320708),
    "Denver": (39.7392358, -104.990251),
    "Washington": (38.9071923, -77.0368707)
	}

	# Keep names in a stable order so response index -> city name
	cities = list(city_coords.keys())
	latitudes  = [city_coords[c][0] for c in cities]
	longitudes = [city_coords[c][1] for c in cities]

	# Helper function to look up city name by lat and long
	def find_city_name(lat, lon, tol=0.1):
		"""Return the nearest city name from the dictionary within a tolerance."""
		for city, (c_lat, c_lon) in city_coords.items():
			if abs(lat - c_lat) < tol and abs(lon - c_lon) < tol:
				return city

# Setup the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session = retry_session)

	# Make sure all required weather variables are listed here
	# The order of variables in hourly or daily is important to assign them correctly below
	url = "https://archive-api.open-meteo.com/v1/archive"
	params = {
		"latitude": latitudes,
		"longitude": longitudes,
		"start_date": start_date,
		"end_date": end_date,
		"daily": ["weather_code", "temperature_2m_min", "temperature_2m_max", "rain_sum", "precipitation_sum", "precipitation_hours", "wind_speed_10m_max", "wind_gusts_10m_max", "daylight_duration", "sunset", "sunrise"],
	}
	responses = openmeteo.weather_api(url, params=params)

	# Empty array to catch rows
	daily_dfs = []

	# Process 20 locations
	for response in responses:
		
		# get city variable
		lat = response.Latitude()
		lon = response.Longitude()
		city = find_city_name(lat,lon)


		# Process daily data. The order of variables needs to be the same as requested.
		daily = response.Daily()
		
		dates = pd.date_range(
			start=pd.to_datetime(daily.Time(), unit="s", utc=True),
			end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
			freq=pd.Timedelta(seconds=daily.Interval()),
			inclusive="left"
		)
		
		sunset_unix  = daily.Variables(9).ValuesInt64AsNumpy().copy()
		sunrise_unix = daily.Variables(10).ValuesInt64AsNumpy().copy()

		df = pd.DataFrame({
			"date": dates,
			"weather_code":        daily.Variables(0).ValuesAsNumpy(),
			"temperature_2m_min":  daily.Variables(1).ValuesAsNumpy(),
			"temperature_2m_max":  daily.Variables(2).ValuesAsNumpy(),
			"rain_sum":            daily.Variables(3).ValuesAsNumpy(),
			"precipitation_sum":   daily.Variables(4).ValuesAsNumpy(),
			"precipitation_hours": daily.Variables(5).ValuesAsNumpy(),
			"wind_speed_10m_max":  daily.Variables(6).ValuesAsNumpy(),
			"wind_gusts_10m_max":  daily.Variables(7).ValuesAsNumpy(),
			"daylight_duration":   daily.Variables(8).ValuesAsNumpy(),
			"sunset":  pd.to_datetime(sunset_unix,  unit="s", utc=True),
			"sunrise": pd.to_datetime(sunrise_unix,  unit="s", utc=True),
			"Latitude": lat,
			"Longitidue" : lon
		})
		
		# Add the city
		df["city"] = city
		
		# concate the dataframes
		daily_dfs.append(df)
		
	# Concat all the seperate dfs
	final_df = pd.concat(daily_dfs, ignore_index=True)
	
	return final_df
		
#%%
start_date = "2023-09-07"
end_date = "2023-09-07"
data = get_weather_data(start_date, end_date)


# %%

def get_historical_weather(start_date: str, end_date: str) -> pd.DataFrame:
	'''
	gathers weather data from openmeteo one day at a time from start_date to end_date.
	Should be used if pulling large amounts of data. Months to Years to comply with free rate limits

	args:
		start_date: string in format "YYYY-MM-DD"
			example: "2023-10-20"
		end_date: string in format "YYYY-MM-DD"
			example: "2024-10-20"
	return:
		pd.Dataframe: Contains weather data for whole time specified in call
	'''
	# array to catch daily weather dataframes
	daily_dfs = []

	# Convert strings to datetimes
	cur = datetime.strptime(start_date, "%Y-%m-%d")
	stop = datetime.strptime(end_date,   "%Y-%m-%d")

	# while loop that increments through days until next day is the end date
	while(cur <= stop):
		cur_str = cur.strftime("%Y-%m-%d") 
		data = get_weather_data(cur_str, cur_str)
		daily_dfs.append(data)
		cur += timedelta(days = 1)
		time.sleep(1)
	
	
	all_weather = pd.concat(daily_dfs, ignore_index=True) if daily_dfs else pd.Dataframe()
	return all_weather
# %%
start_date = "2023-09-01"
end_date = "2023-9-10"

many_days = get_historical_weather(start_date, end_date)
# %%

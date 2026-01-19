
import requests
import json
import openmeteo_requests
import requests_cache
from retry_requests import retry

cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

def get_hourly_weather(endpoint_url, latitude, longitude):
	params = {
		"latitude": latitude,
		"longitude": longitude,
		"current": ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "is_day", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "precipitation", "rain", "showers", "snowfall", "weather_code", "cloud_cover", "pressure_msl", "surface_pressure"],
		"timezone": "America/New_York",
	}
	try:
		data = requests.get(endpoint_url, params=params, timeout=10)
		data.raise_for_status()
		return data.json()
	except requests.exceptions.Timeout as e: 
		print(f"Timeout error for lat={latitude}, lon={longitude}: {e}")
		return None
	except requests.exceptions.HTTPError as e:
		print(f"HTTP error for lat={latitude}, lon={longitude}: {e}")
		return None
	except requests.exceptions.RequestException as e:
		print(f"Request error for lat={latitude}, lon={longitude}: {e}")
		return None
	except json.JSONDecodeError as e:
		print(f"JSON decode error for lat={latitude}, lon={longitude}: {e}")
		return None

def main():
	url = "https://api.open-meteo.com/v1/forecast"
	with open('coordinates.json', 'r') as f:
		coordinates = json.load(f)
	for board in coordinates['community_boards']:
		for neighborhood in board['neighborhoods']:
			name = neighborhood['name']
			lat = neighborhood['latitude']
			lon = neighborhood['longitude']
			data = get_hourly_weather(url, lat, lon)
			print(f"{name}: {data}")

if __name__ == "__main__":
	main()
	
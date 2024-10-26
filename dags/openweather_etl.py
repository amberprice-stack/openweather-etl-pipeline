import pandas as pd
from datetime import datetime
import requests


def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return round(temp_in_fahrenheit)


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No data. Finishing execution")
        return False 
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    return True


def run_weather_etl():
    api_key = "b9ca215d0f5d613feb61b508f3ef27a4"
    api_call = f"https://api.openweathermap.org/data/2.5/forecast?q=San%20Francisco,US&appid={api_key}"

    response = requests.get(api_call)
    data = response.json()
    city = data["city"]["name"]
    weather_description = []
    temp_farenheit = []
    feels_like_farenheit = []
    min_temp_farenheit = []
    max_temp_farenheit = []
    pressure = []
    humidity = []
    wind_speed = []
    time_of_record = []
    sunrise_time = []
    sunset_time = []

    for entry in data["list"]:
        weather_description.append(entry['weather'][0]['description'])
        temp_farenheit.append(kelvin_to_fahrenheit(entry['main']['temp']))
        feels_like_farenheit.append(kelvin_to_fahrenheit(entry['main']['feels_like']))
        min_temp_farenheit.append(kelvin_to_fahrenheit(entry['main']['temp_min']))
        max_temp_farenheit.append(kelvin_to_fahrenheit(entry['main']['temp_max']))
        pressure.append(entry['main']['pressure'])
        humidity.append(entry['main']['humidity'])
        wind_speed.append(entry['wind']['speed'])
        time_of_record.append(entry['dt_txt'])
        sunrise_time.append(datetime.fromtimestamp(data['city']['sunrise']).strftime('%H:%M'))
        sunset_time.append(datetime.fromtimestamp(data['city']['sunset']).strftime('%H:%M'))

    weather_dict = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_farenheit,
        "Feels Like (F)": feels_like_farenheit,
        "Minimun Temp (F)":min_temp_farenheit,
        "Maximum Temp (F)": max_temp_farenheit,
        "Pressure": pressure,
        "Humidty": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)":sunrise_time,
        "Sunset (Local Time)": sunset_time
    }

    weather_data_list = [weather_dict]
    weather_df = pd.DataFrame(weather_data_list)

    # Validate
    if check_if_valid_data(weather_df):
        print("Data valid")

    date_time_now = datetime.now()
    current_time = date_time_now.strftime("%d-%m-%Y_%H-%M")
    csv_file_name = 'san_francisco_weather_data_' + current_time
    weather_df.to_csv(f"{csv_file_name}.csv", index=False)

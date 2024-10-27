import pandas as pd
from datetime import datetime
import requests
import sqlalchemy
import os


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
    DATABASE_LOCATION = "sqlite:///weather_data.sqlite"
    api_key = "b9ca215d0f5d613feb61b508f3ef27a4"
    api_call = f"https://api.openweathermap.org/data/2.5/forecast?q=San%20Francisco,US&appid={api_key}"

    #Extract
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
    
    # Prepare a dictionary in order to turn it into a pandas dataframe
    weather_dict = {
        "city": city,
        "weather_description": weather_description,
        "temperature": temp_farenheit,
        "feels_like": feels_like_farenheit,
        "min_temp":min_temp_farenheit,
        "max_temp": max_temp_farenheit,
        "pressure": pressure,
        "humidity": humidity,
        "wind_speed": wind_speed,
        "time_of_record": time_of_record,
        "sunrise":sunrise_time,
        "sunset": sunset_time
    }

    weather_data_list = [weather_dict]
    weather_df = pd.DataFrame(weather_data_list)

    # Validate
    if check_if_valid_data(weather_df):
        print("Data valid")

    data_folder = "/opt/airflow/data"
    os.chdir(data_folder)
    date_time_now = datetime.now()
    current_time = date_time_now.strftime("%d-%m-%Y_%H-%M")
    csv_file_name = 'san_francisco_weather_data_' + current_time
    weather_df.to_csv(f"{csv_file_name}.csv", index=False)
    
    #Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    weather_csv_file = pd.read_csv(f"/opt/airflow/data/{csv_file_name}.csv")
    
    with engine.connect() as conn:
        weather_csv_file.to_sql("weather_data", conn.connection, index=False, if_exists='append')

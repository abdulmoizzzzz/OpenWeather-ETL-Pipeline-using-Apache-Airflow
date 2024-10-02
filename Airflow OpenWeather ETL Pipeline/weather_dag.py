from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
import pandas as pd
import pendulum
import pytz

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]

    # Pakistani local time zone  
    pakistan_tz = pytz.timezone('Asia/Karachi')
    time_of_record = datetime.fromtimestamp(data['dt'], tz=pakistan_tz)
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'], tz=pakistan_tz)
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'], tz=pakistan_tz)

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_fahrenheit,
        "Feels Like (F)": feels_like_fahrenheit,
        "Minimum Temp (F)": min_temp_fahrenheit,
        "Maximum Temp (F)": max_temp_fahrenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time                        
    }

    df_data = pd.DataFrame([transformed_data])

    # Saving data with timestamp
    now = datetime.now(tz=pakistan_tz)
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = f'current_weather_data_Islamabad_{dt_string}.csv'
    df_data.to_csv(f"s3://aws-openweather-bucket1/{dt_string}.csv", index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 10, tzinfo=pendulum.timezone('Asia/Karachi')),  # Using pendulum timezone object
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('weather_dag',
         default_args=default_args,
         schedule_interval='@hourly',  # Set schedule to hourly
         catchup=False) as dag:
    
    # Task to check if the weather API is ready
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Islamabad&APPID=e18bcbbdae8c95f629c5f3bb111d88f7',
        poke_interval=300,  # Poll every 5 minutes
        timeout=1200  # Timeout after 20 minutes
    )
    
    # Task to extract the weather data
    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Islamabad&APPID=e18bcbbdae8c95f629c5f3bb111d88f7',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )
    
    # Task to transform and load the weather data
    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data
    )

    # Set task dependencies
    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data

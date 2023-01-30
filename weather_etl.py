import datetime as dt
import decimal
import numpy as np
import pandas as pd
from prettytable import PrettyTable
import psycopg2 as ps
import requests
from sqlalchemy import create_engine

# Pick some U.S. Cities
city1, city2, city3, city4 = "New York City", "Los Angeles", "Chicago", "Houston"
city5, city6, city7, city8 = "Phoenix", "Philadelphia", "San Antonio", "San Diego"
city9, city10, city11, city12 = "Dallas", "San Jose", "Austin", "San Francisco"
city13, city14, city15, city16 = "Seattle", "Washington D.C.", "Miami", "Atlanta"
city17, city18, city19, city20 = "Detroit", "Las Vegas", "Salt Lake City", "Denver"

# Define a 'run weather ETL' function - which is our pipeline
def run_weather_etl():
    def scrape_weather(city):
        ### EXTRACT & TRANSFORM PHASES
        # Set URL & API from open source "Open Weather"
        base_url = "https://api.openweathermap.org/data/2.5/weather?"
        api_key = open('api_key.txt','r').read() 
        # Note that api_key.txt contains given active key from Open Weather website

        # Fetch data from API via URL
        url = base_url + "appid=" + api_key + "&q=" + city
        # Get response in JSON format - expect dictionaries to get returned
        response = requests.get(url).json()

        # Defined a temperature conversion function
        def temperature_converter(kelvin):
            celsius = decimal.Decimal(kelvin - 273.15)
            celsius_r = celsius.quantize(decimal.Decimal('0.00'))
            fahrenheit = decimal.Decimal((kelvin - 273.15) * (9/5) + 32)
            fahrenheit_r = fahrenheit.quantize(decimal.Decimal('0.00'))
            return str(celsius_r), str(fahrenheit_r)

        # Set weather variables
        temp_kelvin = response['main']['temp'] # Note that 'main' is a dictionary
        temp_celsius, temp_fahrenheit = temperature_converter(temp_kelvin)
        feels_like_klvn = response['main']['feels_like']
        feels_like_clsius, feels_like_fhrnheit = temperature_converter(feels_like_klvn)
        wind_speed = response['wind']['speed']
        humidity = response['main']['humidity']
        description = response['weather'][0]['description']
        sunrise_local_time = str(dt.datetime.utcfromtimestamp(response['sys']['sunrise'] + response['timezone']))
        sunset_local_time = str(dt.datetime.utcfromtimestamp(response['sys']['sunset'] + response['timezone']))
        
        # Append variables to a list
        weather_info = [
            city,
            temp_kelvin,
            temp_celsius,
            temp_fahrenheit,
            feels_like_klvn,
            feels_like_clsius,
            feels_like_fhrnheit,
            wind_speed,
            humidity,
            description,
            sunrise_local_time,
            sunset_local_time
        ]
        return weather_info
    # End of scrape weather function 

    # Begin preparing header for CSV
    headers = [
            "City",
            "Temperature_(K)",
            "Temperature_(C)",
            "Temperature_(F)",
            "Feels_Like_Temp_(K)",
            "Feels_Like_Temp_(C)",
            "Feels_Like_Temp_(F)",
            "Wind_Speed_(m/s)",
            "Humidity_(%)",
            "Description",
            "Sunrise_(local_time)",
            "Sunset_(local_time)"
        ]
    # Create table based on previous headers
    table = PrettyTable(headers)
    # Now we stack (as a table) the lists being returned by the inner 'scrape weather' function
    table.add_row(scrape_weather(city1)); table.add_row(scrape_weather(city2));
    table.add_row(scrape_weather(city3)); table.add_row(scrape_weather(city4));
    table.add_row(scrape_weather(city5)); table.add_row(scrape_weather(city6));
    table.add_row(scrape_weather(city7)); table.add_row(scrape_weather(city8));
    table.add_row(scrape_weather(city9)); table.add_row(scrape_weather(city10));
    table.add_row(scrape_weather(city11)); table.add_row(scrape_weather(city12));
    table.add_row(scrape_weather(city13)); table.add_row(scrape_weather(city14));
    table.add_row(scrape_weather(city15)); table.add_row(scrape_weather(city16));
    table.add_row(scrape_weather(city17)); table.add_row(scrape_weather(city18));
    table.add_row(scrape_weather(city19)); table.add_row(scrape_weather(city20));
    # Now convert data into strings for csv
    data = table.get_csv_string()
    with open('./data/weather_data.csv', 'w', newline='') as f_output:
        f_output.write(data)
    
    ### LOAD
    # There's 2 use cases of data : 
    # A) Update rows & inserting new rows (like change in data for an online account).
    # B) Rows that never get updated once loaded (like record transactions as ex.)
    # In this case the weather data is unique and like 'snapshots' 
    # create data frame
    df = pd.read_csv('./data/weather_data.csv', index_col=False)
    #df = df.reset_index(drop=True, inplace=True)
    print(df)

    # Connection info.
    host_name = 'database-1.coivz05orajj.us-east-1.rds.amazonaws.com'
    dbname = 'weather_db'
    username = 'postgres'
    password = 'Le9o1na4!050805'
    port = '5432'

    # Create connection
    try:
        conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
    except ps.OperationalError as e:
        raise e
    else:
        print('Connected!')
    
    # Cursor allows python code to execute sql statements
    curr = conn.cursor()

    # We define our table, execute and commit
    create_table_command = ("""CREATE TABLE IF NOT EXISTS weather_data (
                                    "City" VARCHAR(255) NOT NULL,
                                    "Temperature_(K)" FLOAT NOT NULL,
                                    "Temperature_(C)" FLOAT NOT NULL,
                                    "Temperature_(F)" FLOAT NOT NULL,
                                    "Feels_Like_Temp_(K)" FLOAT NOT NULL,
                                    "Feels_Like_Temp_(C)" FLOAT NOT NULL,
                                    "Feels_Like_Temp_(F)" FLOAT NOT NULL,
                                    "Wind_Speed_(m/s)" FLOAT NOT NULL,
                                    "Humidity_(%)" FLOAT NOT NULL,
                                    "Description" VARCHAR(255) NOT NULL,
                                    "Sunrise_(local_time)" TIMESTAMP NOT NULL,
                                    "Sunset_(local_time)" TIMESTAMP NOT NULL);
                            """)
    curr.execute(create_table_command)
    conn.commit()

    # The engine uses AWS's credentials to load into a postgresql database via Azure Data Studio
    engine = create_engine("postgresql+psycopg2://postgres:Le9o1na4!050805@database-1.coivz05orajj.us-east-1.rds.amazonaws.com:5432/weather_db")
    df.to_sql(name="weather_data", con=engine, if_exists='append', index=False)
    # End of ETL-Pipeline function
# Now we call the pipeline function
run_weather_etl()   






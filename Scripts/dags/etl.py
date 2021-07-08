#importing necessary libraries
import requests
import json
import pandas as pd
from matplotlib import pyplot as plt
import datetime
from decouple import config
from collections import Counter
import sqlalchemy
import sqlite3

#data validation function
def data_validation(df):
    #checking if the dataframe is empty
    if df.empty:
        print('Yikes, No songs were downloaded')

    #checking if there are duplicates
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Yikes Primary Key check violated")

    #checking for nulls
    if df.isnull().values.any():
        raise Exception("Yike!!! Found Null values")
    
    return True

def run_etl():
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config('TOKEN')}"
    }

    #getting today's date
    today = datetime.datetime.now()

    #getting last month's date
    last_month = today - datetime.timedelta(days=300)

    #converting last month's date to unix time stamp
    last_month_unix_timestamp = int(last_month.timestamp()) * 1000

    last_month_replace = last_month.replace(hour = 0, minute = 0, second = 0, microsecond = 0)

    url = f"https://api.spotify.com/v1/me/player/recently-played?after={last_month_unix_timestamp}"
    r = requests.get(url, headers = headers)
    data = r.json()

    song_names = []
    artist_name = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song['track']['name'])
        artist_name.append(song['track']['album']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])

        songs_dict = {
        "song_names":song_names,
        "artist_name":artist_name,
        "played_at":played_at_list,
        "timestamp":timestamps
    }

    songs_df = pd.DataFrame(songs_dict)

    if data_validation(songs_df):
        print("Data Validation Passed!")

    #converting played_at column to a datetime object
    songs_df['played_at']=pd.to_datetime(songs_df['played_at'])

    #creating a new column, period
    songs_df['period'] = (songs_df['played_at'].dt.hour % 24 + 4) // 4
    songs_df['period'].replace({1: 'Late Night',
                        2: 'Early Morning',
                        3: 'Morning',
                        4: 'Noon',
                        5: 'Evening',
                        6: 'Night'}, inplace=True)

    # creating engine and connecting to database
    engine = create_engine('sqlite:///my_songs.db') 
    conn = sqlite3.connect('my_songs.db')
    print ("Opened database successfully")

    # creating the table with the columns
    conn.execute("""
    CREATE TABLE IF NOT EXISTS my_songs(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        period VARCHAR(20),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """)
    print("Table created successfully")

    try:
        songs_df.to_sql('my_songs',engine,index=False, if_exists='append')
    except:
        print("Ooops! Data already exists in the database ")

    conn.close()
    print("Database Closed!")

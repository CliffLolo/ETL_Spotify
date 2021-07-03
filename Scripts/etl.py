#importing necessary libraries
import requests
import json
import pandas as pd
import datetime
from decouple import config
import sqlite3
from sqlalchemy.orm import sessionmaker
import sqlalchemy


DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

def data_validation(df):
    #check if the data frame is empty
    if df.empty:
        print('No songs were downloaded')
        # return False

    #check if there are duplicates
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check violated")

    #check for nulls
    if df.isnull().values.any():
        raise Exception("Found Null values")

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    #replacing the time attributes of the datetime object
    yesterday = yesterday.replace(hour = 0, minute = 0, second = 0, microsecond = 0)

    #converting the timestamp column to a python list and looping through to compare the dates
    for timestamp in df['timestamp'].tolist():
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("Some songs are not from the last 24 hours")
    
    return True

       
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {config('TOKEN')}"
}

#getting today's date
today = datetime.datetime.now()
print("today:",today)
#getting yesterday's date
yesterday = today - datetime.timedelta(days=30)
print("yesterday",yesterday)
#converting yesterday's date to unix time stamp
yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
print("yesterdayUnix",yesterday_unix_timestamp)
yesterday_replace = yesterday.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
print("YesterdayReplace:",yesterday_replace)

url = f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}"

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
print(songs_df)

if data_validation(songs_df):
    print("Data Validation Passed!")

# Loading

engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('my_played_tracks.sqlite')
cursor = conn.cursor()

sql_query = """
CREATE TABLE IF NOT EXISTS my_played_tracks(
    song_name VARCHAR(200),
    artist_name VARCHAR(200),
    played_at VARCHAR(200),
    timestamp VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
)
"""

cursor.execute(sql_query)
print("Opened database successfully")

try:
    song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
except:
    print("Data already exists in the database")

conn.close()
print("Close database successfully")
    
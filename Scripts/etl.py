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


def run_etl():
    headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {config('TOKEN')}"
    }

    #getting today's date
    today = datetime.datetime.now()
    print("today:",today)
    #getting last month's date
    last_month = today - datetime.timedelta(days=300)
    print("last_month",last_month)

    #converting last month's date to unix time stamp
    last_month_unix_timestamp = int(last_month.timestamp()) * 1000
    print("last_month_Unix",last_month_unix_timestamp)

    last_month_replace = last_month.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
    print("last_month_Replace:",last_month_replace)
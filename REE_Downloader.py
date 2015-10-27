# Jorge Filipe, jmfilipe@inesctec.pt, June 2015

from bs4 import BeautifulSoup
import pandas as pd
import urllib.request
import configparser
import os
import ctypes
from datetime import datetime, timedelta
from pytz import timezone

config = configparser.ConfigParser()
config.read('REE_configuration.txt')

start_date = config.get('start_date', 'date')
end_date = config.get('end_date', 'date')

forecastToExport = config.getint('forecast_config', 'forecastToExport')

url_options = {1: "preveol_DD",
               2: "demanda_aux"}

fileName_options = {1: "wind_forecast",
                    2: "load_forecast"}

dates = pd.bdate_range(start_date, end_date, freq='D')
df = pd.DataFrame()

for day in dates:

    print(day.strftime('%Y-%m-%d'))

    url = "http://www.esios.ree.es/Solicitar/%s_%s.xml&en" % (url_options[forecastToExport], day.strftime('%Y%m%d'))

    file = urllib.request.urlopen(url)
    data = file.read()

    soup = BeautifulSoup(data, "html.parser")

    hour = []
    for tag in soup.find_all(["pos"]):
        hour.append(int(tag['v']))
    wind = []
    for tag in soup.find_all(["ctd"]):
        wind.append(float(tag['v']))

    dfs = pd.DataFrame({'Day': day, 'Hour': hour, 'forecast': wind})

    if len(dfs) == 24:  # Regular Day
        dfs['Hour'] = [dfs['Hour'].iloc[i]-1 for i in range(0, len(dfs))]
        timestamp = [dfs['Day'].iloc[i]+pd.DateOffset(hours=dfs['Hour'].iloc[i].astype('object')) for i in range(0, len(dfs))]
        timestamp = [datetime.strptime(str(timestamp[i]), "%Y-%m-%d %H:%M:%S") for i in range(0, len(dfs))]
        timestamp_es = [timezone('Europe/Madrid').localize(timestamp[i]) for i in range(0, len(dfs))]
        timestamp_utc = [timestamp_es[i].astimezone(timezone('UTC')) for i in range(0, len(dfs))]

    elif len(dfs) == 23:  # Daylight Saving Time started
        dfs['Hour'] = [dfs['Hour'].iloc[i]-1 for i in range(0, len(dfs))]
        dfs['Hour'].values[2:] = [dfs['Hour'].values[i]+1 for i in range(2, len(dfs))]
        timestamp = [dfs['Day'].iloc[i]+pd.DateOffset(hours=dfs['Hour'].iloc[i].astype('object')) for i in range(0, len(dfs))]
        timestamp = [datetime.strptime(str(timestamp[i]), "%Y-%m-%d %H:%M:%S") for i in range(0, len(dfs))]
        timestamp_es = [timezone('Europe/Madrid').localize(timestamp[i]) for i in range(0, len(dfs))]
        timestamp_utc = [timestamp_es[i].astimezone(timezone('UTC')) for i in range(0, len(dfs))]

    elif len(dfs) == 25:  # Daylight Saving Time ended
        dfs['Hour'] = [dfs['Hour'].iloc[i]-1 for i in range(0, len(dfs))]
        dfs['Hour'].values[3:] = [dfs['Hour'].values[i]-1 for i in range(3, len(dfs))]
        timestamp = [dfs['Day'].iloc[i]+pd.DateOffset(hours=dfs['Hour'].iloc[i].astype('object')) for i in range(0, len(dfs))]
        timestamp = [datetime.strptime(str(timestamp[i]), "%Y-%m-%d %H:%M:%S") for i in range(0, len(dfs))]
        timestamp_es = [timezone('Europe/Madrid').localize(timestamp[i]) for i in range(0, len(dfs))]
        timestamp_utc = [timestamp_es[i].astimezone(timezone('UTC')) for i in range(0, len(dfs))]
        timestamp_utc[2] = timestamp_utc[2] - timedelta(hours=1)

    else:
        timestamp_utc = []  # avoids not define variable warning

    timezone_ = config.get('timezone', 'timezone')
    timestamp = [timestamp_utc[i].astimezone(timezone(timezone_)) for i in range(0, len(dfs))]

    dfs['Day'] = [timestamp[i].strftime('%Y-%m-%d %H:%M:%S') for i in range(0, len(dfs))]
    dfs = dfs.drop('Hour', 1)
    dfs.columns = ['timestamp', fileName_options[forecastToExport]]

    df = pd.concat([df, dfs], ignore_index=True)

os.makedirs('outputFiles\\', exist_ok=True)
fileName = 'outputFiles\\' + fileName_options[forecastToExport] + '.csv'
df.to_csv(fileName, sep=';', index=False)

ctypes.windll.user32.MessageBoxW(0, 'Export Complete', 'REE Wind/Load Forecast', 0)

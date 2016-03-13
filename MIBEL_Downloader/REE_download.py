
import urllib.request
from datetime import datetime, timedelta
from pytz import timezone

from bs4 import BeautifulSoup
import pandas as pd


def REE_download(day, forecast_to_export, timezone_):

        url_options = {'wind_forecast': "preveol_DD",
                       'load_forecast': "demanda_aux"}

        url = "http://www.esios.ree.es/Solicitar/%s_%s.xml&en" % (url_options[forecast_to_export], day.strftime('%Y%m%d'))

        try:
            file = urllib.request.urlopen(url)
            data = file.read()
        except:
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

        timestamp = [timestamp_utc[i].astimezone(timezone(timezone_)) for i in range(0, len(dfs))]

        dfs['Day'] = [timestamp[i].strftime('%Y-%m-%d %H:%M:%S') for i in range(0, len(dfs))]
        dfs = dfs.drop('Hour', 1)
        dfs.columns = ['timestamp', forecast_to_export]

        return dfs

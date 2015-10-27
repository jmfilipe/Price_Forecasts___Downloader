# Jorge Filipe, jmfilipe@inesctec.pt, June 2015

import pandas as pd
import configparser
import os
import ctypes
from datetime import datetime, timedelta
from pytz import timezone

pd.options.mode.chained_assignment = None  # default='warn'


def dateparse(x, y):
    y = (y[:2]) if len(y) > 2 else y  # the .csv file has some hours with decimal values, for instance hour=11.02
    y = str(int(y)-1)
    return pd.datetime.strptime(x + y, '%d-%m-%Y%H') + pd.DateOffset(hours=1)

config = configparser.ConfigParser()
config.read('REN_configuration.txt')

pricesToExport = config.getint('market_config', 'pricesToExport')

url_options = {1: "PrecoMerc",
               2: "PrecoMerc",
               3: "BandaSec",
               4: "Reserva"}

fileName_options = {1: "Preco_diario_e_intra",
                    2: "Preco_diario",
                    3: "BandaSecundaria",
                    4: "Reserva"}

start_date = datetime.strptime(config.get('start_date', 'date'), "%Y-%m-%d")
end_date = datetime.strptime(config.get('end_date', 'date'), "%Y-%m-%d")

dates = pd.date_range(start=start_date, end=end_date, freq='D')
df = pd.DataFrame()

for day in dates:

    print(day.strftime('%Y-%m-%d'))

    url = "http://www.mercado.ren.pt/UserPages/Dados_download.aspx?Dia1=%s&Dia2=%s&Nome=%s" % \
          (day.strftime('%Y-%m-%d'), day.strftime('%Y-%m-%d'), url_options[pricesToExport])

    dfs = pd.read_html(url, header=0, thousands=None)
    dfs = dfs[0]

    if pricesToExport == 2:
        dfs = dfs[dfs['SESSAO'] == 0]
        dfs = dfs.drop('SESSAO', 1)

    for col in dfs.columns[2:]:
        try:
            dfs[col] = dfs[col].str.replace(r",", ".").astype("float")
            dfs[col] = dfs[col].str.replace(r".", "")
        except:
            continue

    if len(dfs) == 24:  # Regular Day
        dfs['HORA'] = [dfs['HORA'].iloc[i]-1 for i in range(0, len(dfs))]
        timestamp = [dfs['DATA'].iloc[i] + " " + str(dfs['HORA'].iloc[i]) + ":00:00" for i in range(0, len(dfs))]
        timestamp = [datetime.strptime(timestamp[i], "%d-%m-%Y %H:%M:%S") for i in range(0, len(dfs))]
        timestamp_es = [timezone('Europe/Madrid').localize(timestamp[i]) for i in range(0, len(dfs))]
        timestamp_utc = [timestamp_es[i].astimezone(timezone('UTC')) for i in range(0, len(dfs))]

    elif len(dfs) == 23:  # Daylight Saving Time started
        dfs['HORA'].set_value(0, 0, 0)
        dfs['HORA'].set_value(1, 1, 0)
        timestamp = [dfs['DATA'].iloc[i] + " " + str(dfs['HORA'].iloc[i]) + ":00:00" for i in range(0, len(dfs))]
        timestamp = [datetime.strptime(timestamp[i], "%d-%m-%Y %H:%M:%S") for i in range(0, len(dfs))]
        timestamp_es = [timezone('Europe/Madrid').localize(timestamp[i]) for i in range(0, len(dfs))]
        timestamp_utc = [timestamp_es[i].astimezone(timezone('UTC')) for i in range(0, len(dfs))]

    elif len(dfs) == 25:  # Daylight Saving Time ended
        dfs['HORA'] = [dfs['HORA'].iloc[i]-1 for i in range(0, len(dfs))]
        dfs['HORA'].iloc[3:] = [dfs['HORA'].iloc[i]-1 for i in range(3, len(dfs))]
        timestamp = [dfs['DATA'].iloc[i] + " " + str(dfs['HORA'].iloc[i]) + ":00:00" for i in range(0, len(dfs))]
        timestamp = [datetime.strptime(timestamp[i], "%d-%m-%Y %H:%M:%S") for i in range(0, len(dfs))]
        timestamp_es = [timezone('Europe/Madrid').localize(timestamp[i]) for i in range(0, len(dfs))]
        timestamp_utc = [timestamp_es[i].astimezone(timezone('UTC')) for i in range(0, len(dfs))]
        timestamp_utc[2] = timestamp_utc[2] - timedelta(hours=1)

    else:
        timestamp_utc = []  # avoids not define variable warning

    timezone_ = config.get('timezone', 'timezone')
    timestamp = [timestamp_utc[i].astimezone(timezone(timezone_)) for i in range(0, len(dfs))]

    dfs['DATA'] = [timestamp[i].strftime('%Y-%m-%d %H:%M:%S') for i in range(0, len(dfs))]
    dfs = dfs.drop('HORA', 1)
    df = pd.concat([df, dfs], ignore_index=True)

os.makedirs('outputFiles\\', exist_ok=True)
fileName = 'outputFiles\\' + fileName_options[pricesToExport] + '.csv'
df.to_csv(fileName, sep=';', index=False)

ctypes.windll.user32.MessageBoxW(0, 'Export Complete', 'Prices Downloader', 0)

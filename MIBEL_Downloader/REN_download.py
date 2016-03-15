
from datetime import datetime, timedelta
from pytz import timezone
import time

import pandas as pd


def REN_download(start, end, prices_to_export, timezone_):

        url_options = {'day_ahead_plus_intraday_price': "PrecoMerc",
                       'day_ahead_price': "PrecoMerc",
                       'secondary_reserve': "BandaSec",
                       'tertiary_reserve': "Reserva"}

        url = "http://www.mercado.ren.pt/UserPages/Dados_download.aspx?Dia1=%s&Dia2=%s&Nome=%s" % \
              (start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'), url_options[prices_to_export])

        """
        http://www.mercado.ren.pt/UserPages/Dados_download.aspx?Dia1=2015-12-31&Dia2=2015-12-31&Nome=OferSec&Ordem=P
        """

        dfs = pd.read_html(url, header=0, thousands=None, flavor='bs4')
        dfs = dfs[0]

        if prices_to_export == 'day_ahead_price':
            dfs = dfs[dfs['SESSAO'] == 0]
            dfs = dfs.drop('SESSAO', 1)

        for col in dfs.columns[2:]:
            try:
                dfs[col] = dfs[col].str.replace(r",", ".").astype("float")
                dfs[col] = dfs[col].str.replace(r".", "")
            except:
                continue

        if len(dfs) % 24 == 0:  # Regular Day
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

        timestamp = [timestamp_utc[i].astimezone(timezone(timezone_)) for i in range(0, len(dfs))]

        dfs['DATA'] = [timestamp[i].strftime('%Y-%m-%d %H:%M:%S') for i in range(0, len(dfs))]
        dfs.drop('HORA', 1, inplace=True)
        dfs.rename(columns={'DATA': 'timestamp'}, inplace=True)

        return dfs


def REN_generation(day, timezone_):

    url = 'http://www.centrodeinformacao.ren.pt/userControls/GetExcel.aspx?T=CRG&P={0}&variation=PT' .format(day.strftime('%d-%m-%Y'))
    dfs = None
    count = 1
    while dfs is None:
        try:
            dfs = pd.read_html(url, header=0, thousands=None, flavor='bs4')[0]
        except ValueError:
            count += 1
            print('Reconnecting... try number: {}'.format(count))
            if count > 6:
                time.sleep(30)
            else:
                time.sleep(5)

    for col in dfs.columns[2:]:
        try:
            dfs[col] = dfs[col].str.replace(r",", ".").astype("float")
            dfs[col] = dfs[col].str.replace(r".", "")
        except:
            continue
    dfs.loc[:, 'timestamp'] = dfs['Data'] + ' ' + dfs['Hora']
    dfs.drop(['Data', 'Hora'], 1, inplace=True)
    dfs['timestamp'] = pd.to_datetime(dfs['timestamp'], format='%d-%m-%Y %H:%M')

    if len(dfs) == 25*4:  # Daylight Saving Time ended
        dfs.loc[:7, 'timestamp'] = dfs.loc[:7, 'timestamp'] - pd.DateOffset(hours=1)
        dfs.loc[:, 'timestamp'] = [timezone('UTC').localize(dfs.loc[i, 'timestamp']) for i in range(len(dfs))]
    elif len(dfs) == 24*4:
        dfs.loc[:, 'timestamp'] = [timezone('Europe/Lisbon').localize(dfs.loc[i, 'timestamp']) for i in len(dfs)]
        dfs.loc[:, 'timestamp'] = [dfs.loc[i, 'timestamp'].astimezone(timezone('UTC')) for i in len(dfs)]
    elif len(dfs) == 23*4:  # Daylight Saving Time started
        dfs.loc[4:, 'timestamp'] = dfs.loc[4:, 'timestamp'] - pd.DateOffset(hours=1)
        dfs.loc[:, 'timestamp'] = [timezone('UTC').localize(dfs.loc[i, 'timestamp']) for i in range(len(dfs))]

    dfs.set_index('timestamp', inplace=True)
    dfs = dfs.resample('1H', how='mean')
    dfs = dfs.reset_index()

    return dfs

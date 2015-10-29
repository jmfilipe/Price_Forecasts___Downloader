
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import urllib.request
import sys
from datetime import datetime, timedelta
from pytz import timezone


# todo dividir em funcoes, para nao ficar tudo no __init__
# todo dar a opcao de retornar dataframe em vez de escrever num csv
def download_range(download_type, start_date, end_date, timezone_, path=''):

    def REE_download(day, forecast_to_export, timezone_):

        url_options = {'wind_forecast': "preveol_DD",
                       'load_forecast': "demanda_aux"}

        url = "http://www.esios.ree.es/Solicitar/%s_%s.xml&en" % (url_options[forecast_to_export], day.strftime('%Y%m%d'))

        file = urllib.request.urlopen(url)
        data = file.read()
        # todo verificar se o ficheiro esta vazio, em caso afirmativo tentar o download outra vez e depois registar o erro

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

    def REN_download(day, prices_to_export, timezone_):

        url_options = {'day_ahead_plus_intraday_price': "PrecoMerc",
                       'day_ahead_price': "PrecoMerc",
                       'secondary_reserve': "BandaSec",
                       'tertiary_reserve': "Reserva"}

        url = "http://www.mercado.ren.pt/UserPages/Dados_download.aspx?Dia1=%s&Dia2=%s&Nome=%s" % \
              (day.strftime('%Y-%m-%d'), day.strftime('%Y-%m-%d'), url_options[prices_to_export])

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

        # todo implement with intraday, right does not work with lenghts
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

        timestamp = [timestamp_utc[i].astimezone(timezone(timezone_)) for i in range(0, len(dfs))]

        dfs['DATA'] = [timestamp[i].strftime('%Y-%m-%d %H:%M:%S') for i in range(0, len(dfs))]
        dfs = dfs.drop('HORA', 1)

        return dfs

    if isinstance(download_type, str):
        download_type = [download_type]

    return_df = {}

    for type_ in download_type:

        print("\n   .:: Downloading %s ::.\n" % type_)

        dates = pd.bdate_range(start_date, end_date, freq='D')
        df = pd.DataFrame()

        if type_ in ['day_ahead_price', 'secondary_reserve', 'tertiary_reserve']:
            for day in dates:
                print(day.strftime('%Y-%m-%d'))
                dfs = REN_download(day, type_, timezone_)
                df = pd.concat([df, dfs], ignore_index=True)
        elif type_ in ['wind_forecast', 'load_forecast']:
            for day in dates:
                print(day.strftime('%Y-%m-%d'))
                dfs = REE_download(day, type_, timezone_)
                df = pd.concat([df, dfs], ignore_index=True)
        else:
            sys.exit("""ERROR! Download Type: \'%s\' differs from expected values:
                    'day_ahead_price',
                    'secondary_reserve',
                    'tertiary_reserve',
                    'wind_forecast',
                    'load_forecast'
                    """ % type_)

        if path != False:
            filename = path + type_ + '.csv'
            df.to_csv(filename, sep=';', index=False)

        return_df[type_] = df

    print('\n\n Download Complete! \n\n')
    return return_df


from datetime import datetime, timedelta
from pytz import timezone
import time
import sys
import pickle

import pandas as pd


def REN_download(start, end, prices_to_export, timezone_):

        start = pd.to_datetime(start)

        url_options = {'day_ahead_plus_intraday_price': "PrecoMerc",
                       'day_ahead_price': "PrecoMerc",
                       'secondary_reserve': "BandaSec",
                       'tertiary_reserve': "Reserva",
                       'secondary_offers': "OferSec&Ordem=P",
                       'tertiary_offers': "OferTer&Ordem=P",}

        count = 0
        dfss = None
        while dfss is None:

            url = "http://www.mercado.ren.pt/UserPages/Dados_download.aspx?Dia1=%s&Dia2=%s&Nome=%s" % \
              (start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'), url_options[prices_to_export])

            try:
                dfss = pd.read_html(url, header=0, thousands=None, flavor='bs4')

            except ValueError:
                count += 1
                print('Reconnecting... try number: {}'.format(count))
                time.sleep(2)
                if count > 3:
                    print("Downloading month by month, it may take awhile")
                    if prices_to_export == 'tertiary_offers':
                        dfss = [pd.DataFrame(), pd.DataFrame()]
                    else:
                        dfss = pd.DataFrame()
                    while start <= end:
                        mid_end = min(start + pd.DateOffset(days=30), end)
                        print('... \t from {} to {} '.format(start, mid_end))
                        try:
                            url = "http://www.mercado.ren.pt/UserPages/Dados_download.aspx?Dia1=%s&Dia2=%s&Nome=%s" % \
                                (start.strftime('%Y-%m-%d'), mid_end.strftime('%Y-%m-%d'), url_options[prices_to_export])
                            aux = pd.read_html(url, header=0, thousands=None, flavor='bs4')
                            if len(aux) == 1:
                                dfss = pd.concat([dfss, aux[0]], axis=0)
                            else:
                                dfss[0] = pd.concat([dfss[0], aux[0]], axis=0)
                                dfss[1] = pd.concat([dfss[1], aux[1]], axis=0)
                        except ValueError:
                            print('Missing days from {} to {} .'.format(start, mid_end))

                        start = mid_end + pd.DateOffset(days=1)

                    if prices_to_export is not 'tertiary_offers':
                        dfss = [dfss]

                    for df in dfss:
                        df.reset_index(drop=True, inplace=True)

        # pickle.dump(dfss, open('dfss.pkl', 'wb'))
        # dfss = pickle.load(open('dfss.pkl', 'rb'))

        if prices_to_export == 'tertiary_offers':
            dfss[0] = dfss[0][dfss[0].PRECO != '*']
            dfss[0].drop('SUBIR(V)', 1, inplace=True)
            dfss[0].columns = ['DATA', 'HORA', 'AREABAL', 'MW_up', 'PRECO_up']

            dfss[1] = dfss[1][dfss[1].PRECO != '*']
            dfss[1].drop('DESCER(C)', 1, inplace=True)
            dfss[1].columns = ['DATA', 'HORA', 'AREABAL', 'MW_down', 'PRECO_down']

        if prices_to_export == 'day_ahead_price':
            dfss[0] = dfss[0][dfss[0]['SESSAO'] == 0]
            dfss[0] = dfss[0].drop('SESSAO', 1)

        for dfs, i in zip(dfss, range(len(dfss))):
            dfs = dfs.reset_index(drop=True)

            for col in dfs.columns[2:]:
                try:
                    dfs[col] = dfs[col].str.replace(r",", ".").astype("float")
                    dfs[col] = dfs[col].str.replace(r".", "")
                except (ValueError, AttributeError):
                    continue

            date_hour_pair = dfs.groupby(['DATA', 'HORA']).count()  # counts the unique pairs of date and hour, useful to find daylights saving periods
            if len(date_hour_pair) % 24 == 0:  # Regular Day
                dfs.loc[:, 'HORA'] = dfs['HORA'].astype(int)-1
                dfs.loc[:, 'DATA'] = pd.to_datetime(dfs.loc[:, 'DATA'], format="%d-%m-%Y")
                dfs.loc[:, 'DATA'] = [dfs.loc[i, 'DATA'] + pd.DateOffset(hours=int(dfs.loc[i, 'HORA'])) for i in dfs.index]
                dfs.loc[:, 'DATA'] = [timezone('Europe/Madrid').localize(dfs.loc[i, 'DATA']) for i in dfs.index]
                dfs.loc[:, 'DATA'] = [(dfs.loc[i, 'DATA']).astimezone(timezone('UTC')) for i in dfs.index]

            elif len(date_hour_pair) == 23:  # Daylight Saving Time started
                dfs.loc[:, 'HORA'] = dfs['HORA'].astype(int)-1
                dfs.loc[:, 'DATA'] = [pd.to_datetime(dfs.loc[i, 'DATA'], format="%d-%m-%Y") + pd.DateOffset(hours=int(dfs.loc[i, 'HORA'])) for i in dfs.index]
                dfs.loc[:, 'DATA'] = dfs.loc[:, 'DATA'] - pd.DateOffset(hours=1)
                dfs.loc[:, 'DATA'] = [timezone('UTC').localize(dfs.loc[i, 'DATA']) for i in dfs.index]

            elif len(date_hour_pair) == 25:  # Daylight Saving Time ended
                dfs.loc[:, 'HORA'] = dfs['HORA'].astype(int)-1
                dfs.loc[:, 'DATA'] = [pd.to_datetime(dfs.loc[i, 'DATA'], format="%d-%m-%Y") + pd.DateOffset(hours=int(dfs.loc[i, 'HORA'])) for i in dfs.index]
                dfs.loc[:, 'DATA'] = dfs.loc[:, 'DATA'] - pd.DateOffset(hours=2)
                dfs.loc[:, 'DATA'] = [timezone('UTC').localize(dfs.loc[i, 'DATA']) for i in dfs.index]

            else:
                sys.exit("Dataframe downloaded has unexpected shape")

            dfs.drop('HORA', 1, inplace=True)
            dfs.rename(columns={'DATA': 'timestamp'}, inplace=True)
            if timezone_ is not 'UTC':
                dfs.loc[:, 'timestamp'] = [(dfs.loc[i, 'timestamp']).astimezone(timezone(timezone_)) for i in dfs.index]

            if prices_to_export == 'tertiary_offers':
                if 'PRECO_up' in dfs.columns:
                    dfs.sort_values(['timestamp', 'PRECO_up'], inplace=True)
                else:
                    dfs.sort_values(['timestamp', 'PRECO_down'], inplace=True)

            dfs.loc[:, 'timestamp'] = pd.to_datetime(dfs['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S'), format='%Y-%m-%d %H:%M:%S')  # used to remove timezone information
            dfss[i] = dfs
        return dfss


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
        except (ValueError, AttributeError):
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

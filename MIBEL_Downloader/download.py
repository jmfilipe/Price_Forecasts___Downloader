
import sys

import pandas as pd
from pytz import timezone

from .REE_download import REE_download
from .REN_download import REN_download, REN_generation


def daylight_changes(dates, timezone_):
    tz = timezone(timezone_)
    tz_changes = [pd.to_datetime(i, format='%Y-%m-%d').date() for i in tz._utc_transition_times[1:]]
    dates = [pd.to_datetime(i, format='%Y-%m-%d').date() for i in dates]
    chngs = [i for i in tz_changes if dates[0] <= i <= dates[-1]]
    date_set = []
    if not chngs:
        date_set.append([dates[0], dates[-1]])
    else:
        start = dates[0]
        end = dates[-1]
        if start == end:
            return [[start, end]]
        for i in range(len(chngs)):
            if start == chngs[i]:
                date_set.append([start, start])
            else:
                date_set.append([start, chngs[i]-pd.DateOffset(days=1)])
                date_set.append([chngs[i], chngs[i]])
            start = chngs[i]+pd.DateOffset(days=1) if chngs[i] != end else chngs[i]
        if start != end:
            date_set.append([start, end])

    return date_set


def download_range(download_type, start_date, end_date, timezone_, path=''):

    if isinstance(download_type, str):
        download_type = [download_type]

    if 'wind_forecast' or 'load_forecast' in download_type:
        sys.exit("\nERROR! \n Wind and Load forecasts download not working since recently REE changed their webservice layout. ")

    return_df = {}

    for type_ in download_type:

        print("\n   .:: Downloading %s ::.\n" % type_)

        dates = pd.bdate_range(start_date, end_date, freq='D')
        if type_ is 'tertiary_offers':
            df = [pd.DataFrame()] * 2
        else:
            df = pd.DataFrame()

        if type_ in ['day_ahead_price', 'secondary_reserve', 'tertiary_reserve', 'secondary_offers', 'tertiary_offers']:
            date_set = daylight_changes(dates, 'Europe/Madrid')
            for start, end in date_set:
                print('Downloading from {} to {}'.format(start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')))
                dfs = REN_download(start, end, type_, timezone_)
                if type_ is 'tertiary_offers':
                    df[0] = pd.concat([df[0], dfs[0]], ignore_index=True)
                    df[1] = pd.concat([df[1], dfs[1]], ignore_index=True)
                else:
                    df = pd.concat([df, dfs[0]], ignore_index=True)

        else:
            for day in dates:
                print(day.strftime('%Y-%m-%d'))
                if type_ in ['wind_forecast', 'load_forecast']:
                    dfs = REE_download(day, type_, timezone_)
                elif type_ == 'generation_PT':
                    dfs = REN_generation(day, timezone_)
                else:
                    sys.exit("""ERROR! Download Type: \'%s\' differs from expected values:
                            'day_ahead_price',
                            'secondary_reserve',
                            'tertiary_reserve',
                            'wind_forecast',
                            'load_forecast',
                            'generation_PT'
                            """ % type_)
                df = pd.concat([df, dfs], ignore_index=True)

        if path is not False:
            filename = path + type_
            if type_ is 'tertiary_offers':
                df[0].to_csv(filename+'_up.csv', sep=';', index=False)
                df[1].to_csv(filename+'_down.csv', sep=';', index=False)
            else:
                df.to_csv(filename+'.csv', sep=';', index=False)

        return_df[type_] = df

    print('\n\n Download Complete! \n\n')
    return return_df

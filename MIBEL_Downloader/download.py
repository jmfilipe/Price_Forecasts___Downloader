
import sys

import pandas as pd
from pytz import timezone

from .REE_download import REE_download
from .REN_download import REN_download, REN_generation


def daylight_changes(dates, timezone_):
    tz = timezone(timezone_)
    tz_changes = tz._utc_transition_times
    chngs = [i for i in tz_changes if dates[0] <= i <= dates[-1]]
    date_set = []
    if not chngs:
        date_set.append([dates[0], dates[-1]])
    else:
        start = dates[0]
        end = dates[-1]
        for i in range(len(chngs)):
            date_set.append([start, chngs[i]-pd.DateOffset(days=1)])
            date_set.append([chngs[i], chngs[i]])
            start = chngs[i]+pd.DateOffset(days=1)
        date_set.append([start, end])

    return date_set


def download_range(download_type, start_date, end_date, timezone_, path=''):

    if isinstance(download_type, str):
        download_type = [download_type]

    return_df = {}

    for type_ in download_type:

        print("\n   .:: Downloading %s ::.\n" % type_)

        dates = pd.bdate_range(start_date, end_date, freq='D')
        df = pd.DataFrame()

        if type_ in ['day_ahead_price', 'secondary_reserve', 'tertiary_reserve']:
            date_set = daylight_changes(dates, 'Europe/Madrid')
            for start, end in date_set:
                print('Downloading from {} to {}'.format(start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')))
                dfs = REN_download(start, end, type_, timezone_)
                df = pd.concat([df, dfs], ignore_index=False)

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
                df = pd.concat([df, dfs], ignore_index=False)

        if path is not False:
            filename = path + type_ + '.csv'
            df.to_csv(filename, sep=';', index=True)

        return_df[type_] = df

    print('\n\n Download Complete! \n\n')
    return return_df

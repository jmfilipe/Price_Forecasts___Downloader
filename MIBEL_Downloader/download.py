
import sys

import pandas as pd

from .REE_download import REE_download
from .REN_download import REN_download, REN_generation


def download_range(download_type, start_date, end_date, timezone_, path=''):

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
        elif type_ == 'generation_PT':
            for day in dates:
                print(day.strftime('%Y-%m-%d'))
                REN_generation(day, timezone_)
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

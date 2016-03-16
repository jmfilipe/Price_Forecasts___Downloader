Downloader for MIBEL Market Prices and Load/Wind Forecasts
==========================================================

Simple script to download public information provided by the portuguese and spanish TSOs (REN and REE).
Exports information as a csv file and/or as a pandas DataFrame. Takes into account daylight savings time.

This downloader supports:

* MIBEL Day-ahead Energy Market Prices [REN.pt]
* Portuguese Secundary Reserve Prices and Offers [REN.pt]
* Portuguese Terciary Reserve Prices and Offers [REN.pt]
* Iberian Load Forecasts - 7 days ahead [REE.es]
* Iberian Wind Power Forecasts - 24 hours ahead [REE.es]
* Portuguese Generation - hydro, solar, etc... [REN.pt]

Installation
------------

.. code-block:: bash

  pip install MIBEL_Downloader

Dependencies:

* pandas
* BeautifulSoup
* html5lib

Documentation
-------------

.. code-block:: python

    download_range(download_type, start_date, end_date, timezone_, path)

* **download_type**:   'day_ahead_price', 'secondary_reserve', 'tertiary_reserve', 'wind_forecast', 'load_forecast', 'generation_PT', 'secondary_offers','tertiary_offers'
* **start_date**: format Y-m-d
* **end_date**: format Y-m-d
* **timezone\_**: complete list of timezones at: http://stackoverflow.com/questions/13866926/python-pytz-list-of-timezones
* **path**: False(bool) to disable .csv export; empty to use current directory; 'path' to defined specific path

Example
-------

.. code-block:: python

    import MIBEL_Downloader as mibel

    # 'day_ahead_price'
    # 'secondary_reserve'
    # 'tertiary_reserve'
    # 'wind_forecast'
    # 'load_forecast'
    # 'generation_PT'
    # 'secondary_offers'
    # 'tertiary_offers'

    # single download type
    df = mibel.download_range(download_type='day_ahead_price',
                         start_date='2015-01-01',
                         end_date='2015-05-01',
                         timezone_='UTC',
                         path=False)
    print(df['day_ahead_price'])

    # multiple download types
    mibel.download_range(download_type=['day_ahead_price', 'wind_forecast', 'load_forecast'],
                         start_date='2015-01-01',
                         end_date='2015-05-01',
                         timezone_='UTC',
                         path='c:/output/')

import sys
sys.path.insert(0, '../')
from functions import *
import pandas_datareader.data as web


@print_log
def vix_download():
    df = (
        # VIX is available from 1986
        web.DataReader('VXOCLS', 'fred', 1986)
        .reset_index()
        .rename(columns={'DATE': 'time_d', 'VXOCLS': 'vix'})
        .sort_values('time_d', ignore_index=True)
        .assign(
            vix_lag1=lambda x: x['vix'].shift(1),
            dvix=lambda x: x['vix']-x['vix_lag1'])
        .drop(columns='vix_lag1')
        .sort_values('time_d', ignore_index=True))

    df.to_parquet(download_dir/'vix.parquet.gzip', compression='gzip')

vix_download()

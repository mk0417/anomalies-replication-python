import sys
sys.path.insert(0, '../')
from functions import *
import pandas_datareader.data as web


@print_log
def gnp_deflator_download():
    df = (
        # GNP is available from 1947
        web.DataReader('GNPCTPI', 'fred', 1947)
        .reset_index()
        .rename(columns=str.lower)
        .assign(time_avail_m=lambda x: x['date']+pd.offsets.MonthEnd(0)))
    df = (
        pd.concat([df]*3)
        .sort_values('time_avail_m', ignore_index=True)
        # Assume that data available with a 3 month lag
        .assign(
            month_inc=lambda x:
            x.groupby('time_avail_m')['time_avail_m'].cumcount(),
            time_avail_m=lambda x: (
                x['time_avail_m'].dt.to_period('M')+x['month_inc'])
            .dt.to_timestamp()+pd.offsets.MonthEnd(0)+pd.offsets.MonthEnd(3),
            gnp_defl=lambda x: x['gnpctpi']/100)
        .filter(['time_avail_m', 'gnp_defl'])
        .pipe(fill_inf_to_nan, 'gnp_defl')
        .query('gnp_defl==gnp_defl')
        .sort_values('time_avail_m', ignore_index=True))

    df.to_parquet(download_dir/'gnp_deflator.parquet.gzip', compression='gzip')

gnp_deflator_download()

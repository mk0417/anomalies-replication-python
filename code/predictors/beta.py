import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# CAPM beta
# beta
# ---------------
@print_log
def predicor_beta():
    df = (
        pd.read_parquet(
            download_dir/'crsp_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        .merge(pd.read_parquet(
            download_dir/'ff_monthly.parquet.gzip',
            columns=['time_avail_m', 'rf']),
               how='inner', on='time_avail_m')
        .merge(pd.read_parquet(
            download_dir/'market_return_monthly.parquet.gzip',
            columns=['time_avail_m', 'ewretd']),
               how='inner', on='time_avail_m')
        .assign(
            retrf=lambda x: x['ret']-x['rf'],
            mktrf=lambda x: x['ewretd']-x['rf'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .pipe(rolling_ols_parallel, 'retrf', ['mktrf'], 8, 60, 20)
        .rename(columns={'mktrf': 'beta'})
        .pipe(predictor_out_clean, 'beta'))

    df.to_parquet(predictors_dir/'beta.parquet.gzip', compression='gzip')

predicor_beta()

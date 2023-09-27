import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Cash-flow variance
# var_cf
# ---------------
@print_log
def predictor_var_cf():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ib', 'dp'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='right', on=['permno', 'time_avail_m'])
        .assign(cf=lambda x: (x['ib']+x['dp'])/x['me'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            sigma=lambda x:
            x.groupby('permno')['cf']
            .rolling(window=60, min_periods=24).std().reset_index(drop=True),
            var_cf=lambda x: x['sigma']**2)
        .pipe(predictor_out_clean, 'var_cf'))

    df.to_parquet(predictors_dir/'var_cf.parquet.gzip', compression='gzip')

predictor_var_cf()

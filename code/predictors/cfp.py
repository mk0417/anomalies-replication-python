import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Cash flow to price
# cfp
# ---------------
@print_log
def predictor_cfp():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'act', 'che',
                     'lct', 'dlc', 'txp', 'dp', 'ib', 'oancf'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='right', on=['permno', 'time_avail_m']))

    for i in ['act', 'che', 'lct', 'dlc', 'txp']:
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m', i, i+'_lag12', 12)

    df['accrual'] = (
        (df['act']-df['act_lag12']-(df['che']-df['che_lag12']))
        -((df['lct']-df['lct_lag12'])-(df['dlc']-df['dlc_lag12'])
          -(df['txp']-df['txp_lag12'])-df['dp']))
    df['cfp'] = (df['ib']-df['accrual']) / df['me']
    df.loc[df['oancf'].notna(), 'cfp'] = df['oancf'] / df['me']
    df = df.pipe(predictor_out_clean, 'cfp')

    df.to_parquet(predictors_dir/'cfp.parquet.gzip', compression='gzip')

predictor_cfp()

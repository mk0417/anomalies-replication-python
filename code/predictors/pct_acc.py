import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Percent operating accruals
# pct_acc
# ---------------
@print_log
def predictor_pct_acc():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ib', 'oancf',
                     'dp', 'act', 'che', 'lct', 'txp', 'dlc'])
        .assign(pct_acc=lambda x: (x['ib']-x['oancf'])/x['ib'].abs())
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'act', 'act_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'che', 'che_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'lct', 'lct_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'dlc', 'dlc_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'txp', 'txp_lag12', 12))
    df.loc[df['ib']==0, 'pct_acc'] = (df['ib']-df['oancf'])/0.01
    df.loc[df['oancf'].isna(), 'pct_acc'] = (
        ((df['act']-df['act_lag12'])-(df['che']-df['che_lag12'])
         -((df['lct']-df['lct_lag12'])-(df['dlc']-df['dlc_lag12'])
           -(df['txp']-df['txp_lag12'])-df['dp']))/df['ib'].abs())
    df.loc[(df['oancf'].isna()) & (df['ib']==0), 'pct_acc'] = (
        ((df['act']-df['act_lag12'])-(df['che']-df['che_lag12'])
         -((df['lct']-df['lct_lag12'])-(df['dlc']-df['dlc_lag12'])
           -(df['txp']-df['txp_lag12'])-df['dp']))/0.01)
    df = df.pipe(predictor_out_clean, 'pct_acc')

    df.to_parquet(predictors_dir/'pct_acc.parquet.gzip', compression='gzip')

predictor_pct_acc()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in order backlog to assets
# order_backlog_chg
# ---------------
@print_log
def predictor_order_backlog_chg():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ob', 'at'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .assign(order_backlog=lambda x: x['ob']/(0.5*(x['at']+x['at_lag12']))))
    df.loc[df['ob']==0, 'order_backlog'] = np.nan
    df = (
        df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'order_backlog', 'order_backlog_lag12', 12)
        .assign(
            order_backlog_chg=lambda x:
            x['order_backlog']-x['order_backlog_lag12'])
        .pipe(predictor_out_clean, 'order_backlog_chg'))

    df.to_parquet(
        predictors_dir/'order_backlog_chg.parquet.gzip', compression='gzip')

predictor_order_backlog_chg()

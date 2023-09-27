import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Order backlog
# order_backlog
# ---------------
@print_log
def predictor_order_backlog():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ob', 'at'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .assign(order_backlog=lambda x: x['ob']/(0.5*(x['at']+x['at_lag12']))))
    df.loc[df['ob']==0, 'order_backlog'] = np.nan
    df = df.pipe(predictor_out_clean, 'order_backlog')

    df.to_parquet(
        predictors_dir/'order_backlog.parquet.gzip', compression='gzip')

predictor_order_backlog()

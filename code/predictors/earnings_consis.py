import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Earnings-to-price ratio
# earnings_consis
# ---------------
@print_log
def predictor_earnings_consis():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'epspx'])
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'epspx', 'epspx_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'epspx', 'epspx_lag24', 24)
        .assign(
            tmp=lambda x:
            (x['epspx']-x['epspx_lag12'])
            /(0.5*(x['epspx_lag12'].abs()+x['epspx_lag24'].abs()))))

    for i in [12, 24, 36, 48]:
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'tmp', 'tmp_lag'+str(i), i)

    df = (
        df.assign(
            earnings_consis=lambda x:
            x.filter(like='tmp').mean(axis=1)))
    df['earnings_consis'] = np.where(
        ((df['epspx']/df['epspx_lag12']).abs()>6)
        | ((df['tmp']>0) & (df['tmp_lag12']<0))
        | ((df['tmp']<0) & (df['tmp_lag12']>0)),
        np.nan, df['earnings_consis'])
    df = df.pipe(predictor_out_clean, 'earnings_consis')

    df.to_parquet(
        predictors_dir/'earnings_consis.parquet.gzip', compression='gzip')

predictor_earnings_consis()

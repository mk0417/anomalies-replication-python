import sys
sys.path.insert(0, '../')
from functions import *



# ---------------
# Earnings surprise
# earnings_surp
# ---------------
@print_log
def predictor_earnings_surp():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip')
        .filter(['permno', 'time_avail_m', 'gvkey'])
        .query('gvkey==gvkey')
        .merge(
            pd.read_parquet(
                download_dir/'compustat_q_monthly.parquet.gzip',
                columns=['time_avail_m', 'epspxq', 'gvkey']),
            how='inner', on=['gvkey', 'time_avail_m'])
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'epspxq', 'epspxq_lag12', 12)
        .assign(eps_d=lambda x: x['epspxq']-x['epspxq_lag12']))

    lag_n_list = [i for i in range(3, 25, 3)]
    for i in lag_n_list:
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'eps_d', 'tmp_lag'+str(i), i)

    df = (
        df.assign(
            drift=lambda x: x.filter(like='tmp').mean(axis=1),
            ed=lambda x: x['epspxq']-x['epspxq_lag12']-x['drift'])
        .drop(columns=['tmp_lag'+str(i) for i in lag_n_list]))

    for i in lag_n_list:
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m', 'ed', 'tmp_lag'+str(i), i)

    df = (
        df.assign(
            sd=df.filter(like='tmp').std(axis=1),
            earnings_surp=lambda x: x['ed']/x['sd'])
        .pipe(predictor_out_clean, 'earnings_surp'))

    df.to_parquet(
        predictors_dir/'earnings_surp.parquet.gzip', compression='gzip')

predictor_earnings_surp()

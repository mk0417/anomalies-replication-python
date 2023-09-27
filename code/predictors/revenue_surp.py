import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Revenue surprise
# revenue_surp
# ---------------
@print_log
def predictor_revenue_surp():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'me', 'gvkey'])
        .query('gvkey==gvkey')
        .merge(
            pd.read_parquet(
                download_dir/'compustat_q_monthly.parquet.gzip',
                columns=['gvkey', 'time_avail_m', 'revtq', 'cshprq']),
            how='inner', on=['gvkey', 'time_avail_m'])
        .assign(revps=lambda x: x['revtq']/x['cshprq'])
        .pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'revps', 'revps_lag12', 12)
        .assign(tmp=lambda x: x['revps']-x['revps_lag12']))

    for i in range(3, 25, 3):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'tmp', 'tmp_lag'+str(i), i)

    df['drift'] = df.filter(like='_lag').mean(1)
    df['revenue_surp'] = df['revps'] - df['revps_lag12'] - df['drift']
    df = df[df.columns[~df.columns.str.contains('tmp')]]

    for i in range(3, 25, 3):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'revenue_surp', 'tmp_lag'+str(i), i)

    df['sd'] = df.filter(like='_lag').std(1)
    df['revenue_surp'] = df['revenue_surp'] / df['sd']
    df = df.pipe(predictor_out_clean, 'revenue_surp')

    df.to_parquet(predictors_dir/'revenue_surp.parquet.gzip', compression='gzip')

predictor_revenue_surp()

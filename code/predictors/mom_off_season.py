import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Off-season long-term reversal
# mom_off_season
# ---------------
@print_log
def predictor_mom_off_season():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        .assign(ret=lambda x: x['ret'].fillna(0)))

    for i in range(23, 60, 12):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ret', 'ret_lag'+str(i), i)

    df = (
        df.assign(
            tmp1=df.filter(like='_lag').sum(1, min_count=1),
            tmp2=df.filter(like='_lag').count(1))
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'ret', 'ret_lag12', 12)
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            tmp_sum=lambda x:
            x.groupby('permno')['ret_lag12']
            .rolling(window=48, min_periods=1).sum().reset_index(drop=True),
            tmp_count=lambda x:
            x.groupby('permno')['ret_lag12']
            .rolling(window=48, min_periods=1).count().reset_index(drop=True),
            mom_off_season=lambda x:
            (x['tmp_sum']-x['tmp1'])/(x['tmp_count']-x['tmp2']))
        .pipe(predictor_out_clean, 'mom_off_season'))

    df.to_parquet(
        predictors_dir/'mom_off_season.parquet.gzip', compression='gzip')

predictor_mom_off_season()

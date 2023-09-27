import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Total accruals
# total_accruals
# ---------------
@print_log
def predictor_total_accruals():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ivao', 'ivst', 'dltt',
                     'dlc', 'pstk', 'sstk', 'prstkc', 'dv', 'act', 'che',
                     'lct', 'at', 'lt', 'ni', 'oancf', 'ivncf', 'fincf']))

    for i in ['ivao', 'ivst', 'dltt', 'dlc', 'pstk']:
        df[i] = df[i].fillna(0)

    df['tmp_wc'] = (df['act']-df['che']) - (df['lct']-df['dlc'])
    df['tmp_nc'] = (
        (df['at']-df['act']-df['ivao']) - (df['lt']-df['dlc']-df['dltt']))
    df['tmp_fi'] = (df['ivst']+df['ivao']) - (df['dltt']+df['dlc']+df['pstk'])
    df['year'] = df['time_avail_m'].dt.year

    for i in ['tmp_wc', 'tmp_nc', 'tmp_fi', 'at']:
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m', i, i+'_lag12', 12)

    df['total_accruals'] = np.where(
        df['year']<=1989, (df['tmp_wc']-df['tmp_wc_lag12'])
        +(df['tmp_nc']-df['tmp_nc_lag12'])+(df['tmp_fi']-df['tmp_fi_lag12']),
        df['ni']-(df['oancf']+df['ivncf']+df['fincf'])
        +(df['sstk']-df['prstkc']-df['dv']))
    df['total_accruals'] = df['total_accruals'] / df['at_lag12']
    df = df.pipe(predictor_out_clean, 'total_accruals')

    df.to_parquet(
        predictors_dir/'total_accruals.parquet.gzip', compression='gzip')

predictor_total_accruals()

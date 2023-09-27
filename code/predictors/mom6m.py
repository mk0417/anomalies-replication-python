import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Six month momentum
# mom6m
# ---------------
@print_log
def predictor_mom6m():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        .assign(ret=lambda x: x['ret'].fillna(0)))

    for i in range(1, 6):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ret', 'ret_lag'+str(i), i)

    df['mom6m'] = (1+df.filter(like='_lag')).prod(1, skipna=False) - 1
    df = df.pipe(predictor_out_clean, 'mom6m')

    df.to_parquet(predictors_dir/'mom6m.parquet.gzip', compression='gzip')

predictor_mom6m()

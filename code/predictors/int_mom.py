import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Intermediate Momentum
# int_mom
# ---------------
@print_log
def predictor_int_mom():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        .assign(ret=lambda x: x['ret'].fillna(0)))

    for i in range(7, 13):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ret', 'ret_lag'+str(i), i)

    df['int_mom'] = (1+df.filter(like='_lag')).prod(1, skipna=False) - 1
    df = df.pipe(predictor_out_clean, 'int_mom')

    df.to_parquet(predictors_dir/'int_mom.parquet.gzip', compression='gzip')

predictor_int_mom()

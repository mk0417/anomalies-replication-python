import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# LT reversal
# lt_reversal
# ---------------
@print_log
def predictor_lt_reversal():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        .assign(ret=lambda x: x['ret'].fillna(0)))

    for i in range(13, 37):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ret', 'ret_lag'+str(i), i)

    df['lt_reversal'] = (1+df.filter(like='_lag')).prod(1, skipna=False) - 1
    df = df.pipe(predictor_out_clean, 'lt_reversal')

    df.to_parquet(predictors_dir/'lt_reversal.parquet.gzip', compression='gzip')

predictor_lt_reversal()

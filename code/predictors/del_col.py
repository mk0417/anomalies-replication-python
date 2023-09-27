import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in current operating liabilities
# del_col
# ---------------
@print_log
def predictor_del_col():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'lct', 'dlc'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'lct', 'lct_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'dlc', 'dlc_lag12', 12)
        .assign(
            del_col=lambda x:
            ((x['lct']-x['dlc'])-(x['lct_lag12']-x['dlc_lag12']))
            / (0.5*(x['at']+x['at_lag12'])))
        .pipe(predictor_out_clean, 'del_col'))

    df.to_parquet(predictors_dir/'del_col.parquet.gzip', compression='gzip')

predictor_del_col()

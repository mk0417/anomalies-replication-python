import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Accruals
# accruals
# ---------------
@print_log
def predictor_accruals():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'txp', 'act', 'che',
                     'lct', 'dlc', 'at', 'dp'])
        .assign(txp=lambda x: x['txp'].fillna(0))
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'act', 'act_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'che', 'che_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'lct', 'lct_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'dlc', 'dlc_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'txp', 'txp_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .assign(
            accruals=lambda x:
            ((x['act']-x['act_lag12'])
             - (x['che']-x['che_lag12'])
             - (x['lct']-x['lct_lag12'])
             - (x['dlc']-x['dlc_lag12'])
             - (x['txp']-x['txp_lag12'])
             - x['dp']) / ((x['at']+x['at_lag12'])/2))
        .pipe(predictor_out_clean, 'accruals'))

    df.to_parquet(predictors_dir/'accruals.parquet.gzip', compression='gzip')

predictor_accruals()

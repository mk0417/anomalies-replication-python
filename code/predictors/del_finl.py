import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in financial liabilities
# del_finl
# ---------------
@print_log
def predictor_del_finl():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'pstk', 'dltt', 'dlc'])
        .assign(pstk=lambda x: x['pstk'].fillna(0))
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'dlc', 'dlc_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'dltt', 'dltt_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'pstk', 'pstk_lag12', 12)
        .assign(
            del_finl=lambda x:
            ((x['dltt']+x['dlc']+x['pstk'])
             -(x['dltt_lag12']+x['dlc_lag12']+x['pstk_lag12']))
            /(0.5*(x['at']+x['at_lag12'])))
        .pipe(predictor_out_clean, 'del_finl'))

    df.to_parquet(predictors_dir/'del_finl.parquet.gzip', compression='gzip')

predictor_del_finl()

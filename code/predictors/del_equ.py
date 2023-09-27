import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in common equity
# del_equ
# ---------------
@print_log
def predictor_del_equ():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'ceq'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'ceq', 'ceq_lag12', 12)
        .assign(
            del_equ=lambda x:
            (x['ceq']-x['ceq_lag12'])/(0.5*(x['at']+x['at_lag12'])))
        .pipe(predictor_out_clean, 'del_equ'))

    df.to_parquet(predictors_dir/'del_equ.parquet.gzip', compression='gzip')

predictor_del_equ()

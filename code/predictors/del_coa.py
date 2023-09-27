import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in current operating assets
# del_coa
# ---------------
@print_log
def predictor_del_coa():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'act', 'che'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'act', 'act_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'che', 'che_lag12', 12)
        .assign(
            del_coa=lambda x:
            ((x['act']-x['che'])-(x['act_lag12']-x['che_lag12']))
            / (0.5*(x['at']+x['at_lag12'])))
        .pipe(predictor_out_clean, 'del_coa'))

    df.to_parquet(predictors_dir/'del_coa.parquet.gzip', compression='gzip')

predictor_del_coa()

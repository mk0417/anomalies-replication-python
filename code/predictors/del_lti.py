import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in long-term investment
# del_lti
# ---------------
@print_log
def predictor_del_lti():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'ivao'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'ivao', 'ivao_lag12', 12)
        .assign(
            del_lti=lambda x:
            (x['ivao']-x['ivao_lag12'])/(0.5*(x['at']+x['at_lag12'])))
        .pipe(predictor_out_clean, 'del_lti'))

    df.to_parquet(predictors_dir/'del_lti.parquet.gzip', compression='gzip')

predictor_del_lti()

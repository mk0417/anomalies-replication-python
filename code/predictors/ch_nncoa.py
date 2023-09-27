import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in net noncurrent operating assets
# ch_nncoa
# ---------------
@print_log
def predictor_ch_nncoa():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'act',
                     'ivao', 'lt', 'dlc', 'dltt'])
        .assign(
            tmp=lambda x:
            ((x['at']-x['act']-x['ivao'])-(x['lt']-x['dlc']-x['dltt']))/x['at'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'tmp', 'tmp_lag12', 12)
        .assign(ch_nncoa=lambda x: x['tmp']-x['tmp_lag12'])
        .pipe(predictor_out_clean, 'ch_nncoa'))

    df.to_parquet(predictors_dir/'ch_nncoa.parquet.gzip', compression='gzip')

predictor_ch_nncoa()

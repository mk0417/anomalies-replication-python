import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in net working capital
# ch_nwc
# ---------------
@print_log
def predictor_ch_nwc():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'act', 'che', 'lct', 'dlc'])
        .assign(tmp=lambda x: ((x['act']-x['che'])-(x['lct']-x['dlc']))/x['at'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'tmp', 'tmp_lag12', 12)
        .assign(ch_nwc=lambda x: x['tmp']-x['tmp_lag12'])
        .pipe(predictor_out_clean, 'ch_nwc'))

    df.to_parquet(predictors_dir/'ch_nwc.parquet.gzip', compression='gzip')

predictor_ch_nwc()

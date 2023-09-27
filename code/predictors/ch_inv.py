import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in inventory
# ch_inv
# ---------------
@print_log
def predictor_ch_inv():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'invt'])
        .pipe(
            shift_var_month, 'permno', 'time_avail_m', 'invt', 'invt_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .assign(
            ch_inv=lambda x:
            (x['invt']-x['invt_lag12']) / ((x['at']+x['at_lag12'])/2))
        .pipe(predictor_out_clean, 'ch_inv'))

    df.to_parquet(predictors_dir/'ch_inv.parquet.gzip', compression='gzip')

predictor_ch_inv()

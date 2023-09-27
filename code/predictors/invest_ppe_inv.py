import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# PPE and inventory changes to assets
# invest_ppe_inv
# ---------------
@print_log
def predictor_invest_ppe_inv():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'invt', 'ppegt', 'at'])
        .pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ppegt', 'ppegt_lag12', 12)
        .pipe(
            shift_var_month, 'permno', 'time_avail_m', 'invt', 'invt_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .assign(
            invest_ppe_inv=lambda x:
            ((x['ppegt']-x['ppegt_lag12'])+(x['invt']-x['invt_lag12']))
            /x['at_lag12'])
        .pipe(predictor_out_clean, 'invest_ppe_inv'))

    df.to_parquet(
        predictors_dir/'invest_ppe_inv.parquet.gzip', compression='gzip')

predictor_invest_ppe_inv()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Inventory growth
# inv_growth
# ---------------
@print_log
def predictor_inv_growth():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'invt', 'sic', 'ppent', 'at'])
        .merge(
            pd.read_parquet(download_dir/'gnp_deflator.parquet.gzip'),
            how='inner', on='time_avail_m')
        .assign(invt=lambda x: x['invt']/x['gnp_defl']))
    df = df[(df['sic']//1000)!=4]
    df = df[(df['sic']//1000)!=6]
    df = df.query('at>0 & ppent>0')
    df = (
        df.pipe(
            shift_var_month, 'permno', 'time_avail_m', 'invt', 'invt_lag12', 12)
        .assign(inv_growth=lambda x: x['invt']/x['invt_lag12']-1)
        .pipe(predictor_out_clean, 'inv_growth'))

    df.to_parquet(predictors_dir/'inv_growth.parquet.gzip', compression='gzip')

predictor_inv_growth()

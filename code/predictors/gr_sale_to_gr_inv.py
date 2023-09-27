import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Sales growth over inventory growth
# gr_sale_to_gr_inv
# ---------------
@print_log
def predictor_gr_sale_to_gr_inv():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'sale', 'invt'])
        .pipe(
            shift_var_month, 'permno', 'time_avail_m', 'sale', 'sale_lag12', 12)
        .pipe(
            shift_var_month, 'permno', 'time_avail_m', 'sale', 'sale_lag24', 24)
        .pipe(
            shift_var_month, 'permno', 'time_avail_m', 'invt', 'invt_lag12', 12)
        .pipe(
            shift_var_month, 'permno', 'time_avail_m', 'invt', 'invt_lag24', 24)
        .assign(
            gr_sale_to_gr_inv=lambda x:
            ((x['sale']-(0.5*(x['sale_lag12']+x['sale_lag24'])))
             /(0.5*(x['sale_lag12']+x['sale_lag24'])))
            -((x['invt']-(0.5*(x['invt_lag12']+x['invt_lag24'])))
              /(0.5*(x['invt_lag12']+x['invt_lag24'])))))
    df.loc[df['gr_sale_to_gr_inv'].isna(), 'gr_sale_to_gr_inv'] = (
        ((df['sale']-df['sale_lag12'])/df['sale_lag12'])
        -((df['invt']-df['invt_lag12'])/df['invt_lag12']))
    df = df.pipe(predictor_out_clean, 'gr_sale_to_gr_inv')

    df.to_parquet(
        predictors_dir/'gr_sale_to_gr_inv.parquet.gzip', compression='gzip')

predictor_gr_sale_to_gr_inv()

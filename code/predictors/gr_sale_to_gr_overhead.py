import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Sales growth over overhead growth
# gr_sale_to_gr_overhead
# ---------------
@print_log
def predictor_gr_sale_to_gr_overhead():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'sale', 'xsga'])
        .pipe(
            shift_var_month, 'permno', 'time_avail_m', 'sale', 'sale_lag12', 12)
        .pipe(
            shift_var_month, 'permno', 'time_avail_m', 'sale', 'sale_lag24', 24)
        .pipe(
            shift_var_month, 'permno', 'time_avail_m', 'xsga', 'xsga_lag12', 12)
        .pipe(
            shift_var_month, 'permno', 'time_avail_m', 'xsga', 'xsga_lag24', 24)
        .assign(
            gr_sale_to_gr_overhead=lambda x:
            ((x['sale']-(0.5*(x['sale_lag12']+x['sale_lag24'])))
             /(0.5*(x['sale_lag12']+x['sale_lag24'])))
            -((x['xsga']-(0.5*(x['xsga_lag12']+x['xsga_lag24'])))
              /(0.5*(x['xsga_lag12']+x['xsga_lag24'])))))
    df.loc[df['gr_sale_to_gr_overhead'].isna(), 'gr_sale_to_gr_overhead'] = (
        ((df['sale']-df['sale_lag12'])/df['sale_lag12'])
        -((df['xsga']-df['xsga_lag12'])/df['xsga_lag12']))
    df = df.pipe(predictor_out_clean, 'gr_sale_to_gr_overhead')

    df.to_parquet(
        predictors_dir/'gr_sale_to_gr_overhead.parquet.gzip', compression='gzip')

predictor_gr_sale_to_gr_overhead()

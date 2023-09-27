import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in taxes
# ch_tax
# ---------------
@print_log
def predictor_ch_tax():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'gvkey'])
        .merge(
            pd.read_parquet(
                download_dir/'compustat_q_monthly.parquet.gzip',
                columns=['gvkey', 'time_avail_m', 'txtq']),
            how='inner', on=['gvkey', 'time_avail_m'])
        .pipe(
            shift_var_month, 'permno', 'time_avail_m', 'txtq', 'txtq_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .assign(ch_tax=lambda x: (x['txtq']-x['txtq_lag12'])/x['at_lag12'])
        .pipe(predictor_out_clean, 'ch_tax'))

    df.to_parquet(predictors_dir/'ch_tax.parquet.gzip', compression='gzip')

predictor_ch_tax()

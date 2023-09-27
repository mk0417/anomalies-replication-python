import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Net operating assets
# noa
# ---------------
@print_log
def predictor_noa():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at',
                     'che', 'dltt', 'mib', 'dc', 'ceq'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .assign(
            oa=lambda x: x['at']-x['che'],
            ol=lambda x: x['at']-x['dltt']-x['mib']-x['dc']-x['ceq'],
            noa=lambda x: (x['oa']-x['ol'])/x['at_lag12'])
        .pipe(predictor_out_clean, 'noa'))

    df.to_parquet(predictors_dir/'noa.parquet.gzip', compression='gzip')

predictor_noa()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in net financial assets
# del_net_fin
# ---------------
@print_log
def predictor_del_net_fin():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'pstk',
                     'dltt', 'dlc', 'ivst', 'ivao'])
        .assign(pstk=lambda x: x['pstk'].fillna(0))
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .assign(
            tmp=lambda x:
            (x['ivst']+x['ivao'])-(x['dltt']+x['dlc']+x['pstk']))
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'tmp', 'tmp_lag12', 12)
        .assign(
            del_net_fin=lambda x:
            (x['tmp']-x['tmp_lag12'])/(0.5*(x['at']+x['at_lag12'])))
        .pipe(predictor_out_clean, 'del_net_fin'))

    df.to_parquet(predictors_dir/'del_net_fin.parquet.gzip', compression='gzip')

predictor_del_net_fin()

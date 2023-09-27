import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Deferred revenue
# del_drc
# ---------------
@print_log
def predictor_del_drc():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'drc',
                     'at', 'ceq', 'sale', 'sic'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'drc', 'drc_lag12', 12)
        .assign(
            del_drc=lambda x:
            (x['drc']-x['drc_lag12'])/(0.5*(x['at']+x['at_lag12']))))
    df['del_drc'] = np.where(
        (df['ceq']<=0)
        | ((df['drc']==0) & (df['del_drc']==0))
        | (df['sale']<5)
        | ((df['sic']>=6000) & (df['sic']<7000)),
        np.nan, df['del_drc'])
    df = df.pipe(predictor_out_clean, 'del_drc')

    df.to_parquet(predictors_dir/'del_drc.parquet.gzip', compression='gzip')

predictor_del_drc()

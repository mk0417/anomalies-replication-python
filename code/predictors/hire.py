import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Employee growth
# hire
# ---------------
@print_log
def predictor_hire():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'emp'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'emp', 'emp_lag12', 12)
        .assign(
            hire=lambda x:
            (x['emp']-x['emp_lag12'])/(0.5*(x['emp']+x['emp_lag12']))))
    df.loc[(df['emp'].isna()) | (df['emp_lag12'].isna()), 'hire'] = 0
    df.loc[df['time_avail_m'].dt.year<1965, 'hire'] = np.nan
    df = df.pipe(predictor_out_clean, 'hire')

    df.to_parquet(predictors_dir/'hire.parquet.gzip', compression='gzip')

predictor_hire()

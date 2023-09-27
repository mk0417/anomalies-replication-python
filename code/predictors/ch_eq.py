import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Sustainable Growth
# ch_eq
# ---------------
@print_log
def predictor_ch_eq():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ceq'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'ceq', 'ceq_lag12', 12))
    df['ch_eq'] = np.where(
        (df['ceq']>0) & (df['ceq_lag12']>0), df['ceq']/df['ceq_lag12'], np.nan)
    df = df.pipe(predictor_out_clean, 'ch_eq')

    df.to_parquet(predictors_dir/'ch_eq.parquet.gzip', compression='gzip')

predictor_ch_eq()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Earnings-to-price ratio
# ep
# ---------------
@print_log
def predictor_ep():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ib'])
        .merge(
            pd.read_parquet(
                download_dir/'crsp_monthly.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='right', on=['permno', 'time_avail_m'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'me', 'me_lag6', 6)
        .assign(ep=lambda x: x['ib']/x['me_lag6']))
    df.loc[df['ep']<0, 'ep'] = np.nan
    df = df.pipe(predictor_out_clean, 'ep')

    df.to_parquet(predictors_dir/'ep.parquet.gzip', compression='gzip')

predictor_ep()

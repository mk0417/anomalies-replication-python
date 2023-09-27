import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Volume to market equity
# vol_mkt
# ---------------
@print_log
def predictor_vol_mkt():
    df = (
        pd.read_parquet(
            download_dir/'crsp_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'vol', 'prc', 'shrout'])
        .assign(
            me=lambda x: x['shrout']*(x['prc'].abs()),
            tmp=lambda x: x['vol']*(x['prc'].abs()))
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            tmp_mean=lambda x:
            x.groupby('permno')['tmp']
            .rolling(window=12, min_periods=10).mean().reset_index(drop=True),
            vol_mkt=lambda x: x['tmp_mean']/x['me'])
        .pipe(predictor_out_clean, 'vol_mkt'))

    df.to_parquet(predictors_dir/'vol_mkt.parquet.gzip', compression='gzip')

predictor_vol_mkt()

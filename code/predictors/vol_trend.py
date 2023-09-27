import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Volume trend
# vol_trend
# ---------------
@print_log
def predictor_vol_trend():
    df_raw = pd.read_parquet(
        download_dir/'crsp_monthly.parquet.gzip',
        columns=['permno', 'time_avail_m', 'vol'])

    df_beta = (
        df_raw.copy()
        .sort_values(['permno', 'time_avail_m'])
        .assign(
            time=lambda x:
            x.groupby('permno')['permno'].cumcount()+1)
        .pipe(rolling_ols_parallel, 'vol', ['time'], 8, 60, 30)
        .rename(columns={'time': 'beta_vol_trend'}))

    df = (
        df_raw.merge(df_beta, how='left', on=['permno', 'time_avail_m'])
        .sort_values(['permno', 'time_avail_m'])
        .assign(
            tmp=lambda x:
            x.groupby('permno')['vol']
            .rolling(window=60, min_periods=30).mean().reset_index(drop=True),
            vol_trend=lambda x: x['beta_vol_trend']/x['tmp'])
        .pipe(predictor_out_clean, 'vol_trend'))

    df.to_parquet(predictors_dir/'vol_trend.parquet.gzip', compression='gzip')

predictor_vol_trend()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Momentum without seasonal part
# mom12m_off_season
# ---------------
@print_log
def predictor_mom12m_off_season():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        .assign(ret=lambda x: x['ret'].fillna(0))
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'ret', 'ret_lag1', 1)
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            # Excludes the most recent return
            mom12m_off_season=lambda x:
            x.groupby('permno')['ret_lag1']
            .rolling(window=9, min_periods=6).mean().reset_index(drop=True))
        .pipe(predictor_out_clean, 'mom12m_off_season'))

    df.to_parquet(
        predictors_dir/'mom12m_off_season.parquet.gzip', compression='gzip')

predictor_mom12m_off_season()

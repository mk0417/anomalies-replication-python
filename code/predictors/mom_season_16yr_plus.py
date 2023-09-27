import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Return Seasonality (16-20)
# mom_season_16yr_plus
# ---------------
@print_log
def predictor_mom_season_16yr_plus():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        .assign(ret=lambda x: x['ret'].fillna(0)))

    for i in range(191, 240, 12):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ret', 'ret_lag'+str(i), i)

    df = (
        df.assign(mom_season_16yr_plus=df.filter(like='_lag').mean(1))
        .pipe(predictor_out_clean, 'mom_season_16yr_plus'))

    df.to_parquet(
        predictors_dir/'mom_season_16yr_plus.parquet.gzip', compression='gzip')

predictor_mom_season_16yr_plus()

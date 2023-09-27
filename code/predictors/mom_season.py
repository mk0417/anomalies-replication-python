import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Return seasonality (years 2 to 5)
# mom_season
# ---------------
@print_log
def predictor_mom_season():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        .assign(ret=lambda x: x['ret'].fillna(0)))

    for i in range(23, 60, 12):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ret', 'ret_lag'+str(i), i)

    df = (
        df.assign(mom_season=df.filter(like='_lag').mean(1))
        .pipe(predictor_out_clean, 'mom_season'))

    df.to_parquet(predictors_dir/'mom_season.parquet.gzip', compression='gzip')

predictor_mom_season()

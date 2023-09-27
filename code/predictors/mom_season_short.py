import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Return seasonality last year
# mom_season_short
# ---------------
@print_log
def predictor_mom_season_short():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        .assign(ret=lambda x: x['ret'].fillna(0))
        .pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ret', 'mom_season_short', 11)
        .pipe(predictor_out_clean, 'mom_season_short'))

    df.to_parquet(
        predictors_dir/'mom_season_short.parquet.gzip', compression='gzip')

predictor_mom_season_short()

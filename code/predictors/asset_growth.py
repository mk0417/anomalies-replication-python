import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Asset growth
# asset_growth
# ---------------
@print_log
def predictor_asset_growth():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .assign(asset_growth=lambda x: x['at']/x['at_lag12'] - 1)
        .pipe(predictor_out_clean, 'asset_growth'))

    df.to_parquet(predictors_dir/'asset_growth.parquet.gzip', compression='gzip')

predictor_asset_growth()

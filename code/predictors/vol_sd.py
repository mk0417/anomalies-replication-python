import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Volume variance
# vol_std
# ---------------
@print_log
def predictor_vol_std():
    df = (
        pd.read_parquet(
            download_dir/'crsp_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'vol'])
        .sort_values(['permno', 'time_avail_m'])
        .assign(
            vol_std=lambda x:
            x.groupby('permno')['vol']
            .rolling(window=36, min_periods=24).std().reset_index(drop=True))
        .pipe(predictor_out_clean, 'vol_std'))

    df.to_parquet(predictors_dir/'vol_std.parquet.gzip', compression='gzip')

predictor_vol_std()

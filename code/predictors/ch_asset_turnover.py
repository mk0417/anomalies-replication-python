import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in asset turnover
# ch_asset_turnover
# ---------------
@print_log
def predictor_ch_asset_turnover():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'rect', 'invt', 'aco',
                     'ppent', 'intan', 'ap', 'lco', 'lo', 'sale'])
        .assign(
            tmp=lambda x:
            x['rect']+x['invt']+x['aco']+x['ppent']+x['intan']
            -x['ap']-x['lco']-x['lo'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'tmp', 'tmp_lag12', 12)
        .assign(
            asset_turnover=lambda x: x['sale']/((x['tmp']+x['tmp_lag12'])/2))
        .pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'asset_turnover', 'asset_turnover_lag12', 12))
    df.loc[df['asset_turnover']<0, 'asset_turnover'] = np.nan
    df['ch_asset_turnover'] = df['asset_turnover'] - df['asset_turnover_lag12']
    df = df.pipe(predictor_out_clean, 'ch_asset_turnover')

    df.to_parquet(
        predictors_dir/'ch_asset_turnover.parquet.gzip', compression='gzip')

predictor_ch_asset_turnover()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Past trading volume
# dol_vol
# ---------------
@print_log
def predictor_dol_vol():
    df = (
        pd.read_parquet(
            download_dir/'crsp_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'vol', 'prc'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'vol', 'vol_lag2', 2)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'prc', 'prc_lag2', 2)
        .assign(dol_vol=lambda x: x['vol_lag2']*x['prc_lag2'].abs()))
    df.loc[df['dol_vol']<=0, 'dol_vol'] = np.nan
    df = (
        df.assign(dol_vol=np.log(df['dol_vol']))
        .pipe(predictor_out_clean, 'dol_vol'))

    df.to_parquet(predictors_dir/'dol_vol.parquet.gzip', compression='gzip')

predictor_dol_vol()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Book-to-market (RRL)
# Rosenberg, Reid, and Lanstein (1985)
# bm_rrl
# ---------------
@print_log
def predictor_bm_rrl():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ceq'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='right', on=['permno', 'time_avail_m'])
        .assign(
            tmp_bm=lambda x: x['ceq']/x['me']))

    df.loc[df['tmp_bm']<=0, 'tmp_bm'] = np.nan
    df = (
        df.assign(bm_rrl=np.log(df['tmp_bm']))
        .pipe(predictor_out_clean, 'bm_rrl'))

    df.to_parquet(predictors_dir/'bm_rrl.parquet.gzip', compression='gzip')

predictor_bm_rrl()

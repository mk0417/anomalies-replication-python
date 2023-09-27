import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Share issuance (5 year)
# share_iss_5yr
# ---------------
@print_log
def predictor_share_iss_5yr():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m'])
        .merge(
            pd.read_parquet(
                download_dir/'crsp_monthly.parquet.gzip',
                columns=['permno', 'time_avail_m', 'shrout', 'cfacshr']),
            how='inner', on=['permno', 'time_avail_m'])
        .assign(tmp=lambda x: x['shrout']/x['cfacshr'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'tmp', 'tmp_lag60', 60)
        .assign(
            share_iss_5yr=lambda x:
            (x['tmp']-x['tmp_lag60'])/x['tmp_lag60'])
        .pipe(predictor_out_clean, 'share_iss_5yr'))

    df.to_parquet(
        predictors_dir/'share_iss_5yr.parquet.gzip', compression='gzip')

predictor_share_iss_5yr()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Share issuance (1 year)
# share_iss_1yr
# ---------------
@print_log
def predictor_share_iss_1yr():
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
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'tmp', 'tmp_lag6', 6)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'tmp', 'tmp_lag18', 18)
        .assign(
            share_iss_1yr=lambda x:
            (x['tmp_lag6']-x['tmp_lag18'])/x['tmp_lag18'])
        .pipe(predictor_out_clean, 'share_iss_1yr'))

    df.to_parquet(
        predictors_dir/'share_iss_1yr.parquet.gzip', compression='gzip')

predictor_share_iss_1yr()

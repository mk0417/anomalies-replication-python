import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Cash productivity
# cash_prod
# ---------------
@print_log
def predictor_cash_prod():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'che'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='inner', on=['permno', 'time_avail_m'])
        .assign(cash_prod=lambda x: (x['me']-x['at'])/x['che'])
        .pipe(predictor_out_clean, 'cash_prod'))

    df.to_parquet(predictors_dir/'cash_prod.parquet.gzip', compression='gzip')

predictor_cash_prod()

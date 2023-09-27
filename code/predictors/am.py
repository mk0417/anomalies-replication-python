import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Total assets to market
# am
# ---------------
@print_log
def predictor_am():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='inner', on=['permno', 'time_avail_m'])
        .assign(am=lambda x: x['at']/x['me'])
        .pipe(predictor_out_clean, 'am'))

    df.to_parquet(predictors_dir/'am.parquet.gzip', compression='gzip')

predictor_am()

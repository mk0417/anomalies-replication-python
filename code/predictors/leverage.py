import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Market leverage
# leverage
# ---------------
@print_log
def predictor_leverage():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'lt'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='right', on=['permno', 'time_avail_m'])
        .assign(leverage=lambda x: x['lt']/x['me'])
        .pipe(predictor_out_clean, 'leverage'))

    df.to_parquet(predictors_dir/'leverage.parquet.gzip', compression='gzip')

predictor_leverage()

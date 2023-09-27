import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Sales-to-price ratio
# sp
# ---------------
@print_log
def predictor_sp():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'sale'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='inner', on=['permno', 'time_avail_m'])
        .assign(sp=lambda x: x['sale']/x['me'])
        .pipe(predictor_out_clean, 'sp'))

    df.to_parquet(predictors_dir/'sp.parquet.gzip', compression='gzip')

predictor_sp()

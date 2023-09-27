import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Cash flow to market
# cf
# ---------------
@print_log
def predictor_cf():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ib', 'dp'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='right', on=['permno', 'time_avail_m'])
        .assign(cf=lambda x: (x['ib']+x['dp'])/x['me'])
        .pipe(predictor_out_clean, 'cf'))

    df.to_parquet(predictors_dir/'cf.parquet.gzip', compression='gzip')

predictor_cf()

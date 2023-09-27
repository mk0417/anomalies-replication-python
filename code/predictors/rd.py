import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# R&D-to-market cap
# rd
# ---------------
@print_log
def predictor_rd():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'me', 'gvkey'])
        .query('gvkey==gvkey')
        .merge(
            pd.read_parquet(
                download_dir/'compustat_a_monthly.parquet.gzip',
                columns=['gvkey', 'time_avail_m', 'xrd']),
            how='inner', on=['gvkey', 'time_avail_m'])
        .assign(rd=lambda x: x['xrd']/x['me'])
        .pipe(predictor_out_clean, 'rd'))

    df.to_parquet(predictors_dir/'rd.parquet.gzip', compression='gzip')

predictor_rd()

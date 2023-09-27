import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Return on assets
# rao_q
# ---------------
@print_log
def predictor_roa_q():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'gvkey'])
        .query('gvkey==gvkey')
        .merge(
            pd.read_parquet(
                download_dir/'compustat_q_monthly.parquet.gzip',
                columns=['gvkey', 'time_avail_m', 'atq', 'ibq']),
            how='inner', on=['gvkey', 'time_avail_m'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'atq', 'atq_lag3', 3)
        .assign(roa_q=lambda x: x['ibq']/x['atq_lag3'])
        .pipe(predictor_out_clean, 'roa_q'))

    df.to_parquet(predictors_dir/'roa_q.parquet.gzip', compression='gzip')

predictor_roa_q()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Junk stock momentum
# mom6m_junk
# ---------------
@print_log
def predictor_mom6m_junk():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret', 'gvkey'])
        .query('gvkey==gvkey')
        .merge(
            pd.read_parquet(
                download_dir/'sp_credit_ratings.parquet.gzip'),
            how='left', on=['gvkey', 'time_avail_m'])
        .assign(ret=lambda x: x['ret'].fillna(0)))

    for i in range(1, 6):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ret', 'ret_lag'+str(i), i)

    df['mom6m_junk'] = (1+df.filter(like='_lag')).prod(1, skipna=False) - 1
    df = (
        df.query('credrat<=14')
        .pipe(predictor_out_clean, 'mom6m_junk'))

    df.to_parquet(predictors_dir/'mom6m_junk.parquet.gzip', compression='gzip')

predictor_mom6m_junk()

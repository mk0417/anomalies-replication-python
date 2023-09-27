import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Enterprise Multiple
# ent_mult
# ---------------
@print_log
def predicotr_ent_mult():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'dltt',
                     'dlc', 'dc', 'che', 'oibdp', 'ceq'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='right', on=['permno', 'time_avail_m'])
        .assign(
            ent_mult=lambda x:
            (x['me']+x['dltt']+x['dlc']+x['dc']-x['che'])/x['oibdp']))
    df.loc[(df['ceq']<0) | (df['oibdp']<0), 'ent_mult'] = np.nan
    df = df.pipe(predictor_out_clean, 'ent_mult')

    df.to_parquet(predictors_dir/'ent_mult.parquet.gzip', compression='gzip')

predicotr_ent_mult()

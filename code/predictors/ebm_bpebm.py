import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Enterprise component of EBM
# ebm
#
# Leverage component of BM
# bpebm
# ---------------
@print_log
def predictor_ebm_bpebm():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'che', 'dltt',
                     'dlc', 'dc', 'dvpa', 'tstkp', 'ceq'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='right', on=['permno', 'time_avail_m'])
        .assign(
            tmp=lambda x:
            x['che']-x['dltt']-x['dlc']-x['dc']-x['dvpa']+x['tstkp'],
            ebm=lambda x: (x['ceq']+x['tmp'])/(x['me']+x['tmp']),
            bp=lambda x: (x['ceq']+x['tstkp']-x['dvpa'])/x['me'],
            bpebm=lambda x: x['bp']-x['ebm']))

    df_ebm = df.pipe(predictor_out_clean, 'ebm')
    df_bpebm = df.pipe(predictor_out_clean, 'bpebm')

    df_ebm.to_parquet(predictors_dir/'ebm.parquet.gzip', compression='gzip')
    df_bpebm.to_parquet(predictors_dir/'bpebm.parquet.gzip', compression='gzip')

predictor_ebm_bpebm()

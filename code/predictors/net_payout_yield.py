import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Net payout yield
# net_payout_yield
# ---------------
@print_log
def predictor_net_payout_yield():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'dvc',
                     'sstk', 'prstkc', 'sic', 'ceq'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='inner', on=['permno', 'time_avail_m'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'me', 'me_lag6', 6)
        .assign(
            net_payout_yield=lambda x:
            (x['dvc']+x['prstkc']-x['sstk'])/x['me_lag6'])
        .query('net_payout_yield!=0')
        .query('(sic<6000 | sic>=7000) & ceq>0')
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(n=lambda x: x.groupby('permno')['permno'].cumcount()+1)
        .query('n>=24')
        .pipe(predictor_out_clean, 'net_payout_yield'))

    df.to_parquet(
        predictors_dir/'net_payout_yield.parquet.gzip', compression='gzip')

predictor_net_payout_yield()

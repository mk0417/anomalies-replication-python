import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Percent total accruals
# pct_tot_acc
# ---------------
@print_log
def predictor_pct_tot_acc():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ni', 'prstkcc',
                     'sstk', 'dvt', 'oancf', 'fincf', 'ivncf'])
        .assign(
            pct_tot_acc=lambda x:
            (x['ni']
             -(x['prstkcc']-x['sstk']+x['dvt']+x['oancf']
               +x['fincf']+x['ivncf']))/x['ni'].abs())
        .pipe(predictor_out_clean, 'pct_tot_acc'))

    df.to_parquet(predictors_dir/'pct_tot_acc.parquet.gzip', compression='gzip')

predictor_pct_tot_acc()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Return on equity
# roe
# ---------------
@print_log
def predictor_roe():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ni', 'ceq'])
        .assign(roe=lambda x: x['ni']/x['ceq'])
        .pipe(predictor_out_clean, 'roe'))

    df.to_parquet(predictors_dir/'roe.parquet.gzip', compression='gzip')

predictor_roe()

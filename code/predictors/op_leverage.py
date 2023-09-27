import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Operating leverage
# op_leverage
# ---------------
@print_log
def predictor_op_leverage():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'xsga', 'cogs', 'at'])
        .assign(
            xsag=lambda x: x['xsga'].fillna(0),
            op_leverage=lambda x: (x['xsga']+x['cogs'])/x['at'])
        .pipe(predictor_out_clean, 'op_leverage'))

    df.to_parquet(predictors_dir/'op_leverage.parquet.gzip', compression='gzip')

predictor_op_leverage()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Gross profitability
# gp
# ---------------
@print_log
def predictor_gp():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'revt', 'cogs', 'at', 'sic'])
        .query('sic<6000 | sic>=7000')
        .assign(gp=lambda x: (x['revt']-x['cogs'])/x['at'])
        .pipe(predictor_out_clean, 'gp'))

    df.to_parquet(predictors_dir/'gp.parquet.gzip', compression='gzip')

predictor_gp()

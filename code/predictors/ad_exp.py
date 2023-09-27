import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Advertising Expenses
# ad_exp
# ---------------
@print_log
def predictor_ad_exp():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'xad'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='right', on=['permno', 'time_avail_m'])
        .assign(
            ad_exp=lambda x: x['xad']/x['me']))

    df.loc[df['xad']<=0, 'ad_exp'] = np.nan
    df = df.pipe(predictor_out_clean, 'ad_exp')

    df.to_parquet(predictors_dir/'ad_exp.parquet.gzip', compression='gzip')

predictor_ad_exp()

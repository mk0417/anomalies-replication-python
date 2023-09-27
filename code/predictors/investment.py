import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Investment
# investment
# ---------------
@print_log
def predictor_investment():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'capx', 'revt'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            tmp_invest=lambda x: x['capx']/x['revt'],
            tmp=lambda x:
            x.groupby('permno')['tmp_invest']
            .rolling(window=36, min_periods=24).mean().reset_index(drop=True),
            investment=lambda x: x['tmp_invest']/x['tmp']))
    # Replace with missing if revenue less than 10 million (units are millions)
    df.loc[df['revt']<10, 'investment'] = np.nan
    df = df.pipe(predictor_out_clean, 'investment')

    df.to_parquet(predictors_dir/'investment.parquet.gzip', compression='gzip')

predictor_investment()

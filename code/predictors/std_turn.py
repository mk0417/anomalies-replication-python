import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Turnover volatility
# std_turn
# ---------------
@print_log
def predictor_std_turn():
    df = (
        pd.read_parquet(
            download_dir/'crsp_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'vol', 'shrout', 'prc'])
        .assign(
            tmp=lambda x: x['vol']/x['shrout'],
            me=lambda x: x['shrout']*x['prc'].abs())
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            std_turn=lambda x:
            x.groupby('permno')['tmp']
            .rolling(window=36, min_periods=24).std().reset_index(drop=True))
        .pipe(port_group_cs, 5, 'all', 'time_avail_m', 'me')
        .assign(port=lambda x: (x['port'].str[1:]).astype(float)))
    df.loc[df['port']>=4, 'std_turn'] = np.nan
    df = df.pipe(predictor_out_clean, 'std_turn')

    df.to_parquet(predictors_dir/'std_turn.parquet.gzip', compression='gzip')

predictor_std_turn()

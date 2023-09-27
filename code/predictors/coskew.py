import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Coskewness of stock return wrt market return
# coskew
# ---------------
@print_log
def predictor_coskew():
    df = (
        pd.read_parquet(
            download_dir/'crsp_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        .merge(
            pd.read_parquet(
                download_dir/'ff_monthly.parquet.gzip',
                columns=['time_avail_m', 'mktrf', 'rf']),
            how='inner', on='time_avail_m')
        .assign(
            mkt=lambda x: x['mktrf'],
            ret=lambda x: x['ret']-x['rf'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            e_ret=lambda x:
            x.groupby('permno')['ret']
            .rolling(window=60, min_periods=12).mean().reset_index(drop=True),
            e_mkt=lambda x:
            x.groupby('permno')['mkt']
            .rolling(window=60, min_periods=12).mean().reset_index(drop=True),
            ret=lambda x: x['ret']-x['e_ret'],
            mkt=lambda x: x['mkt']-x['e_mkt'],
            ret2=lambda x: x['ret']**2,
            mkt2=lambda x: x['mkt']**2,
            ret_mkt2=lambda x: x['ret']*x['mkt2'],
            e_ret2=lambda x:
            x.groupby('permno')['ret2']
            .rolling(window=60, min_periods=12).mean().reset_index(drop=True),
            e_mkt2=lambda x:
            x.groupby('permno')['mkt2']
            .rolling(window=60, min_periods=12).mean().reset_index(drop=True),
            e_ret_mkt2=lambda x:
            x.groupby('permno')['ret_mkt2']
            .rolling(window=60, min_periods=12).mean().reset_index(drop=True),
            coskew=lambda x:
            x['e_ret_mkt2']/(np.sqrt(x['e_ret2'])*x['e_mkt2']))
        .pipe(predictor_out_clean, 'coskew'))

    df.to_parquet(predictors_dir/'coskew.parquet.gzip', compression='gzip')

predictor_coskew()

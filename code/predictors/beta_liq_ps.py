import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Pastor-Stambaugh liquidity beta
# beta_liq_ps
# ---------------
@print_log
def predictor_beta_liq_ps():
    df = (
        pd.read_parquet(
            download_dir/'crsp_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        .merge(pd.read_parquet(
            download_dir/'ff_monthly.parquet.gzip',
            columns=['time_avail_m', 'mktrf', 'hml', 'smb', 'rf']),
               how='inner', on='time_avail_m')
        .merge(pd.read_parquet(
            download_dir/'liq_factor_monthly.parquet.gzip'),
               how='left', on='time_avail_m')
        .assign(retrf=lambda x: x['ret']-x['rf'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .pipe(rolling_ols_parallel, 'retrf',
              ['ps_innov', 'mktrf', 'hml', 'smb'], 8, 60, 36)
        .rename(columns={'ps_innov': 'beta_liq_ps'})
        .pipe(predictor_out_clean, 'beta_liq_ps'))

    df.to_parquet(predictors_dir/'beta_liq_ps.parquet.gzip', compression='gzip')

predictor_beta_liq_ps()

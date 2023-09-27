import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# 6 month residual momentum based on FF3 residuals
# resid_mom6m_ff3
#
# 12 month residual momentum based on FF3 residuals
# resid_mom12m_ff3
# ---------------
@print_log
def predictor_resid_mom():
    df = (
        pd.read_parquet(
            download_dir/'crsp_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        .merge(
            pd.read_parquet(
                download_dir/'ff_monthly.parquet.gzip',
                columns=['time_avail_m', 'mktrf', 'hml', 'smb', 'rf']),
            how='inner', on='time_avail_m')
        .assign(retrf=lambda x: x['ret']-x['rf'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True))

    df_coef = (
        df.pipe(rolling_ols_parallel, 'ret', ['mktrf', 'smb', 'hml'], 8, 36, 36)
        .rename(columns={'mktrf': 'b_mkt', 'smb': 'b_smb', 'hml': 'b_hml'}))

    df = db.sql("""
            select a.permno, a.time_avail_m,
            a.ret-(b.const+b.b_mkt*a.mktrf+b.b_smb*a.smb+b.b_hml*a.hml) as resid
            from df a join df_coef b
            on a.permno=b.permno and a.time_avail_m=b.time_avail_m
        """).df()

    df = (
        df.pipe(shift_var_month, 'permno', 'time_avail_m', 'resid', 'tmp', 1)
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            mean6=lambda x:
            x.groupby('permno')['tmp']
            .rolling(window=6, min_periods=6).mean().reset_index(drop=True),
            std6=lambda x:
            x.groupby('permno')['tmp']
            .rolling(window=6, min_periods=6).std().reset_index(drop=True),
            mean12=lambda x:
            x.groupby('permno')['tmp']
            .rolling(window=11, min_periods=11).mean().reset_index(drop=True),
            std12=lambda x:
            x.groupby('permno')['tmp']
            .rolling(window=11, min_periods=11).std().reset_index(drop=True),
            resid_mom6m_ff3=lambda x: x['mean6']/x['std6'],
            resid_mom12m_ff3=lambda x: x['mean12']/x['std12']))

    df_6m = df.pipe(predictor_out_clean, 'resid_mom6m_ff3')
    df_12m = df.pipe(predictor_out_clean, 'resid_mom12m_ff3')

    df_6m.to_parquet(
        predictors_dir/'resid_mom6m_ff3.parquet.gzip', compression='gzip')
    df_12m.to_parquet(
        predictors_dir/'resid_mom12m_ff3.parquet.gzip', compression='gzip')

predictor_resid_mom()

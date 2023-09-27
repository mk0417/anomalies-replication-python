import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Frazzini-Pedersen beta
# beta_fp
# ---------------
@print_log
def predictor_beta_fp():
    df = pd.read_parquet(
        download_dir/'crsp_daily.parquet.gzip',
        columns=['permno', 'time_d', 'ret'])
    df_ff = pd.read_parquet(
        download_dir/'ff_daily.parquet.gzip',
        columns=['time_d', 'mktrf', 'rf'])
    df = db.sql("""
        select a.permno, last_day(a.time_d) as time_avail_m, a.time_d,
        ln(1+a.ret-b.rf) as logret, ln(1+b.mktrf) as logmkt
        from df a join df_ff b
        on a.time_d=b.time_d
        order by a.permno, a.time_d
        """).df()
    df['std_ret'] = (
        df.groupby('permno')['logret']
        .rolling(window=252, min_periods=120).std().reset_index(drop=True))
    df['std_mkt'] = (
        df.groupby('permno')['logmkt']
        .rolling(window=252, min_periods=120).std().reset_index(drop=True))

    df = df.sort_values(['permno', 'time_d'], ignore_index=True)
    df['logret_lag1'] = df.groupby('permno')['logret'].shift(1)
    df['logret_lag2'] = df.groupby('permno')['logret'].shift(2)
    df['logmkt_lag1'] = df.groupby('permno')['logmkt'].shift(1)
    df['logmkt_lag2'] = df.groupby('permno')['logmkt'].shift(2)
    df['ri'] = df['logret'] + df['logret_lag1'] + df['logret_lag2']
    df['rm'] = df['logmkt'] + df['logmkt_lag1'] + df['logmkt_lag2']

    df = df.sort_values(['permno', 'time_d'], ignore_index=True)
    df['rho'] = (
        df.groupby('permno')
        .apply(lambda x: pd.Series.rolling(
            x['ri'], window=1260, min_periods=750).corr(x['rm']))
        .reset_index(drop=True))

    df['beta_fp'] = df['rho'] * (df['std_ret']/df['std_mkt'])
    df = (
        df.sort_values(['permno', 'time_avail_m', 'time_d'], ignore_index=True)
        .drop_duplicates(['permno', 'time_avail_m'], keep='last')
        .pipe(predictor_out_clean, 'beta_fp'))

    df.to_parquet(predictors_dir/'beta_fp.parquet.gzip', compression='gzip')

predictor_beta_fp()

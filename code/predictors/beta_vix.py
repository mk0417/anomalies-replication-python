import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Systematic volatility
# beta_vix
# ---------------
def group_rolling_ols_d(group_data):
    # group_data is time-series data for each individual stock
    df_key, df = group_data
    df = df.sort_values('time_d', ignore_index=True)
    res = (
        RollingOLS(
            df['ret'], sm.add_constant(df[['mktrf', 'dvix']]),
            window=20, min_nobs=15, expanding=False)
        .fit(params_only=True).params)
    res.insert(0, 'permno', df_key)
    res.insert(1, 'time_d', df['time_d'])
    return res

def rolling_ols_parallel_d(data):
    df = (
        data.copy()
        .assign(
            n_missing=lambda d: d[['ret', 'mktrf', 'dvix']].isna().sum(axis=1),
            n=lambda d: d.groupby('permno')['n_missing']
            .transform(lambda s: (s==0).sum()))
        .query('n>=20')
        .sort_values(['permno', 'time_d'], ignore_index=True))
    res = Parallel(n_jobs=6)(
        delayed(group_rolling_ols_d)(i)
        for i in df.groupby('permno'))
    df = pd.concat(res, ignore_index=True)
    return df

@print_log
def predictor_beta_vix():
    df = pd.read_parquet(
        download_dir/'crsp_daily.parquet.gzip',
        columns=['permno', 'time_d', 'ret'])
    df_ff = pd.read_parquet(
        download_dir/'ff_daily.parquet.gzip',
        columns=['time_d', 'mktrf', 'rf'])
    df_vix = pd.read_parquet(
        download_dir/'vix.parquet.gzip',
        columns=['time_d', 'dvix'])
    df = db.sql("""
        select a.permno, last_day(a.time_d) as time_avail_m, a.time_d,
        a.ret-b.rf as ret, b.mktrf, c.dvix
        from df a
        join df_ff b on a.time_d=b.time_d
        join df_vix c on a.time_d=c.time_d
        order by a.permno, a.time_d
        """).df()

    df_coef = df.pipe(rolling_ols_parallel_d)
    df = db.sql("""
        select a.permno, last_day(a.time_d) as time_avail_m, a.time_d,
        b.dvix as beta_vix
        from df a join df_coef b
        on a.permno=b.permno and a.time_d=b.time_d
        """).df()

    df = (
        df.sort_values(['permno', 'time_avail_m', 'time_d'], ignore_index=True)
        .drop_duplicates(['permno', 'time_avail_m'], keep='last')
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .pipe(predictor_out_clean, 'beta_vix'))

    df.to_parquet(predictors_dir/'beta_vix.parquet.gzip', compression='gzip')

predictor_beta_vix()

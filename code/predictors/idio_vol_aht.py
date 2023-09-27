import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Idiosyncratic risk (AHT)
# idio_vol_aht
# ---------------
def group_rolling_ols_d(group_data):
    # group_data is time-series data for each individual stock
    df_key, df = group_data
    df = df.sort_values('time_d', ignore_index=True)
    est = (
        RollingOLS(
            df['ret'], sm.add_constant(df['mktrf']),
            window=252, min_nobs=100, expanding=False).fit())
    res = est.ssr.to_frame('ssr').join(est.nobs.to_frame('nobs'))
    res.insert(0, 'permno', df_key)
    res.insert(1, 'time_d', df['time_d'])
    return res

def rolling_ols_parallel_d(data):
    df = (
        data.copy()
        .assign(
            n_missing=lambda d: d[['ret', 'mktrf']].isna().sum(axis=1),
            n=lambda d: d.groupby('permno')['n_missing']
            .transform(lambda s: (s==0).sum()))
        .query('n>=252')
        .sort_values(['permno', 'time_d'], ignore_index=True))
    res = Parallel(n_jobs=6)(
        delayed(group_rolling_ols_d)(i)
        for i in df.groupby('permno'))
    df = pd.concat(res, ignore_index=True)
    return df

@print_log
def predictor_idio_vol_aht():
    df = pd.read_parquet(
        download_dir/'crsp_daily.parquet.gzip',
        columns=['permno', 'time_d', 'ret'])
    df_ff = pd.read_parquet(
        download_dir/'ff_daily.parquet.gzip',
        columns=['time_d', 'mktrf', 'rf'])
    df = db.sql("""
        select a.permno, a.time_d, a.ret-b.rf as ret, b.mktrf
        from df a join df_ff b
        on a.time_d=b.time_d
        order by a.permno, a.time_d
        """).df()

    df_coef = df.pipe(rolling_ols_parallel_d)
    df = db.sql("""
        select a.permno, last_day(a.time_d) as time_avail_m, a.time_d,
        sqrt(b.ssr/b.nobs) as idio_vol_aht
        from df a join df_coef b
        on a.permno=b.permno and a.time_d=b.time_d
        """).df()

    df = (
        df.sort_values(
            ['permno', 'time_avail_m', 'time_d'], ignore_index=True)
        .drop_duplicates(['permno', 'time_avail_m'], keep='last')
        .pipe(predictor_out_clean, 'idio_vol_aht'))

    df.to_parquet(
        predictors_dir/'idio_vol_aht.parquet.gzip', compression='gzip')

predictor_idio_vol_aht()

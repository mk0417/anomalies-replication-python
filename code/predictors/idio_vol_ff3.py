import sys
sys.path.insert(0, '../')
from functions import *
import scipy as sp
from pandarallel import pandarallel


# ---------------
# Realized (Total) Vol (Daily)
# realized_vol
#
# Idiosyncratic Risk (3 factor)
# idio_vol_ff3
#
# Skewness of daily idiosyncratic returns (3F model)
# return_skew_ff3
# ---------------
def ols_reg(data):
    # I experiment several linear regression algorithms
    # scipy seems to be slightly faster
    # https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.Ridge.html
    # https://github.com/scikit-learn/scikit-learn/issues/13923
    X = data[['mktrf', 'smb', 'hml']].values
    X = np.c_[np.ones(X.shape[0]), X]
    y = data['ret'].values
    # est = sp.linalg.lstsq(X, y, check_finite=False, lapack_driver='gelsy')[0]
    est = sp.sparse.linalg.lsqr(X, y)[0]
    return pd.Series(est)

def ff3_coef(data, ncpu, inc):
    pandarallel.initialize(nb_workers=ncpu)
    permno_list = list(data['permno'].unique())
    est_list = []
    for i in range(0, len(permno_list), inc):
        tmp_list = permno_list[i:i+inc]
        tmp = (
            data.query('permno==@tmp_list')
            .groupby(['permno', 'time_avail_m']).parallel_apply(ols_reg))
        est_list.append(tmp)

    est = pd.concat(est_list)
    return est

@print_log
def predictor_idio_vol_ff3(ncpu, inc):
    df_d = pd.read_parquet(
        download_dir/'crsp_daily.parquet.gzip',
        columns=['permno', 'time_d', 'ret'])
    df_ff = pd.read_parquet(
        download_dir/'ff_daily.parquet.gzip',
        columns=['time_d', 'mktrf', 'smb', 'hml', 'rf'])

    # More than 100 millions obs
    df = db.sql("""
            select a.permno, last_day(a.time_d) as time_avail_m, a.time_d,
                a.ret-b.rf as ret, b.mktrf, b.smb, b.hml
            from df_d a join df_ff b
            on a.time_d=b.time_d
            where ret not null
        """).df()

    df['n'] = df.groupby(['permno', 'time_avail_m'])['ret'].transform('count')
    df = df.query('n>=15')

    df_ff3 = ff3_coef(df, ncpu, inc).reset_index()
    df_ff3.columns = ['permno', 'time_avail_m',
                      'const', 'b_mkt', 'b_smb', 'b_hml']

    df = db.sql("""
        select permno, time_avail_m, stddev_samp(ret) as realized_vol,
            stddev_samp(resid) as idio_vol_ff3,
            skewness(resid) as return_skew_ff3
        from
        (select a.permno, a.time_avail_m, a.ret,
            a.ret-(b.const+b.b_mkt*a.mktrf+b.b_smb*a.smb+b.b_hml*a.hml) as resid
        from df a join df_ff3 b
        on a.permno=b.permno and a.time_avail_m=b.time_avail_m)
        group by permno, time_avail_m
        order by permno, time_avail_m
        """).df()

    df1 = df.pipe(predictor_out_clean, 'realized_vol')
    df2 = df.pipe(predictor_out_clean, 'idio_vol_ff3')
    df3 = df.pipe(predictor_out_clean, 'return_skew_ff3')

    df1.to_parquet(
        predictors_dir/'realized_vol.parquet.gzip', compression='gzip')
    df2.to_parquet(
        predictors_dir/'idio_vol_ff3.parquet.gzip', compression='gzip')
    df3.to_parquet(
        predictors_dir/'return_skew_ff3.parquet.gzip', compression='gzip')

predictor_idio_vol_ff3(6, 1000)

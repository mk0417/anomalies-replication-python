import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Coskewness following Ang, Chen, Xing 2006
# coskew_acx
# ---------------
def coskew_12month(data, month):
    df = data.copy()
    df.loc[df['time_d'].dt.month==month, 'time_avail_m'] = (
        df['time_d'] + pd.offsets.MonthEnd(0))
    df = df.sort_values(['permno', 'time_d'], ignore_index=True)
    df['time_avail_m'] = df.groupby('permno')['time_avail_m'].bfill()
    df = df.query('time_avail_m==time_avail_m').copy()
    df['e_ret'] = df.groupby(['permno', 'time_avail_m'])['ret'].transform('mean')
    df['e_mkt'] = df.groupby(['permno', 'time_avail_m'])['mkt'].transform('mean')
    df['ret'] = df['ret'] - df['e_ret']
    df['mkt'] = df['mkt'] - df['e_mkt']
    df['ret2'] = df['ret']**2
    df['mkt2'] = df['mkt']**2
    df['ret_mkt2'] = df['ret']*df['mkt2']
    df = db.sql("""
        select permno, time_avail_m, favg(ret_mkt2) as ret_mkt2,
        favg(ret2) as ret2, favg(mkt2) as mkt2, count(ret) as nobs,
        from df
        group by permno, time_avail_m
        """).df()
    df['coskew_acx'] = df['ret_mkt2'] / (np.sqrt(df['ret2'])*df['mkt2'])
    df['max_nobs'] = df.groupby('time_avail_m')['nobs'].transform('max')
    df = df.query('max_nobs-nobs<=5')
    return df

@print_log
def predictor_coskew_acx():
    df_raw = (
        pd.read_parquet(
            download_dir/'crsp_daily.parquet.gzip',
            columns=['permno', 'time_d', 'ret'])
        .merge(
            pd.read_parquet(
                download_dir/'ff_daily.parquet.gzip',
                columns=['time_d', 'mktrf', 'rf']),
            how='inner', on='time_d')
        .assign(
            mkt=lambda x: np.log(1+x['mktrf']+x['rf'])-np.log(1+x['rf']),
            ret=lambda x: np.log(1+x['ret'])-np.log(1+x['rf']))
        .drop(columns=['mktrf', 'rf']))

    df = pd.DataFrame()
    for i in range(1, 13):
        tmp = coskew_12month(df_raw, i)
        df = pd.concat([df, tmp], ignore_index=True)

    df = df.pipe(predictor_out_clean, 'coskew_acx')

    df.to_parquet(predictors_dir/'coskew_acx.parquet.gzip', compression='gzip')

predictor_coskew_acx()

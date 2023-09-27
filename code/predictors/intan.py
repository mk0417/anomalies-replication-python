import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Intangible return (BM)
# intan_bm
#
# Intangible return (SP)
# intan_sp
#
# Intangible return (CFP)
# intan_cfp
#
# Intangible return (EP)
# intan_ep
# ---------------
def ols_reg(data, x):
    df = data.copy()
    resid = (
        sm.OLS(
            df['ret60'], sm.add_constant(df[x]), missing='drop')
        .fit().resid)
    return resid

def cs_reg(data, x):
    df = (
        data.copy()
        .pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'acc_'+x, 'acc_'+x+'_lag60', 60))
    df[x+'_ret'] = df['acc_'+x] - df['acc_'+x+'_lag60'] + df['ret60']
    df = (
        df.dropna(subset=['ret60', 'acc_'+x+'_lag60', x+'_ret'], how='any')
        .sort_values(['time_avail_m', 'permno'], ignore_index=True))
    df['intan_'+x] = (
        df.groupby('time_avail_m')
        .apply(ols_reg, x=['acc_'+x+'_lag60', x+'_ret'])
        .reset_index(drop=True))
    df['intan_'+x] = df['intan_'+x].astype(float)
    df = df.pipe(predictor_out_clean, 'intan_'+x)
    return df

@print_log
def predictor_intan():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'sale',
                     'ib', 'dp', 'ni', 'ceq'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'ret', 'me']),
            how='inner', on=['permno', 'time_avail_m'])
        .assign(
            acc_bm=lambda x: x['ceq']/x['me'],
            acc_sp=lambda x: x['sale']/x['me'],
            acc_cfp=lambda x: (x['ib']+x['dp'])/x['me'],
            acc_ep=lambda x: x['ni']/x['me'],
            ret=lambda x: x['ret'].fillna(0),
            ret_plus=lambda x: 1+x['ret'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(cum_ret=lambda x: x.groupby('permno')['ret_plus'].cumprod())
        .pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'cum_ret', 'cum_ret_lag60', 60)
        .assign(
            ret60=lambda x:
            (x['cum_ret']-x['cum_ret_lag60'])/x['cum_ret_lag60']))
    df.loc[df['acc_bm']<=0, 'acc_bm'] = np.nan
    df['acc_bm'] = np.log(df['acc_bm'])
    p1, p99 = df['ret60'].quantile([0.01, 0.99])
    df = df.query('@p1<=ret60<=@p99')

    df_bm = cs_reg(df, 'bm')
    df_sp = cs_reg(df, 'sp')
    df_cfp = cs_reg(df, 'cfp')
    df_ep = cs_reg(df, 'ep')

    df_bm.to_parquet(predictors_dir/'intan_bm.parquet.gzip', compression='gzip')
    df_sp.to_parquet(predictors_dir/'intan_sp.parquet.gzip', compression='gzip')
    df_cfp.to_parquet(
        predictors_dir/'intan_cfp.parquet.gzip', compression='gzip')
    df_ep.to_parquet(predictors_dir/'intan_ep.parquet.gzip', compression='gzip')

predictor_intan()

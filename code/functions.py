import os
import yaml
import wrds
import pandas as pd
import numpy as np
import duckdb as db
from pathlib import Path
from datetime import datetime
import statsmodels.api as sm
from statsmodels.regression.rolling import RollingOLS
from joblib import Parallel, delayed
import time


data_dir = Path('../../data')
download_dir = data_dir/'data_download'
predictors_dir = data_dir/'predictors'
port_dir = data_dir/'portfolios'
for i in [data_dir, download_dir, predictors_dir, port_dir]:
    if not os.path.exists(i):
        os.makedirs(i)

def wrds_connect():
    try:
        pass_file = Path('~/.pass.yml').expanduser()
        with open(pass_file) as f:
            wrds_pass = yaml.safe_load(f)['wrds']
            wrds_username = wrds_pass['username']

        conn = wrds.Connection(wrds_username=wrds_username, autoconnect=False)
    except:
        conn = wrds.Connection(
            wrds_username='change to your wrds unsername', autoconnect=False)

    return conn

# Duckdb version: 50% faster than pandas
def shift_var_month(data, id_var, date_var, var, new_var, n):
    lead_lag = '+'
    # For lead
    if n < 0:
        lead_lag = '-'
        n = -1 * n

    df = data.copy()
    df = db.sql(f"""
        select a.*, b.{var} as {new_var}
        from df a left join df b
        on a.{id_var}=b.{id_var}
            and a.{date_var}=last_day(b.{date_var} {lead_lag} interval {n} month)
        order by a.{id_var}, a.{date_var}
        """).df()
    return df

def shift_var_year(data, id_var, date_var, var, new_var, n):
    lead_lag = '+'
    # For lead
    if n < 0:
        lead_lag = '-'
        n = -1 * n

    df = data.copy()
    df = db.sql(f"""
        select a.*, b.{var} as {new_var}
        from df a left join df b
        on a.{id_var}=b.{id_var}
            and a.{date_var}=last_day(b.{date_var} {lead_lag} interval {n} year)
        order by a.{id_var}, a.{date_var}
        """).df()
    return df

# Pandas version: slower than Duckdb
# def shift_var_month(data, id_var, date_var, var, new_var, n):
#     df = (
#         data[[id_var, date_var, var]].copy()
#         .rename(columns={var: new_var}))
#     df[date_var] = df[date_var] + pd.offsets.MonthEnd(n)
#     df = (
#         data.merge(df, how='left', on=[id_var, date_var])
#         .sort_values([id_var, date_var], ignore_index=True))
#     return df

def fill_inf_to_nan(data, predictor):
    df = data.copy()
    df.loc[df[predictor]==-np.inf, predictor] = np.nan
    df.loc[df[predictor]==np.inf, predictor] = np.nan
    return df

def predictor_out_clean(data, predictor):
    df = (
        data.copy()
        .pipe(fill_inf_to_nan, predictor))
    df = (
        df[df[predictor].notna()]
        .filter(['permno', 'time_avail_m', predictor])
        .drop_duplicates(['permno', 'time_avail_m'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True))
    return df

def group_rolling_ols(group_data, y, x, window, min_nobs):
    # group_data is time-series data for each individual stock
    # y is dependent variable: str
    # x is independent variable: list
    df_key, df = group_data
    df = df.sort_values('time_avail_m', ignore_index=True)
    res = (
        RollingOLS(
            df[y], sm.add_constant(df[x]),
            window=window, min_nobs=min_nobs, expanding=False)
        .fit(params_only=True).params)
    res.insert(0, 'permno', df_key)
    res.insert(1, 'time_avail_m', df['time_avail_m'])
    return res

def rolling_ols_parallel(data, y, x, ncpu, window, min_nobs):
    df = (
        data.copy()
        .assign(
            n_missing=lambda d: d[[y]+x].isna().sum(axis=1),
            n=lambda d: d.groupby('permno')['n_missing']
            .transform(lambda s: (s==0).sum()))
        .query('n>=@window')
        .sort_values(['permno', 'time_avail_m'], ignore_index=True))
    res = Parallel(n_jobs=ncpu)(
        delayed(group_rolling_ols)(i, y, x, window, min_nobs)
        for i in df.groupby('permno'))
    df = pd.concat(res, ignore_index=True)
    return df

def port_master_data(predictor, price_f=None, exchange_f=None, exfin_f=False):
    # Merge return and predictor data
    df = (
        # Return and stock meta information
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret',
                     'me', 'prc', 'exchcd', 'sic_crsp'])
        .rename(columns={'sic_crsp': 'sic'})
        .assign(
            prc=lambda x: x['prc'].abs(),
            ret=lambda x: x['ret']*100)
        .merge(
            # Predictor data
            pd.read_parquet(predictors_dir/(predictor+'.parquet.gzip')),
            how='right', on=['permno', 'time_avail_m'])
        # Get lagged information (predictor info on portfolio formation date)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'me', 'me_lag', 1)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'prc', 'prc_lag', 1)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'exchcd', 'exchcd_lag', 1)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'sic', 'sic_lag', 1)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              predictor, predictor+'_lag', 1)
        .filter(
            ['permno', 'time_avail_m', 'ret', 'me_lag', 'prc_lag',
             'exchcd_lag', 'sic_lag', predictor+'_lag'])
        # Require lagged information is available when we form portfolios
        #  market value
        #  listing exchange
        #  predictor
        .dropna(subset=['me_lag', 'exchcd_lag', predictor+'_lag'], how='any'))

    if price_f:
        df = df.query('prc_lag>=@price_f')
    if exchange_f:
        df = df.query('exchcd_lag==@exchange_f')
    if exfin_f:
        df = df.query('sic_lag<6000 | sic_lag>6999')

    end_date = '-'.join([str(datetime.today().year-1), '12', '31'])
    df = (
        df.query('time_avail_m<=@end_date')
        .sort_values(['permno', 'time_avail_m'], ignore_index=True))
    return df

def port_group_cs(data, n_port, bp, time_var, var):
    df = data.copy()
    pctls_list = [i/n_port for i in range(1, n_port)]
    pctls_labels = ['pctls'+str(int((i/n_port)*100)) for i in range(1, n_port)]
    if bp == 'all':
        pctls_df = df.copy()
    if bp == 'nyse':
        pctls_df = df.copy().query('exchcd_lag==1')

    pctls = (
        pctls_df.groupby(time_var)[var]
        .quantile(pctls_list).unstack()
        .rename(columns={i: j for i, j in zip(pctls_list, pctls_labels)})
        .reset_index())
    df = df.merge(pctls, how='left', on=time_var)

    port_labels = ['p'+str(i+1) for i in range(n_port)]
    df.loc[df[var]<=df[pctls_labels[0]], 'port'] = port_labels[0]
    for i, j, k in zip(pctls_labels[:-1], pctls_labels[1:], port_labels[1:-1]):
        df['port'] = np.where((df[var]>df[i]) & (df[var]<=df[j]), k, df['port'])

    df.loc[df[var]>df[pctls_labels[-1]], 'port'] = port_labels[-1]
    df.loc[df['port']=='na', 'port'] = np.nan
    df = df[list(df.columns[~df.columns.str.contains('pctls')])]
    return df

def port_monthly_ret(data, predictor, n_port, weight, bp):
    df = (
        data.copy()
        .pipe(port_group_cs, n_port, bp, 'time_avail_m', predictor+'_lag')
        .sort_values(['permno', 'time_avail_m'], ignore_index=True))

    if weight == 'ew':
        df = df.groupby(['time_avail_m', 'port'])['ret'].mean().unstack()
    if weight == 'vw':
        df['w'] = (
            df.groupby(['time_avail_m', 'port'])['me_lag']
            .transform(lambda x: x/x.sum(min_count=1)))
        df['retw'] = df['ret'] * df['w']
        df = (
            df.groupby(['time_avail_m', 'port'])['retw']
            .sum(min_count=1).unstack())

    port_labels = ['p'+str(i+1) for i in range(n_port)]
    df = (
        df.filter(port_labels)
        .assign(ls_ret=df['p'+str(n_port)] - df['p1'])
        .reset_index()
        .rename(columns={'time_avail_m': 'date'})
        .sort_values('date', ignore_index=True))
    return df

def port_ret(data, n_port):
    df = data.copy().set_index('date')
    t = df['ls_ret'].mean()/(df['ls_ret'].std()/np.sqrt(df['ls_ret'].count()))
    port_labels = ['p'+str(i+1) for i in range(n_port)]
    sort_map = {
        i: j for i, j in zip(
            port_labels+['ls_ret'], [i for i in range(n_port+1)])}
    df = (
        df.mean().to_frame('ret')
        .sort_index(key=lambda x: x.map(sort_map))
        .reset_index())
    df.loc[n_port+1]  = ['t', t]
    return df

def print_log(func):
    def wrapper(*args, **kwargs):
        print(f'[{func.__name__}] processing ...')
        start_time = time.time()
        res = func(*args, **kwargs)
        end_time = time.time()
        time_taken = end_time - start_time
        if time_taken >= 60:
            print(f'[{func.__name__}] took {time_taken/60:.1f}mins\n')
        else:
            print(f'[{func.__name__}] took {time_taken:.1f}s\n')

        return res
    return wrapper

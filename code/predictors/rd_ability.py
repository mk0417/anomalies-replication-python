import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# R&D ability
# rd_ability
# ---------------
def group_rolling_ols_y(group_data, x):
    # group_data is time-series data for each individual stock
    df_key, df = group_data
    df = df.sort_values('fyear', ignore_index=True)
    res = (
        RollingOLS(
            df['tmp_y'], sm.add_constant(df[x]),
            window=8, min_nobs=6, expanding=False)
        .fit(params_only=True).params[[x]])
    res.insert(0, 'gvkey', df_key)
    res.insert(1, 'fyear', df['fyear'])
    return res

def rolling_ols_parallel_y(data, x):
    df = (
        data.copy()
        .pipe(fill_inf_to_nan, 'tmp_y')
        .pipe(fill_inf_to_nan, x)
        .assign(
            n_missing=lambda d: d[['tmp_y', x]].isna().sum(axis=1),
            n=lambda d: d.groupby('gvkey')['n_missing']
            .transform(lambda s: (s==0).sum()))
        .query('n>=8')
        .sort_values(['gvkey', 'fyear'], ignore_index=True))
    res = Parallel(n_jobs=6)(
        delayed(group_rolling_ols_y)(i, x)
        for i in df.groupby('gvkey'))
    df = pd.concat(res, ignore_index=True)
    return df

@print_log
def predictor_rd_ability():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_annual.parquet.gzip',
            columns=['permno', 'time_avail_m', 'fyear',
                     'datadate', 'xrd', 'sale', 'gvkey']))
    df['tmp_xrd'] = np.where(df['xrd']<0, np.nan, df['xrd'])
    df['tmp_sale'] = np.where(df['sale']<0, np.nan, df['sale'])
    df = df.pipe(
        shift_var_year, 'gvkey', 'time_avail_m',
        'tmp_sale', 'tmp_sale_lag1', 1)
    df['tmp_y'] = df['tmp_sale'] / df['tmp_sale_lag1']
    df['tmp_x'] = 1 + df['tmp_xrd']/df['tmp_sale']
    df.loc[df['tmp_y']<=0, 'tmp_y'] = np.nan
    df.loc[df['tmp_x']<=0, 'tmp_x'] = np.nan
    df['tmp_y'] = np.log(df['tmp_y'])
    df['tmp_x'] = np.log(df['tmp_x'])

    for i in range(1, 6):
        df = df.pipe(
            shift_var_year, 'gvkey', 'time_avail_m',
            'tmp_x', 'tmp_x_lag'+str(i), i)
        df['tmp_non_zero'] = np.where(df['tmp_x_lag'+str(i)]>0, 1, 0)
        df = df.sort_values(['gvkey', 'fyear'], ignore_index=True)
        df['tmp_mean'] = (
            df.groupby('gvkey')['tmp_non_zero']
            .rolling(window=8, min_periods=6).mean().reset_index(drop=True))
        tmp_coef = (
            df.pipe(rolling_ols_parallel_y, 'tmp_x_lag'+str(i))
            .rename(columns={'tmp_x_lag'+str(i): 'gamma_ability'+str(i)}))
        df = df.merge(tmp_coef, how='left', on=['gvkey', 'fyear'])
        df.loc[df['tmp_mean']<0.5, 'gamma_ability'+str(i)] = np.nan

    df['rd_ability'] = df.filter(like='gamma_').mean(1)
    df['tmp_rd'] = df['xrd'] / df['sale']
    df.loc[df['xrd']<=0, 'tmp_rd'] = np.nan

    df = df.pipe(port_group_cs, 3, 'all', 'time_avail_m', 'tmp_rd')
    df.loc[df['port']!='p3', 'rd_ability'] = np.nan
    df.loc[df['xrd']<=0, 'rd_ability'] = np.nan

    df = (
        pd.concat([df]*12)
        .sort_values(['gvkey', 'time_avail_m'], ignore_index=True)
        .assign(
            month_inc=lambda x:
            x.groupby(['gvkey', 'time_avail_m'])['gvkey'].cumcount(),
            time_avail_m=lambda x:
            (x['time_avail_m'].dt.to_period('M')+x['month_inc'])
            .dt.to_timestamp()+pd.offsets.MonthEnd(0))
        .drop(columns='month_inc')
        .sort_values(['gvkey', 'time_avail_m', 'datadate'], ignore_index=True)
        .drop_duplicates(['gvkey', 'time_avail_m'], keep='last')
        .pipe(predictor_out_clean, 'rd_ability'))

    df.to_parquet(predictors_dir/'rd_ability.parquet.gzip', compression='gzip')

predictor_rd_ability()

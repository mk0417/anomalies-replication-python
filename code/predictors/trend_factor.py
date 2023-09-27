import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Trend factor
# trend_factor
# ---------------
def cs_reg(data, x):
    df = data.copy()
    df = (
        sm.OLS(df['ret'], sm.add_constant(df[x]), missing='drop')
        .fit().params)
    return df

@print_log
def predictor_trend_factor():
    # 1. Compute moving averages
    df_ma = (
        pd.read_parquet(
            download_dir/'crsp_daily.parquet.gzip',
            columns=['permno', 'time_d', 'prc', 'cfacpr']))
    df_ma = db.sql("""
        select permno, last_day(time_d) as time_avail_m,
        time_d, abs(prc)/cfacpr as p
        from df_ma
        order by permno, time_d
        """).df()

    lag_list = [3, 5, 10, 20, 50, 100, 200, 400, 600, 800, 1000]
    for i in lag_list:
        df_ma['a_'+str(i)] = (
            df_ma.groupby('permno')['p']
            .rolling(window=i, min_periods=1).mean().reset_index(drop=True))

    df_ma = (
        df_ma.sort_values(
            ['permno', 'time_avail_m', 'time_d'], ignore_index=True)
        .drop_duplicates(['permno', 'time_avail_m'], keep='last'))

    for i in lag_list:
        df_ma['a_'+str(i)] = df_ma['a_'+str(i)] / df_ma['p']

    # 2. Run cross-sectional regressions on monthly data
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret', 'exchcd', 'shrcd'])
        .query('exchcd==[1, 2, 3] & shrcd==[10, 11]')
        .merge(df_ma, how='inner', on=['permno', 'time_avail_m'])
        .drop(columns=['exchcd', 'shrcd']))

    x_var = ['a_'+str(i) for i in lag_list]
    for i in x_var:
        df = df.pipe(shift_var_month, 'permno', 'time_avail_m', i, i+'_lag', 1)

    x_var_lag = [i+'_lag' for i in x_var]
    df_coef = (
        df.copy()
        .dropna(subset=['ret']+x_var_lag, how='any')
        .assign(
            tmp_std=lambda x:
            x.groupby('time_avail_m')['a_3_lag'].transform('std'))
        .query('tmp_std>0')
        .groupby('time_avail_m').apply(cs_reg, x=x_var_lag).reset_index()
        .sort_values('time_avail_m', ignore_index=True))

    for i in lag_list:
        df_coef = df_coef.sort_values('time_avail_m', ignore_index=True)
        df_coef['e_beta_'+str(i)] = (
            df_coef['a_'+str(i)+'_lag'].rolling(window=12, min_periods=1).mean())

    df_coef = df_coef.drop(columns=x_var_lag)
    df = (
        df.merge(df_coef, how='left', on='time_avail_m')
        .eval("""
        trend_factor=e_beta_3*a_3 + e_beta_5*a_5 + e_beta_10*a_10 \
        + e_beta_20*a_20 + e_beta_50*a_50 + e_beta_100*a_100 \
        + e_beta_200*a_200 + e_beta_400*a_400 + e_beta_600*a_600 \
        + e_beta_800*a_800 + e_beta_1000*a_1000
        """)
        .pipe(predictor_out_clean, 'trend_factor'))

    df.to_parquet(predictors_dir/'trend_factor.parquet.gzip', compression='gzip')

predictor_trend_factor()

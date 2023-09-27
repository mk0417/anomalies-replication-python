import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Abnormal Accruals
# abnormal_accruals
#
# Abnormal Accruals (Percent)
# abnormal_accruals_pct
# ---------------
def ols_reg(data):
    df = data.copy()
    est = (
        sm.OLS(df['accruals'], sm.add_constant(df[['inv_ta', 'del_rev', 'ppe']]))
        .fit()
        .params)
    return est

@print_log
def predictor_abnormal_accruals():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_annual.parquet.gzip',
            columns=['permno', 'time_avail_m', 'fyear', 'datadate',
                     'at', 'oancf', 'fopt', 'act', 'che', 'lct',
                     'dlc', 'ib', 'sale', 'ppegt', 'ni', 'sic', 'gvkey'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'exchcd']),
            how='left', on=['permno', 'time_avail_m'])
        .assign(
            cfo=lambda x: x['oancf'],
            sic2=lambda x: x['sic']//100))

    for i in ['act', 'che', 'lct', 'dlc', 'at', 'sale']:
        df = df.pipe(shift_var_year, 'gvkey', 'time_avail_m', i, i+'_lag1', 1)

    df.loc[df['cfo'].isna(), 'cfo'] = (
        df['fopt']-(df['act']-df['act_lag1'])+(df['che']-df['che_lag1'])
        +(df['lct']-df['lct_lag1'])-(df['dlc']-df['dlc_lag1']))
    df['accruals'] = (df['ib']-df['cfo']) / df['at_lag1']
    df['inv_ta'] = 1 / df['at_lag1']
    df['del_rev'] = (df['sale']-df['sale_lag1']) / df['at_lag1']
    df['ppe'] = df['ppegt'] / df['at_lag1']

    for i in ['cfo', 'accruals', 'inv_ta', 'del_rev', 'ppe']:
        pctls = (
            df.groupby('fyear')[i].quantile([0.001, 0.999])
            .unstack().reset_index())
        df = df.merge(pctls, how='left', on='fyear')
        df.loc[(df[i]<df[0.001]) | (df[i]>df[0.999]), i] = np.nan
        df = df.drop(columns=[0.001, 0.999])

    df_coef = (
        df.copy()
        .dropna(subset=['accruals', 'inv_ta', 'del_rev', 'ppe'], how='any')
        .assign(
            n=lambda x:
            x.groupby(['sic2', 'fyear'])['gvkey'].transform('count'))
        .query('n>=6')
        .groupby(['sic2', 'fyear']).apply(ols_reg).reset_index().
        rename(
            columns={'inv_ta': 'b_inv_ta', 'del_rev':
                     'b_del_rev', 'ppe': 'b_ppe'}))

    df = db.sql("""
        select a.*,
            a.accruals
            -(b.const+b.b_inv_ta*a.inv_ta+b.b_del_rev*a.del_rev+b.b_ppe*a.ppe)
            as abnormal_accruals
        from df a join df_coef b
        on a.sic2=b.sic2 and a.fyear=b.fyear
        where a.exchcd!=3 or a.fyear>=1982
        order by a.permno, a.fyear, a.datadate
        """).df()

    df = (
        df.copy().drop_duplicates(['permno', 'fyear'], keep='last')
        .drop(columns='at_lag1')
        .pipe(shift_var_year, 'permno', 'time_avail_m', 'at', 'at_lag1', 1)
        .assign(
            abnormal_accruals_pct=lambda x:
            x['abnormal_accruals']*x['at_lag1']/x['ni'].abs()))

    df = (pd.concat([df]*12)
          .sort_values(['gvkey', 'time_avail_m'], ignore_index=True)
          .assign(
              month_inc=lambda x:
              x.groupby(['gvkey', 'time_avail_m'])['gvkey'].cumcount(),
              time_avail_m=lambda x: (
                  x['time_avail_m'].dt.to_period('M')+x['month_inc'])
              .dt.to_timestamp() + pd.offsets.MonthEnd(0))
          .drop(columns='month_inc')
          .sort_values(['gvkey', 'time_avail_m', 'datadate'], ignore_index=True)
          .drop_duplicates(['gvkey', 'time_avail_m'], keep='last')
          .sort_values(['permno', 'time_avail_m', 'datadate'], ignore_index=True)
          .drop_duplicates(['permno', 'time_avail_m'], keep='last')
          .sort_values(['permno', 'time_avail_m'], ignore_index=True))

    df1 = df.pipe(predictor_out_clean, 'abnormal_accruals')
    df2 = df.pipe(predictor_out_clean, 'abnormal_accruals_pct')

    df1.to_parquet(
        predictors_dir/'abnormal_accruals.parquet.gzip', compression='gzip')
    df2.to_parquet(
        predictors_dir/'abnormal_accruals_pct.parquet.gzip', compression='gzip')

predictor_abnormal_accruals()

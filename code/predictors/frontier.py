import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Efficient frontier index
# frontier
# ---------------
def ols_reg(data, x):
    df = data.copy()
    pred = (
        sm.OLS(
            df['me'],
            sm.add_constant(df[x+['ind'+str(i) for i in range(1, 50)]]),
            missing='drop')
        .fit().predict())
    return pred

@print_log
def predictor_frontier():
    df_raw = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'me', 'sic_crsp'])
        .merge(
            pd.read_parquet(
                download_dir/'compustat_a_monthly.parquet.gzip',
                columns=['permno', 'time_avail_m', 'at', 'ceq', 'dltt',
                         'capx', 'sale', 'xrd', 'xad', 'ppent', 'ebitda']),
            how='left', on=['permno', 'time_avail_m'])
        .assign(
            xad=lambda x: x['xad'].fillna(0)))
    df_raw.loc[df_raw['me']<=0, 'me'] = np.nan
    df_raw.loc[df_raw['ceq']<=0, 'ceq'] = np.nan
    df_raw['me'] = np.log(df_raw['me'])
    df_raw['ceq'] = np.log(df_raw['ceq'])
    df_raw = df_raw.eval("""
        lt_debt=dltt/at
        capx=capx/sale
        rd=xrd/sale
        adv=xad/sale
        ppe=ppent/at
        ebit=ebitda/at
        """)

    x_var = ['ceq', 'lt_debt', 'capx', 'rd', 'adv', 'ppe', 'ebit']
    for i in x_var:
        df_raw = df_raw.pipe(fill_inf_to_nan, i)

    df_raw = df_raw.dropna(subset=['me', 'sic_crsp']+x_var, how='any')

    ff_ind49 = pd.read_parquet(download_dir/'ff_ind49.parquet.gzip')
    for _, i in ff_ind49.iterrows():
        df_raw.loc[df_raw['sic_crsp']
                   .between(i['sic1'], i['sic2'], inclusive='both'),
                   'ff_ind'] = i['ind49']

    df_raw.loc[df_raw['ff_ind'].isna(), 'ff_ind'] = 49
    for i in range(1, 50):
        df_raw['ind'+str(i)] = np.where(df_raw['ff_ind']==i, 1, 0)

    month_list = (
        df_raw.copy().drop_duplicates('time_avail_m')[['time_avail_m']]
        .sort_values('time_avail_m', ignore_index=True)
        .assign(t=lambda x: x.index+1))
    df_raw = df_raw.merge(month_list, how='inner', on='time_avail_m')

    df = pd.DataFrame()
    for i in range(1, len(month_list)+1):
        tmp = df_raw.copy().query('@i-60<t<=@i')
        tmp['pred'] = ols_reg(tmp, x_var)
        tmp = tmp.query('t==@i')
        df = pd.concat([df, tmp])

    df['frontier'] = -1 * (df['ceq']-df['pred'])
    df = (
        df.query('ceq>=0')
        .pipe(predictor_out_clean, 'frontier'))

    df.to_parquet(predictors_dir/'frontier.parquet.gzip', compression='gzip')

predictor_frontier()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Organizational capital without industry adjustment
# org_cap_noadj
#
# Organizational capital
# org_cap
# ---------------
@print_log
def predictor_org_cap():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'sic_crsp', 'shrcd', 'exchcd'])
        .merge(
            pd.read_parquet(
                download_dir/'compustat_a_monthly.parquet.gzip',
                columns=['permno', 'time_avail_m',
                         'xsga', 'at', 'datadate', 'sic']),
            how='left', on=['permno', 'time_avail_m'])
        .merge(
            pd.read_parquet(download_dir/'gnp_deflator.parquet.gzip'),
            how='inner', on='time_avail_m')
        .assign(month=lambda x: x['datadate'].dt.month)
        .query('month==12 & (sic<6000 | sic>=7000)')
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            n=lambda x:
            x.groupby('permno')['permno'].cumcount()+1,
            xsga=lambda x: x['xsga'].fillna(0))
        .assign(xsga=lambda x: x['xsga']/x['gnp_defl']))

    df.loc[df['n']<=12, 'org_cap_noadj'] = 4 * df['xsga']
    df = df.pipe(
        shift_var_month, 'permno', 'time_avail_m',
        'org_cap_noadj', 'org_cap_noadj_lag12', 12)
    df.loc[df['n']>12, 'org_cap_noadj'] = (
        0.85 * df['org_cap_noadj_lag12'] + df['xsga'])
    df['org_cap_noadj'] = df['org_cap_noadj'] / df['at']
    df.loc[df['org_cap_noadj']==0, 'org_cap_noadj'] = np.nan

    pctls = (
        df.groupby('time_avail_m')['org_cap_noadj']
        .quantile([0.01, 0.99]).unstack().reset_index())
    df = df.merge(pctls, how='left', on='time_avail_m')
    df.loc[df['org_cap_noadj']<df[0.01], 'org_cap_noadj'] = df[0.01]
    df.loc[df['org_cap_noadj']>df[0.99], 'org_cap_noadj'] = df[0.99]

    ff_ind17 = pd.read_parquet(download_dir/'ff_ind17.parquet.gzip')
    for _, i in ff_ind17.iterrows():
        df.loc[df['sic_crsp']
               .between(i['sic1'], i['sic2'], inclusive='both'),
               'ff_ind'] = i['ind17']

    df.loc[df['ff_ind'].isna(), 'ff_ind'] = 17

    df['tmp_mean'] = (
        df.groupby(['ff_ind', 'time_avail_m'])
        ['org_cap_noadj'].transform('mean'))
    df['tmp_std'] = (
        df.groupby(['ff_ind', 'time_avail_m'])
        ['org_cap_noadj'].transform('std'))

    df['org_cap'] = (df['org_cap_noadj']-df['tmp_mean']) / df['tmp_std']

    df_noadj = df.pipe(predictor_out_clean, 'org_cap_noadj')
    df_adj = df.pipe(predictor_out_clean, 'org_cap')

    df_noadj.to_parquet(
        predictors_dir/'org_cap_noadj.parquet.gzip', compression='gzip')
    df_adj.to_parquet(predictors_dir/'org_cap.parquet.gzip', compression='gzip')

predictor_org_cap()

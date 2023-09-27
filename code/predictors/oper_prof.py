import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Operating profitability
# oper_prof
# ---------------
@print_log
def predictor_oper_prof():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'me', 'gvkey'])
        .query('gvkey==gvkey')
        .merge(
            pd.read_parquet(
                download_dir/'compustat_a_monthly.parquet.gzip',
                columns=['gvkey', 'time_avail_m', 'revt',
                         'cogs', 'xsga', 'xint', 'ceq']),
            how='inner', on=['gvkey', 'time_avail_m'])
        .assign(
            oper_prof=lambda x:
            (x['revt']-x['cogs']-x['xsga']-x['xint'])/x['ceq'])
        .pipe(port_group_cs, 3, 'all', 'time_avail_m', 'me'))
    df.loc[df['port']=='p1', 'oper_prof'] = np.nan
    df = df.pipe(predictor_out_clean, 'oper_prof')

    df.to_parquet(predictors_dir/'oper_prof.parquet.gzip', compression='gzip')

predictor_oper_prof()

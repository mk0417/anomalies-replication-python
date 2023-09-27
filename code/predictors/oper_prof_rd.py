import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Operating profitability R&D adjusted
# oper_prof_rd
# ---------------
@print_log
def predictor_oper_prof_rd():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'me',
                     'exchcd', 'sic_crsp', 'shrcd'])
        .merge(
            pd.read_parquet(
                download_dir/'compustat_a_monthly.parquet.gzip',
                columns=['permno', 'time_avail_m', 'revt', 'xrd',
                         'cogs', 'xsga', 'at', 'ceq']),
            how='left', on=['permno', 'time_avail_m'])
        .assign(
            xrd=lambda x: x['xrd'].fillna(0),
            oper_prof_rd=lambda x:
            (x['revt']-x['cogs']-x['xsga']+x['xrd'])/x['at'])
        .query(
            'shrcd<=11 & me==me & ceq==ceq & at==at \
            & (sic_crsp<6000 | sic_crsp>=7000)')
        .pipe(predictor_out_clean, 'oper_prof_rd'))

    df.to_parquet(predictors_dir/'oper_prof_rd.parquet.gzip', compression='gzip')

predictor_oper_prof_rd()

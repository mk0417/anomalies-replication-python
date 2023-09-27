import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in net operating assets
# dnoa
# ---------------
@print_log
def predictor_dnoa():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'che',
                     'dltt', 'dlc', 'mib', 'pstk', 'ceq']))

    for i in ['dltt', 'dlc', 'mib', 'pstk']:
        df[i] = df[i].fillna(0)

    df = (
        df.assign(
            oa=lambda x: x['at']-x['che'],
            ol=lambda x: x['at']-x['dltt']-x['mib']-x['dlc']-x['pstk']-x['ceq'],
            noa=lambda x: x['oa']-x['ol'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'noa', 'noa_lag12', 12)
        .assign(dnoa=lambda x: (x['noa']-x['noa_lag12'])/x['at_lag12'])
        .pipe(predictor_out_clean, 'dnoa'))

    df.to_parquet(predictors_dir/'dnoa.parquet.gzip', compression='gzip')

predictor_dnoa()

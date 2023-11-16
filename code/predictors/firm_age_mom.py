import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Firm age - momentum
# firm_age_mom
# ---------------
@print_log
def predictor_firm_age_mom():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret', 'prc'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            tmp=lambda x: x.groupby('permno')['permno'].cumcount() + 1,
            ret=lambda x: x['ret'].fillna(0)))

    for i in range(1, 6):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ret', 'ret_lag'+str(i), i)

    df['firm_age_mom'] = (1+df.filter(like='_lag')).prod(1, skipna=False) - 1
    df.loc[(df['prc'].abs()<5) & (df['tmp']<12), 'firm_age_mom'] = np.nan
    df = (
        df.pipe(port_group_cs, 5, 'all', 'time_avail_m', 'tmp')
        .assign(port=lambda x: (x['port'].str[1:]).astype(float))
        .sort_values(['permno', 'time_avail_m']))
    df.loc[df['port']>1, 'firm_age_mom'] = np.nan
    df = df.pipe(predictor_out_clean, 'firm_age_mom')

    df.to_parquet(predictors_dir/'firm_age_mom.parquet.gzip', compression='gzip')

predictor_firm_age_mom()

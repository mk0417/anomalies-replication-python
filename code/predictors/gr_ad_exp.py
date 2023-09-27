import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Growth in advertising expenses
# gr_ad_exp
# ---------------
@print_log
def predictor_gr_ad_exp():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'xad'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='left', on=['permno', 'time_avail_m']))
    df.loc[df['xad']<=0, 'xad'] = np.nan
    df = (
        df.pipe(
            shift_var_month, 'permno', 'time_avail_m', 'xad', 'xad_lag12', 12)
        .assign(gr_ad_exp=lambda x: np.log(x['xad']/x['xad_lag12']))
        .pipe(port_group_cs, 10, 'all', 'time_avail_m', 'me'))
    df.loc[(df['xad']<0.1) | (df['port']=='p1'), 'gr_ad_exp'] = np.nan
    df = df.pipe(predictor_out_clean, 'gr_ad_exp')

    df.to_parquet(predictors_dir/'gr_ad_exp.parquet.gzip', compression='gzip')

predictor_gr_ad_exp()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Book-to-market, original, Stattman (1980)
# see https://github.com/OpenSourceAP/CrossSection/issues/126
# bm
# ---------------
@print_log
def predictor_bm():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ceqt', 'datadate'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='right', on=['permno', 'time_avail_m'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'me', 'me_lag6', 6)
        .pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'time_avail_m', 'time_lag6', 6))
    df.loc[df['time_lag6']!=(df['datadate']+pd.offsets.MonthEnd(0)),
           'me_lag6'] = np.nan
    df = df.sort_values(['permno', 'time_avail_m'], ignore_index=True)
    df['me_lag6'] = df.groupby('permno')['me_lag6'].ffill()
    df['bm'] = df['ceqt'] / df['me_lag6']
    df.loc[df['bm']<=0, 'bm'] = np.nan
    df['bm'] = np.log(df['bm'])
    df = df.pipe(predictor_out_clean, 'bm')

    df.to_parquet(predictors_dir/'bm.parquet.gzip', compression='gzip')

predictor_bm()

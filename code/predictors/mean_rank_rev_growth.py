import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Average revenue growth
# mean_rank_rev_growth
# ---------------
@print_log
def predictor_mean_rank_rev_growth():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'revt']))
    df.loc[df['revt']<=0, 'revt'] = np.nan
    df = (
        df.pipe(
            shift_var_month, 'permno', 'time_avail_m', 'revt', 'revt_lag12', 12)
        .assign(
            tmp=lambda x: np.log(x['revt'])-np.log(x['revt_lag12']),
            tmp_rank=lambda x:
            x.groupby('time_avail_m')['tmp'].rank(ascending=False)))

    for i in range(12, 61, 12):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'tmp_rank', 'tmp_rank_lag'+str(i), i)

    df['mean_rank_rev_growth'] = (
        (5*df['tmp_rank_lag12'] + 4*df['tmp_rank_lag24']
         + 3*df['tmp_rank_lag36'] + 2*df['tmp_rank_lag48']
         + df['tmp_rank_lag60'])/15)
    df = df.pipe(predictor_out_clean, 'mean_rank_rev_growth')

    df.to_parquet(
        predictors_dir/'mean_rank_rev_growth.parquet.gzip', compression='gzip')

predictor_mean_rank_rev_growth()

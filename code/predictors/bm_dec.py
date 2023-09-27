import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Book-to-market (December market equity)
# bm_dec
# ---------------
@print_log
def predictor_bm_dec():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'seq', 'ceq', 'at',
                     'lt', 'pstk', 'pstkrv', 'pstkl', 'txditc'])
        .merge(
            pd.read_parquet(
                download_dir/'crsp_monthly.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='inner', on=['permno', 'time_avail_m'])
        .assign(
            year=lambda x: x['time_avail_m'].dt.year,
            month=lambda x: x['time_avail_m'].dt.month))

    df.loc[df['month']!=12, 'me'] = np.nan
    df['me_dec'] = df.groupby(['permno', 'year'])['me'].transform(min)

    df['txditc'] = df['txditc'].fillna(0)
    df['ps'] = df['pstk']
    df['ps'] = np.where(df['ps'].isna(), df['pstkrv'], df['ps'])
    df['ps'] = np.where(df['ps'].isna(), df['pstkl'], df['ps'])
    df['se'] = df['seq']
    df['se'] = np.where(df['se'].isna(), df['ceq']+df['ps'], df['se'])
    df['se'] = np.where(df['se'].isna(), df['at']-df['lt'], df['se'])
    df['be'] = df['se'] + df['txditc'] - df['ps']

    df = (
        df.pipe(shift_var_month, 'permno', 'time_avail_m',
                'me_dec', 'me_dec_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'me_dec', 'me_dec_lag17', 17))
    df['bm_dec'] = np.where(
        df['month']>=6, df['be']/df['me_dec_lag12'], df['be']/df['me_dec_lag17'])
    df = df.pipe(predictor_out_clean, 'bm_dec')

    df.to_parquet(predictors_dir/'bm_dec.parquet.gzip', compression='gzip')

predictor_bm_dec()

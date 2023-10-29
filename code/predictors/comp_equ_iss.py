import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Composite equity issuance
# comp_equ_iss
# ---------------
@print_log
def predicor_comp_equ_iss():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret', 'me'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'me', 'me_lag60', 60)
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            tmp_me=lambda x: x['me']/x['me_lag60'],
            tmp=lambda x: x.groupby(['permno'])['permno'].cumcount()+1))
    df['tmp'] = df['tmp'].astype(float)
    df.loc[df['tmp']>1, 'tmp'] = 1 + df['ret']
    df.loc[df['tmp_me']<=0, 'tmp_me'] = np.nan
    df = (
        df.sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            tmp=lambda x: x.groupby('permno')['tmp'].cumprod())
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'tmp', 'tmp_lag60', 60)
        .assign(
            bh=lambda x: (x['tmp']-x['tmp_lag60'])/x['tmp_lag60'],
            comp_equ_iss=lambda x: np.log(x['tmp_me'])-x['bh'])
        .pipe(predictor_out_clean, 'comp_equ_iss'))

    df.to_parquet(predictors_dir/'comp_equ_iss.parquet.gzip', compression='gzip')

predicor_comp_equ_iss()

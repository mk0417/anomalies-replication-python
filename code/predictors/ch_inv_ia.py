import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in capital inv (ind adj)
# ch_inv_ia
# ---------------
@print_log
def predictor_ch_inv_ia():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'capx', 'ppent', 'at'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'sic_crsp']),
            how='right', on=['permno', 'time_avail_m'])
        .assign(sic2d=lambda x: x['sic_crsp']//100)
        .pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ppent', 'ppent_lag12', 12))
    df.loc[df['capx'].isna(), 'capx'] = df['ppent'] - df['ppent_lag12']
    df = (
        df.pipe(shift_var_month, 'permno', 'time_avail_m',
                'capx', 'capx_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'capx', 'capx_lag24', 24)
        .assign(
            pchcapx=lambda x:
            (x['capx']-0.5*(x['capx_lag12']+x['capx_lag24'])) /
            (0.5*(x['capx_lag12']+x['capx_lag24']))))
    df.loc[df['pchcapx'].isna(), 'pchcapx'] = (
        (df['capx']-df['capx_lag12'])/df['capx_lag12'])
    df = (
        df.assign(
            tmp=lambda x:
            x.groupby(['sic2d', 'time_avail_m'])['pchcapx'].transform('mean'),
            ch_inv_ia=lambda x: x['pchcapx']-x['tmp'])
        .pipe(predictor_out_clean, 'ch_inv_ia'))

    df.to_parquet(predictors_dir/'ch_inv_ia.parquet.gzip', compression='gzip')

predictor_ch_inv_ia()

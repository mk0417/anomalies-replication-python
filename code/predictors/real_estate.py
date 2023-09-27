import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Real estate holdings
# real_estate
# ---------------
@print_log
def predictor_real_estate():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ppenb', 'ppenls',
                     'fatb', 'fatl', 'ppegt', 'ppent', 'at'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'sic_crsp']),
            how='right', on=['permno', 'time_avail_m'])
        .assign(
            sic2d=lambda x: x['sic_crsp']//100,
            n=lambda x:
            x.groupby(['sic2d', 'time_avail_m'])['at'].transform('count'))
        .query('n>=5 & at==at')
        .dropna(subset=['ppent', 'ppegt'], how='all')
        .assign(
            re_old=lambda x: (x['ppenb']+x['ppenls'])/x['ppent'],
            re_new=lambda x: (x['fatb']+x['fatl'])/x['ppegt']))
    df['re'] = np.where(df['re_new'].isna(), df['re_old'], df['re_new'])
    df['tmp'] = df.groupby(['sic2d', 'time_avail_m'])['re'].transform('mean')
    df['real_estate'] = df['re'] - df['tmp']
    df = df.pipe(predictor_out_clean, 'real_estate')

    df.to_parquet(predictors_dir/'real_estate.parquet.gzip', compression='gzip')

predictor_real_estate()

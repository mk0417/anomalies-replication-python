import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Book leverage (annual)
# book_leverage
# ---------------
@print_log
def predictor_book_leverage():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'seq', 'ceq', 'pstk',
                     'pstkrv', 'pstkl', 'at', 'lt', 'txditc'])
        .assign(
            txditc=lambda x: x['txditc'].fillna(0),
            ps=lambda x: x['pstk'],
            se=lambda x: x['seq']))

    df['ps'] = np.where(df['ps'].isna(), df['pstkrv'], df['ps'])
    df['ps'] = np.where(df['ps'].isna(), df['pstkl'], df['ps'])
    df['se'] = np.where(df['se'].isna(), df['ceq']+df['ps'], df['se'])
    df['se'] = np.where(df['se'].isna(), df['at']-df['lt'], df['se'])
    df['book_leverage'] = df['at'] / (df['se']+df['txditc']-df['ps'])
    df = df.pipe(predictor_out_clean, 'book_leverage')

    df.to_parquet(
        predictors_dir/'book_leverage.parquet.gzip', compression='gzip')

predictor_book_leverage()

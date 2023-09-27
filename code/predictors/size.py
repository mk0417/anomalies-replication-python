import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Size
# size
# ---------------
@print_log
def predictor_size():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'me']))
    df.loc[df['me']<=0, 'me'] = np.nan
    df['size'] = np.log(df['me'])
    df = df.pipe(predictor_out_clean, 'size')

    df.to_parquet(predictors_dir/'size.parquet.gzip', compression='gzip')

predictor_size()

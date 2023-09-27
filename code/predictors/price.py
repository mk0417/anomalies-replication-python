import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Price
# price
# ---------------
@print_log
def predictor_price():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'prc'])
        .assign(price=lambda x: np.log(x['prc'].abs()))
        .pipe(predictor_out_clean, 'price'))

    df.to_parquet(predictors_dir/'price.parquet.gzip', compression='gzip')

predictor_price()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Short-term reversal
# st_reversal
# ---------------
@print_log
def predictor_st_reversal():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        # NOTE 2023-08-04: do I need to replace missing returns with zeros
        # CZ did it
        .rename(columns={'ret': 'st_reversal'})
        .pipe(predictor_out_clean, 'st_reversal'))

    df.to_parquet(predictors_dir/'st_reversal.parquet.gzip',  compression='gzip')

predictor_st_reversal()

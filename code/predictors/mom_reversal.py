import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Momentum-reversal
# mom_reversal
# ---------------
@print_log
def predictor_mom_reversal():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret'])
        .assign(ret=lambda x: x['ret'].fillna(0)))

    for i in range(13, 19):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ret', 'ret_lag'+str(i), i)

    df['mom_reversal'] = (1+df.filter(like='_lag')).prod(1, skipna=False) - 1
    df = df.pipe(predictor_out_clean, 'mom_reversal')

    df.to_parquet(predictors_dir/'mom_reversal.parquet.gzip', compression='gzip')

predictor_mom_reversal()

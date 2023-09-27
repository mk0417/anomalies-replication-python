import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Net external financing
# x_fin
# ---------------
@print_log
def predictor_x_fin():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'sstk', 'dv',
                     'prstkc', 'dltis', 'dltr', 'at', 'dlcch'])
        .assign(
            # https://github.com/OpenSourceAP/CrossSection/issues/88
            # Hou, Xue and Zhang (2020) p2091
            dlcch=lambda x: x['dlcch'].fillna(0),
            x_fin=lambda x:
            (x['sstk']-x['dv']-x['prstkc']+x['dltis']-x['dltr']+x['dlcch'])
            /x['at'])
        .pipe(predictor_out_clean, 'x_fin'))

    df.to_parquet(predictors_dir/'x_fin.parquet.gzip', compression='gzip')

predictor_x_fin()

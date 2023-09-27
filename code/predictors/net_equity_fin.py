import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Net equity finance
# net_equity_fin
# ---------------
@print_log
def predictor_net_equity_fin():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'sstk', 'prstkc', 'at', 'dv'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .assign(
            # https://github.com/OpenSourceAP/CrossSection/issues/88
            # Hou, Xue and Zhang (2020) p2091
            net_equity_fin=lambda x:
            (x['sstk']-x['prstkc']-x['dv'])/((x['at']+x['at_lag12'])/2)))
    df.loc[df['net_equity_fin'].abs()>1, 'net_equity_fin'] = np.nan
    df = df.pipe(predictor_out_clean, 'net_equity_fin')

    df.to_parquet(
        predictors_dir/'net_equity_fin.parquet.gzip', compression='gzip')

predictor_net_equity_fin()

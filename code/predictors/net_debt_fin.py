import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# NetDebtFinance
# net_debt_fin
# ---------------
@print_log
def predictor_net_debt_fin():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'dlcch', 'dltis', 'dltr'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'at', 'at_lag12', 12)
        .assign(
            # https://github.com/OpenSourceAP/CrossSection/issues/88
            # Hou, Xue and Zhang (2020) p2091
            dlcch=lambda x: x['dlcch'].fillna(0),
            net_debt_fin=lambda x:
            (x['dltis']-x['dltr']+x['dlcch'])/((x['at']+x['at_lag12'])/2)))
    df.loc[df['net_debt_fin'].abs()>1, 'net_debt_fin'] = np.nan
    df = df.pipe(predictor_out_clean, 'net_debt_fin')

    df.to_parquet(predictors_dir/'net_debt_fin.parquet.gzip', compression='gzip')

predictor_net_debt_fin()

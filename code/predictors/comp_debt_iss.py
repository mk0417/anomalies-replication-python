import sys
sys.path.insert(0, '../')
from functions import *



# ---------------
# Composite debt issuance
# comp_debt_iss
# ---------------
@print_log
def predictor_comp_debt_iss():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'dltt', 'dlc'])
        .assign(bd=lambda x: x['dltt']+x['dlc'])
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'bd', 'bd_lag60', 60)
        .assign(tmp=lambda x: x['bd']/x['bd_lag60']))
    df.loc[df['tmp']<=0, 'tmp'] = np.nan
    df = (
        df.assign(comp_debt_iss=lambda x: np.log(x['tmp']))
        .pipe(predictor_out_clean, 'comp_debt_iss'))

    df.to_parquet(
        predictors_dir/'comp_debt_iss.parquet.gzip', compression='gzip')

predictor_comp_debt_iss()

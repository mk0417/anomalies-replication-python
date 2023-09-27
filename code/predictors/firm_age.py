import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Firm age
# firm_age
# ---------------
@print_log
def predictor_firm_age():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            firm_age=lambda x:
            x.groupby('permno')['permno'].cumcount() + 1,
            bdate=lambda x: np.datetime64('1926-07-01')+pd.offsets.MonthEnd(0),
            tmp=lambda x:
            (x['time_avail_m'].dt.to_period('M')-x['bdate'].dt.to_period('M'))
            .apply(lambda x: x.n)+1))

    df.loc[df['tmp']==df['firm_age'], 'firm_age'] = np.nan
    df = df.pipe(predictor_out_clean, 'firm_age')

    df.to_parquet(predictors_dir/'firm_age.parquet.gzip', compression='gzip')

predictor_firm_age()

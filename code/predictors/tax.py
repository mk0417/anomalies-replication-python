import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Taxable income to income
# tax
# ---------------
@print_log
def predictor_tax():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'txfo',
                     'txfed', 'ib', 'txt', 'txdi'])
        .assign(year=lambda x: x['time_avail_m'].dt.year))
    df['tr'] = 0.48
    df['tr'] = np.where((df['year']>=1979) & (df['year']<=1986), 0.46, df['tr'])
    df['tr'] = np.where(df['year']==1987, 0.4, df['tr'])
    df['tr'] = np.where((df['year']>=1988) & (df['year']<=1992), 0.34, df['tr'])
    df['tr'] = np.where(df['year']>=1993, 0.35, df['tr'])
    df['tax'] = ((df['txfo']+df['txfed'])/df['tr'])/df['ib']
    df.loc[(df['txfo'].isna()) | (df['txfed'].isna()), 'tax'] = (
        ((df['txt']-df['txdi'])/df['tr'])/df['ib'])
    df['tax'] = np.where(
        (((df['txfo']+df['txfed'])>0) | (df['txt']>df['txdi']))
        & (df['ib']<=0), 1, df['tax'])
    df = df.pipe(predictor_out_clean, 'tax')

    df.to_parquet(predictors_dir/'tax.parquet.gzip', compression='gzip')

predictor_tax()

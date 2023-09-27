import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Net debt to price ratio
# net_debt_price
# ---------------
@print_log
def predictor_net_debt_price():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'dltt',
                     'dlc', 'pstk', 'dvpa', 'tstkp', 'che',
                     'sic', 'ib', 'csho', 'ceq', 'prcc_f'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='inner', on=['permno', 'time_avail_m'])
        .assign(
            net_debt_price=lambda x:
            (x['dltt']+x['dlc']+x['pstk']+x['dvpa']-x['tstkp']-x['che'])
            /x['me']))
    df.loc[(df['sic']>=6000) & (df['sic']<=6999), 'net_debt_price'] = np.nan
    df.loc[
        (df['at'].isna()) | (df['ib'].isna()) | (df['csho'].isna())
        | (df['ceq'].isna()) | (df['prcc_f'].isna()),
        'net_debt_price'] = np.nan
    df['bm'] = df['ceq'] / df['me']
    df.loc[df['bm']<=0, 'bm'] = np.nan
    df = (
        df.assign(bm=np.log(df['bm']))
        .pipe(port_group_cs, 5, 'all', 'time_avail_m', 'bm')
        .assign(port=lambda x: (x['port'].str[1:]).astype(float)))
    df.loc[df['port']<=2, 'net_debt_price'] = np.nan
    df = df.pipe(predictor_out_clean, 'net_debt_price')

    df.to_parquet(
        predictors_dir/'net_debt_price.parquet.gzip', compression='gzip')

predictor_net_debt_price()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Industry concentration
# herf
# ---------------
@print_log
def predictor_herf():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'sale'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'sic_crsp', 'shrcd']),
            how='right', on=['permno', 'time_avail_m'])
        .query('sic_crsp==sic_crsp')
        .assign(
            sic=lambda x: x['sic_crsp'].astype(int),
            indsale=lambda x:
            x.groupby(['sic', 'time_avail_m'])['sale'].transform('sum'),
            tmp=lambda x: (x['sale']/x['indsale'])**2,
            tmp_herf=lambda x:
            x.groupby(['sic', 'time_avail_m'])['tmp'].transform('sum'))
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            year=lambda x: x['time_avail_m'].dt.year,
            herf=lambda x:
            x.groupby('permno')['tmp_herf'].rolling(window=36, min_periods=12)
            .mean().reset_index(drop=True)))
    df.loc[df['shrcd']>11, 'herf'] = np.nan
    df.loc[(df['sic'].isin([4011, 4210, 4213])) & (df['year']<=1980), 'herf'] = (
        np.nan)
    df.loc[(df['sic']==4512) & (df['year']<=1978), 'herf'] = np.nan
    df.loc[(df['sic'].isin([4812, 4813])) & (df['year']<=1982), 'herf'] = np.nan
    df.loc[(df['sic']//100)==49, 'herf'] = np.nan
    # Set to missing before 1951 (no sales data)
    df.loc[df['year']<1951, 'herf'] = np.nan
    df = df.pipe(predictor_out_clean, 'herf')

    df.to_parquet(predictors_dir/'herf.parquet.gzip', compression='gzip')

predictor_herf()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Industry concentration (asset based)
# herf_asset
# ---------------
@print_log
def predictor_herf_asset():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'sic_crsp', 'shrcd']),
            how='inner', on=['permno', 'time_avail_m'])
        .query('sic_crsp==sic_crsp')
        .assign(
            sic=lambda x: x['sic_crsp'].astype(int),
            indasset=lambda x:
            x.groupby(['sic', 'time_avail_m'])['at'].transform('sum'),
            tmp=lambda x: (x['at']/x['indasset'])**2,
            tmp_herf_asset=lambda x:
            x.groupby(['sic', 'time_avail_m'])['tmp'].transform('sum'))
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            year=lambda x: x['time_avail_m'].dt.year,
            herf_asset=lambda x:
            x.groupby('permno')['tmp_herf_asset']
            .rolling(window=36, min_periods=12)
            .mean().reset_index(drop=True)))
    df.loc[df['shrcd']>11, 'herf_asset'] = np.nan
    df.loc[
        (df['sic'].isin([4011, 4210, 4213])) & (df['year']<=1980),
        'herf_asset'] = np.nan
    df.loc[(df['sic']==4512) & (df['year']<=1978), 'herf_asset'] = np.nan
    df.loc[
        (df['sic'].isin([4812, 4813])) & (df['year']<=1982),
        'herf_asset'] = np.nan
    df.loc[(df['sic']//100)==49, 'herf_asset'] = np.nan
    df = df.pipe(predictor_out_clean, 'herf_asset')

    df.to_parquet(predictors_dir/'herf_asset.parquet.gzip', compression='gzip')

predictor_herf_asset()

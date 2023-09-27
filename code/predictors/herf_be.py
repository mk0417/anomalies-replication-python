import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Industry concentration (asset based)
# herf_be
# ---------------
@print_log
def predictor_herf_be():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'txditc', 'pstk',
                     'pstkrv', 'pstkl', 'seq', 'ceq', 'at', 'lt'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'sic_crsp', 'shrcd']),
            how='inner', on=['permno', 'time_avail_m'])
        .query('sic_crsp==sic_crsp')
        .assign(
            sic=lambda x: x['sic_crsp'].astype(int),
            txditc=lambda x: x['txditc'].fillna(0),
            ps=lambda x: x['pstk'],
            se=lambda x: x['seq']))
    df['ps'] = np.where(df['ps'].isna(), df['pstkrv'], df['ps'])
    df['ps'] = np.where(df['ps'].isna(), df['pstkl'], df['ps'])
    df['se'] = np.where(df['se'].isna(), df['ceq']+df['ps'], df['se'])
    df['se'] = np.where(df['se'].isna(), df['at']-df['lt'], df['se'])

    df = (
        df.assign(
            be=df['se']+df['txditc']-df['ps'],
            indequity=lambda x:
            x.groupby(['sic', 'time_avail_m'])['be'].transform('sum'),
            tmp=lambda x: (x['be']/x['indequity'])**2,
            tmp_herf_be=lambda x:
            x.groupby(['sic', 'time_avail_m'])['tmp'].transform('sum'))
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            year=lambda x: x['time_avail_m'].dt.year,
            herf_be=lambda x:
            x.groupby('permno')['tmp_herf_be']
            .rolling(window=36, min_periods=12)
            .mean().reset_index(drop=True)))
    df.loc[df['shrcd']>11, 'herf_be'] = np.nan
    df.loc[
        (df['sic'].isin([4011, 4210, 4213])) & (df['year']<=1980),
        'herf_be'] = np.nan
    df.loc[(df['sic']==4512) & (df['year']<=1978), 'herf_be'] = np.nan
    df.loc[
        (df['sic'].isin([4812, 4813])) & (df['year']<=1982),
        'herf_be'] = np.nan
    df.loc[(df['sic']//100)==49, 'herf_be'] = np.nan
    df = df.pipe(predictor_out_clean, 'herf_be')

    df.to_parquet(predictors_dir/'herf_be.parquet.gzip', compression='gzip')

predictor_herf_be()

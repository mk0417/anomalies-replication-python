import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Earnings announcement return
# ann_ret
# ---------------
@print_log
def predictor_ann_ret():
    df_ann = (
        pd.read_parquet(
            download_dir/'compustat_q_monthly.parquet.gzip',
            columns=['gvkey', 'rdq'])
        .query('rdq==rdq')
        .drop_duplicates(['gvkey', 'rdq'])
        .sort_values(['gvkey', 'rdq'], ignore_index=True))
    df = (
        pd.read_parquet(
            download_dir/'crsp_daily.parquet.gzip',
            columns=['permno', 'time_d', 'ret'])
        .merge(
            pd.read_parquet(
                download_dir/'ccm_link.parquet.gzip',
                columns=['permno', 'gvkey', 'timeLinkStart_d', 'timeLinkEnd_d']),
            how='inner', on='permno')
        .query('timeLinkStart_d<=time_d<=timeLinkEnd_d')
        .drop(columns=['timeLinkStart_d', 'timeLinkEnd_d'])
        .merge(
            df_ann, how='left',
            left_on=['gvkey', 'time_d'], right_on=['gvkey', 'rdq'])
        .drop(columns='gvkey'))
    df.loc[df['rdq'].notna(), 'anndat'] = 1
    df = (
        df.merge(
            pd.read_parquet(
                download_dir/'ff_daily.parquet.gzip',
                columns=['time_d', 'mktrf', 'rf']),
            how='inner', on='time_d')
        .sort_values(['permno', 'time_d'], ignore_index=True)
        .assign(
            anndat_lead1=lambda x: x.groupby('permno')['anndat'].shift(-1),
            anndat_lead2=lambda x: x.groupby('permno')['anndat'].shift(-2),
            anndat_lag1=lambda x: x.groupby('permno')['anndat'].shift(1),
            tmp_time=lambda x: x.groupby('permno')['permno'].cumcount()+1,
            ann_ret=lambda x: x['ret']-(x['mktrf']+x['rf'])))
    df.loc[df['anndat']==1, 'time_ann_d'] = df['tmp_time']
    df.loc[df['anndat_lead1']==1, 'time_ann_d'] = df['tmp_time'] + 1
    df.loc[df['anndat_lead2']==1, 'time_ann_d'] = df['tmp_time'] + 2
    df.loc[df['anndat_lag1']==1, 'time_ann_d'] = df['tmp_time'] - 1
    df = df.query('time_ann_d==time_ann_d').copy()

    df = db.sql("""
        select a.permno, last_day(a.time_d) as time_avail_m,
        a.time_d, b.ann_ret
        from
        (select permno, time_ann_d, time_d
        from df
        where anndat=1) a
        join
        (select permno, time_ann_d, sum(ann_ret) as ann_ret
        from df
        group by permno, time_ann_d) b
        on a.permno=b.permno and a.time_ann_d=b.time_ann_d
        """).df()
    df = (
        df.sort_values(['permno', 'time_avail_m', 'time_d'], ignore_index=True)
        .drop_duplicates(['permno', 'time_avail_m'], keep='last')
        .drop(columns='time_d')
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .set_index('time_avail_m')
        .groupby('permno').resample('M').asfreq()
        .drop(columns='permno')
        .reset_index()
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            ann_ret=lambda x:
            x.groupby('permno')['ann_ret'].fillna(method='ffill', limit=6))
        .pipe(predictor_out_clean, 'ann_ret'))

    df.to_parquet(predictors_dir/'ann_ret.parquet.gzip', compression='gzip')

predictor_ann_ret()

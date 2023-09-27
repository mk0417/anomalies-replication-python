import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# 52-week High
# high52
# ---------------
@print_log
def predictor_high52():
    df = (
        pd.read_parquet(
            download_dir/'crsp_daily.parquet.gzip',
            columns=['permno', 'time_d', 'prc', 'cfacpr'])
        .assign(
            tmp=lambda x: x['time_d'].dt.year*100+x['time_d'].dt.month,
            prcadj=lambda x: x['prc'].abs()/x['cfacpr']))

    df = db.sql("""
            select a.permno, a.time_avail_m, a.maxpr, b.prcadj
            from
            (select permno, last_day(time_d) as time_avail_m,
            max(prcadj) as maxpr
            from df
            group by permno, time_avail_m) a
            join
            (select permno, last_day(time_d) as time_avail_m, prcadj
            from
            (select *,
            row_number() over(partition by permno, tmp order by time_d desc) as n
            from df
            where prcadj not null)
            where n=1) b
            on a.permno=b.permno and a.time_avail_m=b.time_avail_m
        """).df()

    for i in range(1, 13):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'maxpr', 'maxpr_lag'+str(i), i)

    df = (
        df.assign(
            tmp=lambda x: x.filter(like='_lag').max(1),
            high52=lambda x: x['prcadj']/x['tmp'])
        .pipe(predictor_out_clean, 'high52'))

    df.to_parquet(predictors_dir/'high52.parquet.gzip', compression='gzip')

predictor_high52()

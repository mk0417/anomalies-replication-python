import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Tail risk beta
# beta_tail_risk
# ---------------
@print_log
def predictor_beta_tail_risk():
    df_d = (
        pd.read_parquet(
            download_dir/'crsp_daily.parquet.gzip',
            columns=['permno', 'time_d', 'ret']))
    df_m = (
        pd.read_parquet(
            download_dir/'crsp_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'ret', 'shrcd']))
    df_tail = db.sql("""
        select c.*, d.tailex
        from df_m c
        left join
        (select time_avail_m, favg(tailex) as tailex
        from
        (select a.*, last_day(a.time_d) as time_avail_m, ln(a.ret/b.p5) as tailex
        from df_d a
        left join
        (select last_day(time_d) as time_avail_m, quantile_disc(ret, 0.05) as p5
        from df_d
        group by time_avail_m) b
        on last_day(a.time_d)=b.time_avail_m
        where a.ret<=b.p5)
        group by time_avail_m) d
        on c.time_avail_m=d.time_avail_m
        order by c.permno, c.time_avail_m
        """).df()

    df = (
        df_tail.pipe(rolling_ols_parallel, 'ret', ['tailex'], 8, 120, 72)
        .rename(columns={'tailex': 'beta_tail_risk'})
        .merge(df_m, how='left', on=['permno', 'time_avail_m']))

    df.loc[df['shrcd']>11, 'beta_tail_risk'] = np.nan
    df = df.pipe(predictor_out_clean, 'beta_tail_risk')

    df.to_parquet(
        predictors_dir/'beta_tail_risk.parquet.gzip', compression='gzip')

predictor_beta_tail_risk()

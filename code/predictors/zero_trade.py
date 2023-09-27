import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Days with zero trades (1 month version)
# zero_trade1m
#
# Days with zero trades (6 month version)
# zero_trade6m
#
# Days with zero trades (12 month version)
# zero_trade12m
# ---------------
@print_log
def predictor_zero_trade():
    df =  pd.read_parquet(
        download_dir/'crsp_daily.parquet.gzip',
        columns=['permno', 'time_d', 'vol', 'shrout'])
    df['turn'] = df['vol'] / df['shrout']
    df['zero'] = np.where(df['vol']==0, 1, 0)

    df = db.sql("""
        select *, (zero+((1/turn)/480000))*(21/ndays) as tmp
        from
        (select permno, last_day(time_d) as time_avail_m, sum(zero) as zero,
            sum(turn) as turn, count(permno) as ndays
        from df
        group by permno, time_avail_m)
        """).df()

    for i in range(1, 13):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'tmp', 'tmp_lag'+str(i), i)

    df['zero_trade1m'] = df['tmp_lag1']
    df['zero_trade6m'] = (
        df[['tmp_lag'+str(i) for i in range(1, 7)]].mean(1, skipna=False))
    df['zero_trade12m'] = (
        df[['tmp_lag'+str(i) for i in range(1, 13)]].mean(1, skipna=False))

    df_1m = df.pipe(predictor_out_clean, 'zero_trade1m')
    df_6m = df.pipe(predictor_out_clean, 'zero_trade6m')
    df_12m = df.pipe(predictor_out_clean, 'zero_trade12m')

    df_1m.to_parquet(
        predictors_dir/'zero_trade1m.parquet.gzip', compression='gzip')
    df_6m.to_parquet(
        predictors_dir/'zero_trade6m.parquet.gzip', compression='gzip')
    df_12m.to_parquet(
        predictors_dir/'zero_trade12m.parquet.gzip', compression='gzip')

predictor_zero_trade()

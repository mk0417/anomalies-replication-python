import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Maximum return over month
# max_ret
# ---------------
@print_log
def predictor_max_ret():
    df = (
        pd.read_parquet(
            download_dir/'crsp_daily.parquet.gzip',
            columns=['permno', 'time_d', 'ret']))
    df = (
        db.sql("""
            select permno, last_day(time_d) as time_avail_m, max(ret) as max_ret
            from df
            group by permno, time_avail_m
        """).df()
        .pipe(predictor_out_clean, 'max_ret'))

    df.to_parquet(predictors_dir/'max_ret.parquet.gzip', compression='gzip')

predictor_max_ret()

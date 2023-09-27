import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Return skewness
# return_skew
# ---------------
@print_log
def predictor_return_skew():
    df = (
        pd.read_parquet(
            download_dir/'crsp_daily.parquet.gzip',
            columns=['permno', 'time_d', 'ret']))
    df = (
        # pandas count ignores missing value
        # duckdb count dose not ignore missing value
        db.sql("""
            select permno, last_day(time_d) as time_avail_m,
                skewness(ret) as return_skew, count(ret) as n
            from df
            group by permno, time_avail_m
            having n>=15
        """).df()
        .pipe(predictor_out_clean, 'return_skew'))

    df.to_parquet(predictors_dir/'return_skew.parquet.gzip', compression='gzip')

predictor_return_skew()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Illiquidity
# illiquidity
# ---------------
@print_log
def perdictor_illiquidity():
    df = (
        pd.read_parquet(
            download_dir/'crsp_daily.parquet.gzip',
            columns=['permno', 'time_d', 'ret', 'prc', 'vol'])
        .assign(
            # ill is very small and this will cause precision issue
            # (i.e. tiny valus will return inf)
            # pandas and duckdb (avg vs favg) will give different
            # results due to precision loss of different algorithm
            # Comparison of computation accuracy:
            # https://github.com/duckdb/duckdb/issues/6829
            # duckdb (favg) > pandas (mean) > duckdb (avg)
            # Not sure the precision in other software, like Stata
            ill=lambda x: x['ret'].abs()/(x['prc'].abs()*x['vol'])))

    df = db.sql("""
            select permno, last_day(time_d) as time_avail_m, favg(ill) as ill,
                count(ill) filter(where ill is not null) as nvol
            from df
            group by permno, time_avail_m
        """).df()

    for i in range(1, 12):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ill', 'ill_lag'+str(i), i)

    for i in range(1, 12):
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'nvol', 'nvol_lag'+str(i), i)

    df = (
        df.assign(
            illiquidity=df.filter(like='ill').mean(1),
            n_tot=df.filter(like='nvol').sum(1))
        # Amihud (2002) requires more than 200-day valid data over past year, p36
        .query('n_tot>200')
        .pipe(predictor_out_clean, 'illiquidity'))

    df.to_parquet(predictors_dir/'illiquidity.parquet.gzip', compression='gzip')

perdictor_illiquidity()

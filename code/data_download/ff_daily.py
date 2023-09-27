import sys
sys.path.insert(0, '../')
from functions import *


@print_log
def ff_daily_download():
    conn = wrds_connect()
    conn.connect()

    sql_query = """
        select date, mktrf, smb, hml, rf, umd
        from ff.factors_daily
    """

    df = (
        conn.raw_sql(sql_query, date_cols=['date'])
        .reset_index(drop=True)
        .rename(columns={'date': 'time_d'})
        .sort_values('time_d', ignore_index=True))

    df.to_parquet(download_dir/'ff_daily.parquet.gzip', compression='gzip')

ff_daily_download()

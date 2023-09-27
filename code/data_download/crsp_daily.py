import sys
sys.path.insert(0, '../')
from functions import *


# 22 mins
@print_log
def crsp_daily_download():
    conn = wrds_connect()
    conn.connect()

    sql_query = """
        select permno, date, ret, vol, shrout, prc, cfacpr
        from crsp.dsf
    """

    df = (
        conn.raw_sql(sql_query, date_cols=['date'])
        .reset_index(drop=True)
        .assign(permno=lambda x: x['permno'].astype(int))
        .rename(columns={'date': 'time_d'})
        .drop_duplicates(['permno', 'time_d'])
        .sort_values(['permno', 'time_d'], ignore_index=True))

    df.to_parquet(download_dir/'crsp_daily.parquet.gzip', compression='gzip')

crsp_daily_download()

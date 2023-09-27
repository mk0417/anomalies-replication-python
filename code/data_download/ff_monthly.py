import sys
sys.path.insert(0, '../')
from functions import *


@print_log
def ff_monthly_download():
    conn = wrds_connect()
    conn.connect()

    sql_query = """
        select date, mktrf, smb, hml, rf, umd
        from ff.factors_monthly
    """

    df = (
        conn.raw_sql(sql_query, date_cols=['date'])
        .reset_index(drop=True)
        .assign(date=lambda x: x['date'] + pd.offsets.MonthEnd(0))
        .rename(columns={'date': 'time_avail_m'})
        .sort_values('time_avail_m', ignore_index=True))

    df.to_parquet(download_dir/'ff_monthly.parquet.gzip', compression='gzip')

ff_monthly_download()

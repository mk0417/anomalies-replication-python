import sys
sys.path.insert(0, '../')
from functions import *


@print_log
def sp_credit_ratings_download():
    # 1978 Nov to 2017 Feb
    rating_map = {
        'D': 1,
        'C': 2,
        'CC': 3,
        'CCC-': 4,
        'CCC': 5,
        'CCC+': 6,
        'B-': 7,
        'B': 8,
        'B+': 9,
        'BB-': 10,
        'BB': 11,
        'BB+': 12,
        'BBB-': 13,
        'BBB': 14,
        'BBB+': 15,
        'A-': 16,
        'A': 17,
        'A+': 18,
        'AA-': 19,
        'AA': 20,
        'AA+': 21,
        'AAA': 22}

    conn = wrds_connect()
    conn.connect()

    sql_query = """
        select gvkey, datadate, splticrm
        from comp.adsprate
    """

    df = (
        conn.raw_sql(sql_query, date_cols=['datadate'])
        .reset_index(drop=True)
        .assign(
            time_avail_m=lambda x: x['datadate']+pd.offsets.MonthEnd(0),
            tmp=lambda x: x['splticrm'].map(rating_map),
            credrat=lambda x: x['tmp'].fillna(0))
        .drop(columns=['datadate', 'splticrm', 'tmp'])
        .drop_duplicates(['gvkey', 'time_avail_m'])
        .sort_values(['gvkey', 'time_avail_m'], ignore_index=True))

    df.to_parquet(
        download_dir/'sp_credit_ratings.parquet.gzip', compression='gzip')

sp_credit_ratings_download()

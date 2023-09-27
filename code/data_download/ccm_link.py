import sys
sys.path.insert(0, '../')
from functions import *


@print_log
def ccm_link_download():
    conn = wrds_connect()
    conn.connect()

    sql_query = """
        select a.gvkey, a.conm, a.tic, a.cusip, a.cik, a.sic, a.naics,
            b.linkprim, b.linktype, b.liid, b.lpermno, b.lpermco, b.linkdt,
            b.linkenddt
        from comp.names as a
        inner join crsp.ccmxpf_lnkhist as b
        on a.gvkey = b.gvkey
        where b.linktype in ('LC', 'LU') and b.linkprim in ('P', 'C')
        order by a.gvkey
    """

    df = (
        conn.raw_sql(sql_query, date_cols=['linkdt', 'linkenddt'])
        .reset_index(drop=True)
        .rename(
            columns={'linkdt': 'timeLinkStart_d',
                     'linkenddt': 'timeLinkEnd_d',
                     'lpermno': 'permno'})
        .assign(
            permno=lambda x: x['permno'].astype(int),
            lpermco=lambda x: x['lpermco'].astype(int),
            sic=lambda x: x['sic'].astype(int)))

    df.loc[df['timeLinkEnd_d'].isna(), 'timeLinkEnd_d'] = np.datetime64('today')
    df.to_parquet(download_dir/'ccm_link.parquet.gzip', compression='gzip')

ccm_link_download()

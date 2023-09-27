import sys
sys.path.insert(0, '../')
from functions import *


@print_log
def crsp_monthly_download():
    conn = wrds_connect()
    conn.connect()

    sql_query = """
        select a.permno, a.date, a.ret, a.retx, a.vol, a.shrout, a.prc,
            a.cfacshr, a.bidlo, a.askhi, b.shrcd, b.exchcd, b.siccd, b.ticker,
            b.shrcls, c.dlstcd, c.dlret
        from crsp.msf as a
	left join crsp.msenames as b
            on a.permno=b.permno and b.namedt<=a.date and a.date<=b.nameendt
	left join crsp.msedelist as c
	    on a.permno=c.permno and
            date_trunc('month', a.date) = date_trunc('month', c.dlstdt)
    """

    df = (
        conn.raw_sql(sql_query, date_cols=['date'])
        .reset_index(drop=True)
        .rename(columns={'siccd': 'sic_crsp'})
        .assign(
            permno=lambda x: x['permno'].astype(int),
            time_avail_m=lambda x: x['date'] + pd.offsets.MonthEnd(0),
            shrout=lambda x: x['shrout']/1e3,
            vol=lambda x: x['vol']/1e4,
            me=lambda x: x['shrout']*(x['prc'].abs()))
        .drop_duplicates(['permno', 'time_avail_m']))

    # ret without delisting adjustment
    df_raw = (
        df.copy()
        .drop(columns=['date', 'dlret', 'dlstcd'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True))

    # Incorporate delisting return
    df['dlret'] = np.where(
        (df['dlret'].isna()) &
        (df['dlstcd'].eq(500) | (df['dlstcd'].ge(520) & df['dlstcd'].le(584))) &
        (df['exchcd'].isin([1, 2])), -0.35, df['dlret'])
    # GHZ cite Johnson and Zhao (2007), Shumway and Warther (1999)
    df['dlret'] = np.where(
        (df['dlret'].isna()) &
        (df['dlstcd'].eq(500) | (df['dlstcd'].ge(520) & df['dlstcd'].le(584))) &
        (df['exchcd']==3), -0.55, df['dlret'])
    df.loc[df['dlret']<-1, 'dlret'] = -1
    df.loc[df['dlret'].notna(), 'ret'] = (1+df['ret']) * (1+df['dlret']) - 1
    df.loc[(df['ret'].isna()) & (df['dlret'].notna()), 'ret'] = df['dlret']
    df = (
        df.drop(columns=['date', 'dlret', 'dlstcd'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True))

    df_raw.to_parquet(
        download_dir/'crsp_monthly_raw.parquet.gzip', compression='gzip')
    df.to_parquet(download_dir/'crsp_monthly.parquet.gzip', compression='gzip')

crsp_monthly_download()

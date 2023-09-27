import sys
sys.path.insert(0, '../')
from functions import *


def fill_missing_to_0(data):
    df = data.copy()
    # For these variables, missing is assumed to be 0
    var_list = [
        'acoq', 'actq', 'apq', 'cheq', 'dpq', 'drcq', 'invtq', 'intanq',
        'ivaoq', 'gdwlq', 'lcoq', 'lctq', 'loq', 'mibq', 'prstkcy', 'rectq',
        'sstky', 'txditcq']
    df[var_list] = df[var_list].fillna(0)
    return df

def year_to_date_var(data):
    # Prepare year-to-date items
    var_list = ['sstky', 'prstkcy', 'oancfy', 'fopty']
    df = data.copy()
    for i in var_list:
        df[i+'lag'] = df.groupby(['gvkey', 'fyearq'])[i].shift(1)
        df[i+'q'] = df[i] - df[i+'lag']
        df.loc[df['fqtr']==1, i+'q'] = df[i]
        del df[i+'lag']

    return df

@print_log
def compustat_quarterly_download():
    conn = wrds_connect()
    conn.connect()

    sql_query = """
        select gvkey, datadate, fyearq, fqtr, datacqtr, datafqtr, acoq, actq,
            ajexq, apq, atq, ceqq, cheq, cogsq, cshoq, cshprq, dlcq, dlttq, dpq,
            drcq, drltq, dvpsxq, dvpq, dvy, epspiq, epspxq, fopty, gdwlq, ibq,
            invtq, intanq, ivaoq, lcoq, lctq, loq, ltq, mibq, niq, oancfy,
            oiadpq, oibdpq, piq, ppentq, ppegtq, prstkcy, prccq, pstkq, rdq,
            req, rectq, revtq, saleq, seqq, sstky, txdiq, txditcq, txpq, txtq,
            xaccq, xintq, xsgaq, xrdq, capxy
        from comp.fundq
	where consol='C' and popsrc='D' and datafmt='STD'
            and curcdq='USD' and indfmt='INDL'
    """

    df = (
        conn.raw_sql(sql_query, date_cols=['datadate', 'rdq'])
        .sort_values(['gvkey', 'fyearq', 'fqtr', 'datadate'], ignore_index=True)
        .drop_duplicates(['gvkey', 'fyearq', 'fqtr'], keep='last')
        .assign(
            # Assume data available with a 3 month lag
            time_avail_m=lambda x: x['datadate']+pd.offsets.MonthEnd(0)
            +pd.offsets.MonthEnd(3)))

    # Patch cases with earlier data availability
    df['time_avail_m'] = np.where(
        df['rdq']>df['time_avail_m'], df['rdq'], df['time_avail_m'])
    df['time_avail_m'] = df['time_avail_m'] + pd.offsets.MonthEnd(0)
    # Drop cases with very late release
    df['month_gap'] = (
        (df['rdq'].dt.to_period('M')-df['datadate'].dt.to_period('M'))
        .apply(lambda x: x.n if pd.notna(x) else np.nan))
    df = (
        df[~(df['month_gap']>6)].drop(columns='month_gap')
        .sort_values(['gvkey', 'time_avail_m', 'datadate'], ignore_index=True)
        .drop_duplicates(['gvkey', 'time_avail_m'], keep='last')
        .pipe(fill_missing_to_0)
        .sort_values(['gvkey', 'fyearq', 'fqtr'], ignore_index=True)
        .pipe(fill_missing_to_0)
        .pipe(year_to_date_var))

    df_m = (
        pd.concat([df]*3)
        .sort_values(['gvkey', 'time_avail_m'], ignore_index=True)
        .assign(
            month_inc=lambda x: x.groupby(['gvkey', 'time_avail_m'])
            ['gvkey'].cumcount(),
            time_avail_m=lambda x: (
                x['time_avail_m'].dt.to_period('M')+x['month_inc'])
            .dt.to_timestamp() + pd.offsets.MonthEnd(0))
        .drop(columns='month_inc')
        .sort_values(['gvkey', 'time_avail_m', 'datadate'], ignore_index=True)
        .drop_duplicates(['gvkey', 'time_avail_m'], keep='last')
        .rename(columns={'datadate': 'datadateq'})
        .sort_values(['gvkey', 'time_avail_m'], ignore_index=True))

    df_m.to_parquet(
        download_dir/'compustat_q_monthly.parquet.gzip', compression='gzip')

compustat_quarterly_download()

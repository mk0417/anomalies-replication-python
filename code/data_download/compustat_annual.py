import sys
sys.path.insert(0, '../')
from functions import *


def gen_dr(data):
    # Deferred revenue
    # drc  = deferred revenue current
    # drlt = deferred revenue long-term
    df = data.copy()
    df['dr'] = df['drc'] + df['drlt']
    df.loc[(df['drc'].notna()) & (df['drlt'].isna()), 'dr'] = df['drc']
    df.loc[(df['drlt'].notna()) & (df['drc'].isna()), 'dr'] = df['drlt']
    return df

def gen_dc(data):
    # dcpstk = convertible debt and preferred stock
    # pstk = preferred stock
    # dcvt = convertible debt
    df = data.copy()
    df['dc'] = df['dcvt']
    df.loc[(df['dc'].isna()) & (df['dcpstk']>df['pstk']), 'dc'] = (
        df['dcpstk'] - df['pstk'])
    df.loc[df['dc'].isna(), 'dc'] = df['dcpstk']
    return df

def fill_missing_to_0(data):
    df = data.copy()
    # For these variables, missing is assumed to be 0
    var_list = [
        'nopi', 'dvt', 'ob', 'dm', 'dc', 'aco', 'ap', 'intan', 'ao', 'lco', 'lo',
        'rect', 'invt', 'drc', 'spi', 'gdwl', 'che', 'dp', 'act', 'lct', 'tstkp',
        'dvpa', 'scstkc', 'sstk', 'mib', 'ivao', 'prstkc', 'prstkcc', 'txditc',
        'ivst', 'xint0', 'xsga0', 'xad0']
    df[var_list] = df[var_list].fillna(0)
    return df

@print_log
def compustat_annual_download():
    conn = wrds_connect()
    conn.connect()

    sql_query = """
        select gvkey, datadate, conm, fyear, tic, cusip, naicsh, sich, aco, act,
            ajex, am, ao, ap, at, capx, ceq, ceqt, che, cogs, csho, cshrc,
            dcpstk, dcvt, dlc, dlcch, dltis, dltr, dltt, dm, dp, drc, drlt, dv,
            dvc, dvp, dvpa, dvpd, dvpsx_c, dvt, ebit, ebitda, emp, epspi, epspx,
            fatb, fatl, ffo, fincf, fopt, gdwl, gdwlia, gdwlip, gwo, ib, ibcom,
            intan, invt, ivao, ivncf, ivst, lco, lct, lo, lt, mib, msa, ni, nopi,
            oancf, ob, oiadp, oibdp, pi, ppenb, ppegt, ppenls, ppent, prcc_c,
            prcc_f, prstkc, prstkcc, pstk, pstkl, pstkrv, re, rect, recta, revt,
            sale, scstkc, seq, spi, sstk, tstkp, txdb, txdi, txditc, txfo, txfed,
            txp, txt, wcap, wcapch, xacc, xad, xint, xrd, xpp, xsga
        from comp.funda
        where consol='C' and popsrc='D' and datafmt='STD'
            and curcd='USD' and indfmt='INDL'
    """

    df = (
        conn.raw_sql(sql_query, date_cols=['datadate'])
        .reset_index(drop=True)
        # Require some reasonable amount of information
        .dropna(subset=['at', 'prcc_c', 'ni'], how='all')
        .assign(
            cnum=lambda x: x['cusip'].str[:6],
            # xint = interest and related expense - Total
            xint0=lambda x: x['xint'],
            # xsga = selling, general and administrative expenses
            xsga0=lambda x: x['xsga'],
            xad0=lambda x: x['xad'])
        .pipe(gen_dr)
        .pipe(gen_dc)
        .pipe(fill_missing_to_0)
        .merge(
            pd.read_parquet(
                download_dir/'ccm_link.parquet.gzip',
                columns=['gvkey', 'permno', 'sic',
                         'timeLinkStart_d', 'timeLinkEnd_d']),
            how='inner', on='gvkey')
        .query('timeLinkStart_d<=datadate<=timeLinkEnd_d'))

    # Create two versions:
    # Annual and monthly (monthly makes matching to monthly CRSP easier)
    df_a= (
        df.copy()
        .drop(columns=['timeLinkStart_d', 'timeLinkEnd_d'])
        # Assuming 6 month reporting lag
        .assign(
            time_avail_m=lambda x: x['datadate']+pd.offsets.MonthEnd(0)
            +pd.offsets.MonthEnd(6))
        .drop_duplicates(['permno', 'time_avail_m'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True))

    df_m = (
        pd.concat([df_a]*12)
        .sort_values(['gvkey', 'time_avail_m'], ignore_index=True)
        .assign(
            month_inc=lambda x:
            x.groupby(['gvkey', 'time_avail_m'])['gvkey'].cumcount(),
            time_avail_m=lambda x: (
                x['time_avail_m'].dt.to_period('M')+x['month_inc'])
            .dt.to_timestamp() + pd.offsets.MonthEnd(0))
        .drop(columns='month_inc')
        .sort_values(['gvkey', 'time_avail_m', 'datadate'], ignore_index=True)
        .drop_duplicates(['gvkey', 'time_avail_m'], keep='last')
        .sort_values(['permno', 'time_avail_m', 'datadate'], ignore_index=True)
        .drop_duplicates(['permno', 'time_avail_m'], keep='last')
        .sort_values(['permno', 'time_avail_m'], ignore_index=True))

    df_a.to_parquet(
        download_dir/'compustat_a_annual.parquet.gzip', compression='gzip')
    df_m.to_parquet(
        download_dir/'compustat_a_monthly.parquet.gzip', compression='gzip')

compustat_annual_download()

import sys
sys.path.insert(0, '../')
from functions import *


# SignalMasterTable
# Holds monthly list of firms with identifiers and some meta information
@print_log
def signal_master_table():
    # Start with monthly CRSP
    df = (
        pd.read_parquet(
            download_dir/'crsp_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'exchcd', 'shrcd',
                     'me', 'prc', 'ret', 'sic_crsp'])
        # Screen on Stock market information: common stocks and major exchanges
        .query('shrcd==[10, 11, 12] & exchcd==[1, 2, 3]')
        .merge(
            pd.read_parquet(
                download_dir/'compustat_a_monthly.parquet.gzip',
                columns=['gvkey', 'permno', 'time_avail_m', 'sic']),
            how='left', on=['permno', 'time_avail_m'])
        .rename(columns={'sic': 'sic_cs'})
        # Future buy and hold return
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'ret', 'bh1m', -1)
        .filter(['gvkey', 'permno', 'time_avail_m', 'ret', 'bh1m', 'me',
                 'prc', 'exchcd', 'shrcd', 'sic_cs', 'sic_crsp'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True))

    df.to_parquet(
        download_dir/'signal_master_table.parquet.gzip', compression='gzip')

signal_master_table()

import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Cash to assets
# cash
# ---------------
@print_log
def predictor_cash():
    df = (
        pd.read_parquet(
            download_dir/'compustat_q_monthly.parquet.gzip',
            columns=['gvkey', 'rdq', 'atq', 'cheq'])
        .query('gvkey==gvkey & atq==atq & rdq==rdq')
        .sort_values(['gvkey', 'rdq'], ignore_index=True)
        .drop_duplicates(['gvkey', 'rdq'])
        .assign(time_avail_m=lambda x: x['rdq']+pd.offsets.MonthEnd(0))
        .sort_values(['gvkey', 'rdq'], ignore_index=True))

    df = (pd.concat([df]*3)
          .sort_values(['gvkey', 'time_avail_m'], ignore_index=True)
          .assign(
              month_inc=lambda x:
              x.groupby(['gvkey', 'time_avail_m'])['gvkey'].cumcount(),
              time_avail_m=lambda x:
              (x['time_avail_m'].dt.to_period('M')+x['month_inc'])
              .dt.to_timestamp()+pd.offsets.MonthEnd(0))
          .drop(columns='month_inc')
          .sort_values(['gvkey', 'time_avail_m', 'rdq'], ignore_index=True)
          .drop_duplicates(['gvkey', 'time_avail_m'], keep='last')
          .sort_values(['gvkey', 'time_avail_m'], ignore_index=True)
          .merge(
              pd.read_parquet(
                  download_dir/'signal_master_table.parquet.gzip',
                  columns=['permno', 'time_avail_m', 'gvkey'])
              .query('gvkey==gvkey'),
              how='inner', on=['gvkey', 'time_avail_m'])
          .assign(cash=lambda x: x['cheq']/x['atq'])
          .pipe(predictor_out_clean, 'cash'))

    df.to_parquet(predictors_dir/'cash.parquet.gzip', compression='gzip')

predictor_cash()

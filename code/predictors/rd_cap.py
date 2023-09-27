import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# R&D capital to assets (for constrained only)
# rd_cap
# ---------------
@print_log
def predictor_rd_cap():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'at', 'xrd'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'me']),
            how='left', on=['permno', 'time_avail_m'])
        .assign(
            year=lambda x: x['time_avail_m'].dt.year,
            xrd=lambda x: x['xrd'].fillna(0))
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'xrd', 'xrd_lag12', 12)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'xrd', 'xrd_lag24', 24)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'xrd', 'xrd_lag36', 36)
        .pipe(shift_var_month, 'permno', 'time_avail_m', 'xrd', 'xrd_lag48', 48)
        .assign(
            rd_cap=lambda x:
            (x['xrd']+0.8*x['xrd_lag12']+0.6*x['xrd_lag24']
             +0.4*x['xrd_lag36']+0.2*x['xrd_lag48'])/x['at'])
        .query('year>=1980')
        .pipe(port_group_cs, 3, 'all', 'time_avail_m', 'me')
        .query('port=="p1"')
        .pipe(predictor_out_clean, 'rd_cap'))

    df.to_parquet(predictors_dir/'rd_cap.parquet.gzip', compression='gzip')

predictor_rd_cap()

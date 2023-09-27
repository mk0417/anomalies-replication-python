import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Change in capex
# gr_capx
#
# Change in capex (one year)
# gr_capx_1yr
#
# Change in capex (three years)
# gr_capx_3yr
# ---------------
@print_log
def predictor_gr_capx():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'capx', 'ppent', 'at'])
        .merge(
            pd.read_parquet(
                download_dir/'signal_master_table.parquet.gzip',
                columns=['permno', 'time_avail_m', 'exchcd']),
            how='right', on=['permno', 'time_avail_m'])
        .sort_values(['permno', 'time_avail_m'], ignore_index=True)
        .assign(
            firm_age=lambda x:
            x.groupby('permno')['permno'].cumcount()+1,
            bdate=lambda x: np.datetime64('1926-07-01')+pd.offsets.MonthEnd(0),
            tmp=lambda x:
            (x['time_avail_m'].dt.to_period('M')-x['bdate'].dt.to_period('M'))
            .apply(lambda x: x.n)+1)
        .pipe(
            shift_var_month, 'permno', 'time_avail_m',
            'ppent', 'ppent_lag12', 12))

    df.loc[df['tmp']==df['firm_age'], 'firm_age'] = np.nan
    df.loc[df['capx'].isna() & df['firm_age']>=24, 'capx'] = (
        df['ppent'] - df['ppent_lag12'])

    df = (
        df.pipe(
            shift_var_month, 'permno', 'time_avail_m', 'capx', 'capx_lag12', 12)
        .pipe(
            shift_var_month, 'permno', 'time_avail_m', 'capx', 'capx_lag24', 24)
        .pipe(
            shift_var_month, 'permno', 'time_avail_m', 'capx', 'capx_lag36', 36)
        .assign(
            gr_capx=lambda x: (x['capx']-x['capx_lag24'])/x['capx_lag24'],
            gr_capx_1yr=lambda x:
            (x['capx_lag12']-x['capx_lag24'])/x['capx_lag24'],
            gr_capx_3yr=lambda x:
            x['capx']/((x['capx_lag12']+x['capx_lag24']+x['capx_lag36'])/3)))

    df_capx = df.pipe(predictor_out_clean, 'gr_capx')
    df_capx1 = df.pipe(predictor_out_clean, 'gr_capx_1yr')
    df_capx3 = df.pipe(predictor_out_clean, 'gr_capx_3yr')

    df_capx.to_parquet(predictors_dir/'gr_capx.parquet.gzip', compression='gzip')
    df_capx1.to_parquet(
        predictors_dir/'gr_capx_1yr.parquet.gzip', compression='gzip')
    df_capx3.to_parquet(
        predictors_dir/'gr_capx_3yr.parquet.gzip', compression='gzip')

predictor_gr_capx()

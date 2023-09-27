import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Cash-based operating profitability (no lag)
# cb_oper_prof
# ---------------
@print_log
def predictor_cb_oper_prof():
    df = (
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'exchcd',
                     'sic_crsp', 'shrcd', 'me'])
        .merge(
            pd.read_parquet(
                download_dir/'compustat_a_monthly.parquet.gzip',
                columns=['permno', 'time_avail_m', 'revt', 'cogs',
                         'xsga', 'xrd', 'rect', 'invt', 'xpp',
                         'drc', 'drlt', 'ap', 'xacc', 'at', 'ceq']),
            how='left', on=['permno', 'time_avail_m']))

    fillna_list = ['revt', 'cogs', 'xsga', 'xrd', 'rect',
                   'invt', 'xpp', 'drc', 'drlt', 'ap', 'xacc']
    for i in fillna_list:
        df[i] = df[i].fillna(0)

    for i in ['rect', 'invt', 'xpp', 'drc', 'drlt', 'ap', 'xacc']:
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m',
            i, i+'_lag12', 12)

    df = df.eval("""
        cb_oper_prof=((revt-cogs-(xsga-xrd))-(rect-rect_lag12) \
        -(invt-invt_lag12)-(xpp-xpp_lag12)+(drc+drlt-drc_lag12-drlt_lag12) \
        +(ap-ap_lag12)+(xacc-xacc_lag12))/at
        bm=ceq/me
        """)
    df.loc[df['bm']<=0, 'bm'] = np.nan
    df['bm'] = np.log(df['bm'])
    df.loc[
        (df['shrcd']>11) | (df['me'].isna()) | (df['bm'].isna())
        | (df['at'].isna())
        | ((df['sic_crsp']>=6000) & (df['sic_crsp']<7000)),
        'cb_oper_prof'] = np.nan
    df = df.pipe(predictor_out_clean, 'cb_oper_prof')

    df.to_parquet(predictors_dir/'cb_oper_prof.parquet.gzip', compression='gzip')

predictor_cb_oper_prof()

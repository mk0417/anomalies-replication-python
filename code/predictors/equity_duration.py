import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Equity duration
# equity_duration
# ---------------
@print_log
def predictor_equity_duration():
    df = (
        # * ib, ceq etc are in millions, as is csho
        pd.read_parquet(
            download_dir/'compustat_a_annual.parquet.gzip',
            columns=['permno', 'time_avail_m', 'fyear', 'datadate',
                     'ceq', 'ib', 'sale', 'prcc_f', 'csho', 'gvkey'])
        .pipe(shift_var_year, 'gvkey', 'time_avail_m', 'ceq', 'ceq_lag1', 1)
        .pipe(shift_var_year, 'gvkey', 'time_avail_m', 'sale', 'sale_lag1', 1)
        .assign(
            roe=lambda x: x['ib']/x['ceq_lag1'],
            g_eq=lambda x: x['sale']/x['sale_lag1']-1,
            cd=lambda x: x['ceq_lag1']*(x['roe']-x['g_eq']),
            # Autocorrelation coefficient for return on equity = 0.57
            # Cost of equity capital = 0.12
            # Autocorrelation coefficient for growth in sales/book value = 0.24
            # Long-run growth rate in sales/book value = 0.06
            roe1=lambda x: 0.57*x['roe']+0.12*(1-0.57),
            g_eq1=lambda x: 0.24*x['g_eq']+0.06*(1-0.24),
            bv1=lambda x: x['ceq']*(1+x['g_eq1']),
            cd1=lambda x: x['ceq']-x['bv1']+x['ceq']*x['roe1']))

    for i in range(2, 11):
        j = str(i-1)
        i = str(i)
        df['roe'+i] = 0.57*df['roe'+j]+0.12*(1-0.57)
        df['g_eq'+i] = 0.24*df['g_eq'+j]+0.06*(1-0.24)
        df['bv'+i] = df['bv'+j]*(1+df['g_eq'+i])
        df['cd'+i] = df['bv'+j]-df['bv'+i]+df['bv'+j]*df['roe'+i]

    df = df.eval("""
        md=1*cd1/(1+0.12)+2*cd2/(1+0.12)**2+3*cd3/(1+0.12)**3 \
        + 4*cd4/(1+0.12)**4+5*cd5/(1+0.12)**5+6*cd6/(1+0.12)**6 \
        + 7*cd7/(1+0.12)**7+8*cd8/(1+0.12)**8+9*cd9/(1+0.12)**9 \
        + 10*cd10/(1+0.12)**10
        pv=cd1/(1+0.12)+cd2/(1+0.12)**2+cd3/(1+0.12)**3 \
        + cd4/(1+0.12)**4+cd5/(1+0.12)**5+cd6/(1+0.12)**6 \
        + cd7/(1+0.12)**7+cd8/(1+0.12)**8+cd9/(1+0.12)**9 \
        + cd10/(1+0.12)**10
        me=prcc_f*csho
        equity_duration=md/me+(10+(1+0.12)/0.12)*(1-pv/me)
        """)

    df = (
        pd.concat([df]*12)
        .sort_values(['gvkey', 'time_avail_m'], ignore_index=True)
        .assign(
            month_inc=lambda x:
            x.groupby(['gvkey', 'time_avail_m'])['gvkey'].cumcount(),
            time_avail_m=lambda x:
            (x['time_avail_m'].dt.to_period('M')+x['month_inc'])
            .dt.to_timestamp()+pd.offsets.MonthEnd(0))
        .drop(columns='month_inc')
        .sort_values(['gvkey', 'time_avail_m', 'datadate'], ignore_index=True)
        .drop_duplicates(['gvkey', 'time_avail_m'], keep='last')
        .pipe(predictor_out_clean, 'equity_duration'))

    df.to_parquet(
        predictors_dir/'equity_duration.parquet.gzip', compression='gzip')

predictor_equity_duration()

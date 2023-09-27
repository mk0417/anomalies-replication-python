import sys
sys.path.insert(0, '../')
from functions import *


def port_master_data_nodlst(
        predictor, price_f=None, exchange_f=None, exfin_f=False):
    # Merge return and predictor data
    df = (
        # Return and stock meta information
        pd.read_parquet(
            download_dir/'signal_master_table.parquet.gzip',
            columns=['permno', 'time_avail_m', 'me',
                     'prc', 'exchcd', 'sic_crsp'])
        .merge(
            pd.read_parquet(
                download_dir/'crsp_monthly_raw.parquet.gzip',
                columns=['permno', 'time_avail_m', 'ret']),
            how='left', on=['permno', 'time_avail_m'])
        .rename(columns={'sic_crsp': 'sic'})
        .assign(
            prc=lambda x: x['prc'].abs(),
            ret=lambda x: x['ret']*100)
        .merge(
            # Predictor data
            pd.read_parquet(predictors_dir/(predictor+'.parquet.gzip')),
            how='right', on=['permno', 'time_avail_m'])
        # Get lagged information (predictor info on portfolio formation date)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'me', 'me_lag', 1)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'prc', 'prc_lag', 1)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'exchcd', 'exchcd_lag', 1)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              'sic', 'sic_lag', 1)
        .pipe(shift_var_month, 'permno', 'time_avail_m',
              predictor, predictor+'_lag', 1)
        .filter(
            ['permno', 'time_avail_m', 'ret', 'me_lag', 'prc_lag',
             'exchcd_lag', 'sic_lag', predictor+'_lag'])
        # Require lagged information is available when we form portfolios
        #  market value
        #  listing exchange
        #  predictor
        .dropna(subset=['me_lag', 'exchcd_lag', predictor+'_lag'], how='any'))

    if price_f:
        df = df.query('prc_lag>=@price_f')
    if exchange_f:
        df = df.query('exchcd_lag==@exchange_f')
    if exfin_f:
        df = df.query('sic_lag<6000 | sic_lag>6999')

    end_date = '-'.join([str(datetime.today().year-1), '12', '31'])
    df = (
        df.query('time_avail_m<=@end_date')
        .sort_values(['permno', 'time_avail_m'], ignore_index=True))
    return df

def port_monthly_loop_predictors(
        n_port, weight, bp, out_dir,
        price_f=None, exchange_f=None, exfin_f=False):
    if not os.path.exists(port_dir/out_dir):
        os.makedirs(port_dir/out_dir)

    predictor_master = pd.read_csv('../../predictors.csv')
    predictor_list = predictor_master['predictor'].to_list()
    for i in predictor_list:
        sign = predictor_master.query('predictor==@i')['Sign'].iloc[0]
        df = port_master_data_nodlst(i, price_f, exchange_f, exfin_f)
        df = port_monthly_ret(df, i, n_port, weight, bp)
        if sign == -1:
            df['ls_ret'] = df['ls_ret'] * -1

        df.to_csv(port_dir/out_dir/(i+'.csv'), index=False)

@print_log
def run_port_monthly():
    for i in ['ew', 'vw']:
        for j in ['all', 'nyse']:
            for k in [5, 10]:
                print(f'{i}-{j}-{k} ...')
                # no price, exchange and industry filter
                port_monthly_loop_predictors(
                    k, i, j, 'nodlst_'+i+'_'+j+'_port'+str(k))
                # exclude NASDAQ
                port_monthly_loop_predictors(
                    k, i, j, 'nodlst_'+i+'_'+j+'_port'+str(k)+'_ex_nasdaq',
                    exchange_f=[1, 2])
                # exclude financial industry
                port_monthly_loop_predictors(
                    k, i, j, 'nodlst_'+i+'_'+j+'_port'+str(k)+'_ex_fin',
                    exfin_f=True)
                # price>=5
                port_monthly_loop_predictors(
                    k, i, j, 'nodlst_'+i+'_'+j+'_port'+str(k)+'_price5',
                    price_f=5)
                # price>=5 and exclude NASDAQ
                port_monthly_loop_predictors(
                    k, i, j,
                    'nodlst_'+i+'_'+j+'_port'+str(k)+'_price5_ex_nasdaq',
                    price_f=5, exchange_f=[1, 2])
                # price>=5 and exclude financial industry
                port_monthly_loop_predictors(
                    k, i, j, 'nodlst_'+i+'_'+j+'_port'+str(k)+'_price5_ex_fin',
                    price_f=5, exfin_f=True)

run_port_monthly()

import sys
sys.path.insert(0, '../')
from functions import *


# If need to run a single predictor
def port_monthly_single_predictor(
        signal, n_port, weight, bp, out_dir,
        price_f=None, exchange_f=None, exfin_f=False):
    if not os.path.exists(port_dir/out_dir):
        os.makedirs(port_dir/out_dir)

    predictor_master = pd.read_csv('../../predictors.csv')
    predictor_list = predictor_master['predictor'].to_list()
    sign = predictor_master.query('predictor==@signal')['Sign'].iloc[0]
    df = port_master_data(signal, price_f, exchange_f, exfin_f)
    df = port_monthly_ret(df, signal, n_port, weight, bp)
    if sign == -1:
        df['ls_ret'] = df['ls_ret'] * -1

    df.to_csv(port_dir/out_dir/(signal+'.csv'), index=False)

@print_log
def run_port_monthly_single(signal):
    for i in ['ew', 'vw']:
        for j in ['all', 'nyse']:
            for k in [5, 10]:
                print(f'{i}-{j}-{k} ...')
                port_monthly_single_predictor(
                    signal, k, i, j, i+'_'+j+'_port'+str(k))
                port_monthly_single_predictor(
                    signal, k, i, j, i+'_'+j+'_port'+str(k)+'_ex_nasdaq',
                    exchange_f=[1, 2])
                port_monthly_single_predictor(
                    signal, k, i, j, i+'_'+j+'_port'+str(k)+'_ex_fin',
                    exfin_f=True)
                port_monthly_single_predictor(
                    signal, k, i, j, i+'_'+j+'_port'+str(k)+'_price5', price_f=5)
                port_monthly_single_predictor(
                    signal, k, i, j, i+'_'+j+'_port'+str(k)+'_price5_ex_nasdaq',
                    price_f=5, exchange_f=[1, 2])
                port_monthly_single_predictor(
                    signal, k, i, j, i+'_'+j+'_port'+str(k)+'_price5_ex_fin',
                    price_f=5, exfin_f=True)

def port_monthly_loop_predictors(
        n_port, weight, bp, out_dir,
        price_f=None, exchange_f=None, exfin_f=False):
    if not os.path.exists(port_dir/out_dir):
        os.makedirs(port_dir/out_dir)

    predictor_master = pd.read_csv('../../predictors.csv')
    predictor_list = predictor_master['predictor'].to_list()
    for i in predictor_list:
        sign = predictor_master.query('predictor==@i')['Sign'].iloc[0]
        df = port_master_data(i, price_f, exchange_f, exfin_f)
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
                port_monthly_loop_predictors(k, i, j, i+'_'+j+'_port'+str(k))
                # exclude NASDAQ
                port_monthly_loop_predictors(
                    k, i, j, i+'_'+j+'_port'+str(k)+'_ex_nasdaq',
                    exchange_f=[1, 2])
                # exclude financial industry
                port_monthly_loop_predictors(
                    k, i, j, i+'_'+j+'_port'+str(k)+'_ex_fin', exfin_f=True)
                # price>=5
                port_monthly_loop_predictors(
                    k, i, j, i+'_'+j+'_port'+str(k)+'_price5', price_f=5)
                # price>=5 and exclude NASDAQ
                port_monthly_loop_predictors(
                    k, i, j, i+'_'+j+'_port'+str(k)+'_price5_ex_nasdaq',
                    price_f=5, exchange_f=[1, 2])
                # price>=5 and exclude financial industry
                port_monthly_loop_predictors(
                    k, i, j, i+'_'+j+'_port'+str(k)+'_price5_ex_fin',
                    price_f=5, exfin_f=True)

run_port_monthly()

import sys
sys.path.insert(0, '../')
from functions import *


predictor_master = pd.read_csv('../../predictors.csv')
predictor_list = predictor_master['predictor'].to_list()

def port_t_table(port_sub_dir, n_port, in_sample=None):
    res = []
    for i in predictor_list:
        tmp = pd.read_csv(port_dir/port_sub_dir/(i+'.csv'))
        if in_sample:
            sample_start, sample_end = (
                predictor_master.query('predictor==@i')
                [['SampleStartYear', 'SampleEndYear']].iloc[0])
            tmp['date'] = pd.to_datetime(tmp['date'])
            tmp['year'] = tmp['date'].dt.year
            tmp = (
                tmp.copy().query('@sample_start<=year<=@sample_end')
                .sort_values('date', ignore_index=True))

        t = port_ret(tmp, n_port).iloc[-1, -1]
        res.append((i, t))

    df = (
        pd.DataFrame(res, columns=['predictor', 't'])
        .sort_values('predictor', ignore_index=True))
    return df

ew_all_port10 = port_t_table('ew_all_port10', 10)

vw_all_port10 = port_t_table('vw_all_port10', 10)

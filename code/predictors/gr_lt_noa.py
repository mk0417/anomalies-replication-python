import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Growth in long term net operating assets
# gr_lt_noa
# ---------------
@print_log
def predictor_gr_lt_noa():
    df = (
        pd.read_parquet(
            download_dir/'compustat_a_monthly.parquet.gzip',
            columns=['permno', 'time_avail_m', 'rect', 'invt', 'ppent',
                     'aco', 'intan', 'ao', 'ap', 'lco', 'lo', 'at', 'dp']))

    loop_list = ['rect', 'invt', 'ppent', 'aco', 'intan',
                 'ao', 'ap', 'lco', 'lo', 'at']
    for i in loop_list:
        df = df.pipe(
            shift_var_month, 'permno', 'time_avail_m', i, i+'_lag12', 12)

    df = (
        df.assign(
            gr_lt_noa=lambda x:
            (x['rect']+x['invt']+x['ppent']+x['aco']+x['intan']+x['ao']
             -x['ap']-x['lco']-x['lo'])/x['at']
            -(x['rect_lag12']+x['invt_lag12']+x['ppent_lag12']+x['aco_lag12']
              +x['intan_lag12']+x['ao_lag12']-x['ap_lag12']-x['lco_lag12']
              -x['lo_lag12'])/x['at_lag12']
            -(x['rect']-x['rect_lag12']+x['invt']-x['invt_lag12']
              +x['aco']-x['aco_lag12']
              -(x['ap']-x['ap_lag12']+x['lco']-x['lco_lag12'])-x['dp'])
            /((x['at']+x['at_lag12'])/2))
        .pipe(predictor_out_clean, 'gr_lt_noa'))

    df.to_parquet(predictors_dir/'gr_lt_noa.parquet.gzip', compression='gzip')

predictor_gr_lt_noa()

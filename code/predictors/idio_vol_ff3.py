import sys
sys.path.insert(0, '../')
from functions import *


# ---------------
# Realized (Total) Vol (Daily)
# realized_vol
#
# Idiosyncratic Risk (3 factor)
# idio_vol_ff3
#
# Skewness of daily idiosyncratic returns (3F model)
# return_skew_ff3
# ---------------
@njit
def ols_nb(data):
    k = data.shape[1] 
    # Find non-missing values
    notna_mask = ~np.isnan(data[:, 0])
    for i in range(1, k): 
        notna_mask = notna_mask & (~np.isnan(data[:, i]))

    # Dependent variable
    y = data[notna_mask, 0]
    # Independent variables
    X = data[notna_mask, 1:]
    # Number of observations
    n = y.shape[0]
    # Run regression if at least 15 daily return per stock per month
    if n >= 15: 
        # Add constant
        X = np.column_stack((np.ones(n), X))
        return np.linalg.inv(X.T@X) @ X.T @ y
    else:
        # There are k coefficients (constant + k-1 independent variables)
        return np.array([np.nan]*k)

@njit
def group_ols_nb_est(data):
    # We run regression in each group, so get number of groups first
    # It is the last group ID + 1
    n_groups = int(data[-1, 0]) + 1
    # Total number of observations (all groups)
    n_rows = data.shape[0] 
    # Results list
    res = []
    idx = 0
    for i in range(n_groups):
        idxend = idx + 1 
        # The while loop will store the first index for next group 
        # It is same group if group ID equals to previous group ID
        while idxend < n_rows and data[idxend-1, 0] == data[idxend, 0]:
            idxend += 1

        # Run regression in each group
        tmp = ols_nb(data[idx:idxend, 1:])
        # Append group ID and coefficients for all groups
        res.append(np.hstack((np.array([i]), tmp)))
        # Update start index for each group
        idx = idxend

    return res

@print_log
def predictor_idio_vol_ff3():
    df_d = pd.read_parquet(
        download_dir/'crsp_daily.parquet.gzip',
        columns=['permno', 'time_d', 'ret'])
    df_ff = pd.read_parquet(
        download_dir/'ff_daily.parquet.gzip',
        columns=['time_d', 'mktrf', 'smb', 'hml', 'rf'])
    # More than 100 millions obs
    df = db.sql("""
            select a.permno, last_day(a.time_d) as time_avail_m, a.time_d,
                a.ret-b.rf as ret, b.mktrf, b.smb, b.hml
            from df_d a join df_ff b
            on a.time_d=b.time_d
            order by permno, time_avail_m, a.time_d
        """).df()
    df['gid'] = df.groupby(['permno', 'time_avail_m']).ngroup()

    x_vars = ['mktrf', 'smb', 'hml']
    est_array = group_ols_nb_est(df[['gid', 'ret']+x_vars].to_numpy())
    col_names = ['gid', 'const'] + ['b_'+i for i in x_vars]
    est = (
        pd.DataFrame([i for i in est_array], columns=col_names)
        .astype({'gid': int}))
    df_ff3 = (
        df.drop_duplicates(['permno', 'time_avail_m'])
        .loc[:, ['permno', 'time_avail_m', 'gid']]
        .merge(est, how='inner', on='gid')
        .dropna()
        .drop(columns='gid')
        .sort_values(['permno', 'time_avail_m'], ignore_index=True))

    df = db.sql("""
        select permno, time_avail_m, stddev_samp(ret) as realized_vol,
            stddev_samp(resid) as idio_vol_ff3,
            skewness(resid) as return_skew_ff3
        from
        (select a.permno, a.time_avail_m, a.ret,
        a.ret-(b.const+b.b_mktrf*a.mktrf+b.b_smb*a.smb+b.b_hml*a.hml) as resid
        from df a join df_ff3 b
        on a.permno=b.permno and a.time_avail_m=b.time_avail_m)
        group by permno, time_avail_m
        order by permno, time_avail_m
        """).df()

    df1 = df.pipe(predictor_out_clean, 'realized_vol')
    df2 = df.pipe(predictor_out_clean, 'idio_vol_ff3')
    df3 = df.pipe(predictor_out_clean, 'return_skew_ff3')

    df1.to_parquet(
        predictors_dir/'realized_vol.parquet.gzip', compression='gzip')
    df2.to_parquet(
        predictors_dir/'idio_vol_ff3.parquet.gzip', compression='gzip')
    df3.to_parquet(
        predictors_dir/'return_skew_ff3.parquet.gzip', compression='gzip')

predictor_idio_vol_ff3()

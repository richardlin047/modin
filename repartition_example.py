import timeit
import ray
import modin.pandas as pd
from modin.config import NPartitions
import numpy as np

def get_ips(df):
    ips = [part.ip() for row in df._query_compiler._modin_frame._partitions for part in row]
    return ips

def run_map_reduce(df):
    counts = []
    for function in [lambda df: df.groupby('passenger_count').count, lambda df: df.groupby('passenger_count').sum, lambda df: df.groupby('passenger_count').prod, lambda df: df['passenger_count'].any, lambda df: df.memory_usage]:
        count = time_operation(function, df)
        counts.append(count)
    # print('The above MAP_REDUCE operation took approximately,', counts, "seconds")
    return counts

def run_map(df):
    counts = []
    #for function in [df.isna, df.applymap(lambda x: sum(x)), lambda : df.replace(0,5)]:
    for function in [lambda df: df.isna, lambda df: df.replace(0,5), lambda df: df['trip_id'].abs, lambda df: df.isin([1,2])]:
        count = time_operation(function, df)
        counts.append(count)
    # print('The above MAP operation took approximately,', counts, "seconds")
    return counts

def run_reduce(df):
    counts = []
    #for function in [df.isna, df.applymap(lambda x: sum(x)), lambda : df.replace(0,5)]:
    #df['trip_id'].to_datetime]
    for function in [lambda df: df['trip_id'].median, lambda df: df.nunique, lambda df: df['trip_id'].std, lambda df: df['trip_id'].var]:
        count = time_operation(function, df)
        counts.append(count)
    # print('The above REDUCE operation took approximately,', counts, "seconds")
    return counts

def run_fold(df):
    counts = []
    #NOTE putting this sleep seemed to eliminate the column mismatch issue
    #time.sleep(16)
    #for function in [df.isna, df.applymap(lambda x: sum(x)), lambda : df.replace(0,5)]:
    #df['trip_id'].to_datetime]
    #breakpoint()
    for function in [lambda df: df['trip_id'].rolling(2).count, lambda df: df['trip_id'].rolling(2).sum, lambda df: df['trip_id'].rolling(2).mean]:
        count = time_operation(function, df)
        counts.append(count)
    # print('The above FOLD operation took approximately,', counts, "seconds")
    return counts

def run_binary(df):
    counts = []
    #NOTE putting this sleep seemed to eliminate the column mismatch issue
    #time.sleep(16)
    #for function in [df.isna, df.applymap(lambda x: sum(x)), lambda : df.replace(0,5)]:
    #df['trip_id'].to_datetime]
    for function in [lambda df: df['trip_id'].add(1), lambda df: df['trip_id'].sub(1), lambda df: df['trip_id'].mul(2),
                     lambda df: df['trip_id'].eq(2), lambda df: df['trip_id'].pow(3)]:
        count = time_operation(function, df)
        counts.append(count)
    # print('The above BINARY operation took approximately,', counts, "seconds")
    return counts

def time_operation(function, df):
    times = []
    # NOTE that we're only doing 1 iteration rn since it caches the operation
    for i in range(5):
        # copy_start = timeit.default_timer()
        # actually quite fast!
        df_copy = df.copy()
        df_copy._query_compiler.finalize()
        # print("Time to df.copy: ", timeit.default_timer() - copy_start)
        start1 = timeit.default_timer()
        result = function(df_copy)
        # print(df)
        # force evaluation without materialization
        df._query_compiler.finalize()
        val = timeit.default_timer() - start1
        times.append(val)
        # break
    return sum(times)/len(times)

def create_df(df, parts):
    # create modin frame
    frame = df._query_compiler._modin_frame.__constructor__(
        parts,
        df._query_compiler.index,
        df._query_compiler.columns,
    )

    # create query compiler
    qc = df._query_compiler.__constructor__(frame)

    # create dataframe
    df = pd.DataFrame(
        query_compiler=qc
    )
    return df

def repartition(df, columnPartition=True):
    axis = 0 if columnPartition else 1
    print("REPARTITIONING AXIS ", axis)
    parts = df._query_compiler._modin_frame._frame_mgr_cls.map_axis_partitions(
           axis,
           df._query_compiler._modin_frame._partitions,
           lambda df: df,
           lengths=[df.shape[axis]],
           # keep_partitioning should be False, True maintains original lengths
           keep_partitioning=False,
       )
    return create_df(df, parts)

def blockPartition(df):
    print("BLOCK REPARTITIONING")
    num_partitions = df._query_compiler._modin_frame._partitions.shape[0]
    new_partitions = int(np.floor(np.sqrt(num_partitions))) + 1
    print("New num partitions: ", new_partitions)
    column_widths = []
    sum_cols = 0
    while sum_cols < df.shape[1]:
        curr = min(df.shape[1]-sum_cols, df.shape[1] // new_partitions + 1)
        column_widths.append(curr)
        sum_cols += curr
    print("Column Widths: ", column_widths)
    row_lengths = []
    sum_rows = 0
    while sum_rows < df.shape[0]:
        curr = min(df.shape[0]-sum_rows, int(df.shape[0] // new_partitions + 1))
        row_lengths.append(curr)
        sum_rows += curr
    print("Row Lengths: ", row_lengths)
    parts = df._query_compiler._modin_frame._frame_mgr_cls.map_axis_partitions(
           0,
           df._query_compiler._modin_frame._partitions,
           lambda df: df,
           lengths=row_lengths,
           # keep_partitioning should be False, True maintains original lengths
           keep_partitioning=False,
       )

    parts = df._query_compiler._modin_frame._frame_mgr_cls.map_axis_partitions(
           1,
           parts,
           lambda df: df,
           lengths=column_widths,
           # keep_partitioning should be False, True maintains original lengths
           keep_partitioning=False,
       )

    return create_df(df, parts)


if __name__=="__main__":
    # ray.init(address="auto")
    ray.init()
    print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
    '''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))
    # TODO: Set number of partitions if needed
    # NPartitions.put(5)
    # columns_names = [
    #     "trip_id", "vendor_id", "pickup_datetime", "dropoff_datetime", "store_and_fwd_flag",
    #     "rate_code_id", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
    #     "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
    #     "tolls_amount", "ehail_fee", "improvement_surcharge", "total_amount", "payment_type",
    #     "trip_type", "pickup", "dropoff", "cab_type", "precipitation", "snow_depth", "snowfall",
    #     "max_temperature", "min_temperature", "average_wind_speed", "pickup_nyct2010_gid",
    #     "pickup_ctlabel", "pickup_borocode", "pickup_boroname", "pickup_ct2010",
    #     "pickup_boroct2010", "pickup_cdeligibil", "pickup_ntacode", "pickup_ntaname", "pickup_puma",
    #     "dropoff_nyct2010_gid", "dropoff_ctlabel", "dropoff_borocode", "dropoff_boroname",
    #     "dropoff_ct2010", "dropoff_boroct2010", "dropoff_cdeligibil", "dropoff_ntacode",
    #     "dropoff_ntaname", "dropoff_puma",
    # ]
    parse_dates = ["pickup_datetime", "dropoff_datetime"]
    # df = pd.read_csv('s3://modin-datasets/trips_data.csv', names=columns_names,
    #                  header=None, parse_dates=parse_dates)
    df = pd.read_csv('taxi_1m.csv', parse_dates=parse_dates)
    print("Dataframe shape: ", df.shape)
    print(f"Partition shape before: {df._query_compiler._modin_frame._partitions.shape}")
    print(f"IPs before: {get_ips(df)}")
    print("Num unique IPs: ", len(set(get_ips(df))))

    # Uncomment specific line
    # df = blockPartition(df)  # sqrt(n) block partitions, doesn't work locally
    # df = repartition(df, columnPartition=True) # Column partitions
    # df = repartition(df, columnPartition=False) # Row partitions
    print(f"Partition shape after partitioning: {df._query_compiler._modin_frame._partitions.shape}")

    print(df.head())
    funcs = [run_map_reduce, run_map, run_reduce, run_fold, run_binary]
    counts = []
    for f in funcs:
        print("Running ", f)
        try:
            counts.append(f(df))
        except Exception as error:
            print(error)

    print(counts)
    # for c in counts:
    #     print(c)

    print(f"IPs after: {get_ips(df)}")
    print("Num unique IPs: ", len(set(get_ips(df))))

    # Uncomment for local
    # ray.shutdown()

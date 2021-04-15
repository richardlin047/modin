import time
import timeit
import ray
import modin.pandas as pd
from modin.config import NPartitions
import s3fs
def get_ips(df):
    ips = [part.ip() for row in df._query_compiler._modin_frame._partitions for part in row]
    return ips

def run_map_reduce(df):
    counts = []
    for function in [df.groupby('passenger_count').count, df.groupby('passenger_count').sum, df.groupby('passenger_count').prod, df['passenger_count'].any, df.memory_usage]:
        count = time_operation(lambda : function, df)
        counts.append(count)
    return counts
    print('The above MAP_REDUCE operation took approximately,', counts, "seconds")

def run_map(df):
    counts = []
    #for function in [df.isna, df.applymap(lambda x: sum(x)), lambda : df.replace(0,5)]:
    for function in [df.isna, lambda : df.replace(0,5), df['trip_id'].abs, lambda: df.isin([1,2])]:
        count = time_operation(function,df)
        counts.append(count)
    return counts
    print('The above MAP operation took approximately,', counts, "seconds")

def run_reduce(df):
    counts = []
    #for function in [df.isna, df.applymap(lambda x: sum(x)), lambda : df.replace(0,5)]:
    #df['trip_id'].to_datetime]
    for function in [df['trip_id'].median, df.nunique,df['trip_id'].std, df['trip_id'].var]:
        count = time_operation(function,df)
        counts.append(count)
    return counts
    print('The above REDUCE operation took approximately,', counts, "seconds")

def run_fold(df):
    counts = []
    #NOTE putting this sleep seemed to eliminate the column mismatch issue
    #time.sleep(16)
    #for function in [df.isna, df.applymap(lambda x: sum(x)), lambda : df.replace(0,5)]:
    #df['trip_id'].to_datetime]
    #breakpoint()
    for function in [df['trip_id'].rolling(2).count, df['trip_id'].rolling(2).sum,df['trip_id'].rolling(2).mean]:
        count = time_operation(function,df)
        counts.append(count)
    return counts
    print('The above FOLD operation took approximately,', counts, "seconds")

def run_binary(df):
    counts = []
    #NOTE putting this sleep seemed to eliminate the column mismatch issue
    #time.sleep(16)
    #for function in [df.isna, df.applymap(lambda x: sum(x)), lambda : df.replace(0,5)]:
    #df['trip_id'].to_datetime]
    for function in [df['trip_id'].add(1), df['trip_id'].sub(1),df['trip_id'].mul(2),df['trip_id'].eq(2),df['trip_id'].pow(3)]:
        count = time_operation(lambda : function, df)
        counts.append(count)
    return counts
    print('The above BINARY operation took approximately,', counts, "seconds")

def time_operation(function_call,df):
    times = []
    # NOTE that we're only doing 1 iteration rn since it caches the operation
    for i in range(5):
        start1 = timeit.default_timer()
        print(function_call())
        print(df)
        val = timeit.default_timer() - start1
        times.append(val)
        break
    return sum(times)/len(times)

if __name__=="__main__":
    ray.init(address="auto")
    print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
    '''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))
    # TODO: Set number of partitions if needed
    # NPartitions.put(5)
    columns_names = [
        "trip_id", "vendor_id", "pickup_datetime", "dropoff_datetime", "store_and_fwd_flag",
        "rate_code_id", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
        "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "ehail_fee", "improvement_surcharge", "total_amount", "payment_type",
        "trip_type", "pickup", "dropoff", "cab_type", "precipitation", "snow_depth", "snowfall",
        "max_temperature", "min_temperature", "average_wind_speed", "pickup_nyct2010_gid",
        "pickup_ctlabel", "pickup_borocode", "pickup_boroname", "pickup_ct2010",
        "pickup_boroct2010", "pickup_cdeligibil", "pickup_ntacode", "pickup_ntaname", "pickup_puma",
        "dropoff_nyct2010_gid", "dropoff_ctlabel", "dropoff_borocode", "dropoff_boroname",
        "dropoff_ct2010", "dropoff_boroct2010", "dropoff_cdeligibil", "dropoff_ntacode",
        "dropoff_ntaname", "dropoff_puma",
    ]
    parse_dates = ["pickup_datetime", "dropoff_datetime"]
    df = pd.read_csv('s3://modin-datasets/trips_data.csv', names=columns_names,
                     header=None, parse_dates=parse_dates)
    print(f"Partition shape before: {df._query_compiler._modin_frame._partitions.shape}")
    print(f"IPs before: {get_ips(df)}")


    print(f"IPs Inter: {get_ips(df)}")

# Returns np.array of column partitions
    #parts = df._query_compiler._modin_frame._frame_mgr_cls.map_axis_partitions(
    #    0,
    #    df._query_compiler._modin_frame._partitions,
    #    lambda df: df,
    #    # the lengths are correct, don't split up column
    #    lengths=[df.shape[0]],
    #    # keep_partitioning should be False, True maintains original lengths
    #    keep_partitioning=False,
    #    )
    # Uncomment to repartition into full row partitions
    parts = df._query_compiler._modin_frame._frame_mgr_cls.map_axis_partitions(
            1,
            df._query_compiler._modin_frame._partitions,
            lambda df: df,
            # the lengths are correct
            lengths=[df.shape[1]],
            )

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
    print(df.head())
    count1 = run_map_reduce(df)
    count2 = run_map(df)
    count3 = run_reduce(df)

    #count4 = run_fold(df)
    count4 = 0
    count5 = run_binary(df)
    print(count1)
    print(count2)
    print(count3)
    print(count4)
    print(count5)
    #print(count1,count2,count3,count4,count5)
    print(f"Partition shape after: {df._query_compiler._modin_frame._partitions.shape}")
    print(f"IPs after: {get_ips(df)}")

import modin.pandas as pd
from modin.config import NPartitions

def get_ips(df):
    ips = [part.ip() for row in df._query_compiler._modin_frame._partitions for part in row]
    return ips

if __name__=="__main__":
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

    # Returns np.array of column partitions
    parts = df._query_compiler._modin_frame._frame_mgr_cls.map_axis_partitions(
        0,
        df._query_compiler._modin_frame._partitions,
        lambda df: df,
        # the lengths are correct, don't split up column
        lengths=[df.shape[0]],
        # keep_partitioning should be False, True maintains original lengths
        keep_partitioning=False,
    )
    # Uncomment to repartition into full row partitions
    # parts = df._query_compiler._modin_frame._frame_mgr_cls.map_axis_partitions(
    #     1,
    #     parts,
    #     lambda df: df,
    #     # the lengths are correct
    #     lengths=[df.shape[1]],
    # )

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

    print(f"Partition shape after: {df._query_compiler._modin_frame._partitions.shape}")
    print(f"IPs after: {get_ips(df)}")

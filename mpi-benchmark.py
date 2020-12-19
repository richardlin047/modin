import modin.pandas as pd
import pandas as old_pd
import time
import pdb

# s3_path = "s3://dask-data/nyc-taxi/2015/yellow_tripdata_2015-01.csv"
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
parse_dates=["pickup_datetime", "dropoff_datetime"]
s3_path = 'https://modin-datasets.s3.amazonaws.com/trips_data.csv'

use_modin = True

if __name__ == '__main__':
    if use_modin:
        # from dask.distributed import Client
        # client = Client('tcp://172.31.5.10:8786', n_workers=6)
        import modin.pandas as pd
    else:
        import pandas as pd
    start = time.time()
    # TODO: Load the csv into an object and then read_csv rather than create df directly from s3
    # Should test loading, not network bandwidth
    # pandas_df = old_pd.read_csv(s3_path, parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"], quoting=3)
    # pandas_df = pd.read_csv('https://modin-datasets.s3.amazonaws.com/trips_data.csv', names=columns_names,
    #                   header=None, parse_dates=parse_dates, chunksize = 1000)
    pandas_df = pd.DataFrame({"trip_distance":[1,2,3,4,5], "b":[6,7,8,9,10]})
    # pandas_df = pd.DataFrame([[1,2],[6,7]], columns=["trip_distance", "b"])
    end = time.time()
    pandas_duration = end - start
    print(pandas_duration)

    time.sleep(1)
    # doesn't work on mpi
    start = time.time()
    pdb.set_trace()
    pandas_count = pandas_df.count()
    print(pandas_count)
    end = time.time()
    pandas_duration = end - start
    print(pandas_duration)


    start = time.time()
    pandas_isnull = pandas_df.isnull()
    end = time.time()
    pandas_duration = end - start
    print(pandas_duration)

    time.sleep(3)
    start = time.time()
    # pdb.set_trace()
    trip_distance_pandas = pandas_df["trip_distance"]
    # print("between")
    rounded_trip_distance_pandas = trip_distance_pandas.apply(round)
    end = time.time()
    pandas_duration = end - start
    print(pandas_duration)

    # doesn't work on mpi
    start = time.time()
    pandas_df["rounded_trip_distance"] = rounded_trip_distance_pandas
    end = time.time()
    pandas_duration = end - start
    print(pandas_duration)


    start = time.time()
    # pandas_groupby = pandas_df.groupby(by="trip_distance")
    # count doesn't work on mpi
    pandas_groupby = pandas_df.groupby(by="rounded_trip_distance").count()
    end = time.time()
    pandas_duration = end - start
    print(pandas_duration)


import modin.pandas as pd
import pandas as old_pd
import time

s3_path = "s3://dask-data/nyc-taxi/2015/yellow_tripdata_2015-01.csv"

start = time.time()
pandas_df = old_pd.read_csv(s3_path, parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"], quoting=3)
end = time.time()
pandas_duration = end - start
print(pandas_duration)


start = time.time()
pandas_count = pandas_df.count()
end = time.time()
pandas_duration = end - start
print(pandas_duration)


start = time.time()
pandas_isnull = pandas_df.isnull()
end = time.time()
pandas_duration = end - start
print(pandas_duration)


start = time.time()
rounded_trip_distance_pandas = pandas_df["trip_distance"].apply(round)
end = time.time()
pandas_duration = end - start
print(pandas_duration)


start = time.time()
pandas_df["rounded_trip_distance"] = rounded_trip_distance_pandas
end = time.time()
pandas_duration = end - start
print(pandas_duration)


start = time.time()
pandas_groupby = pandas_df.groupby(by="rounded_trip_distance").count()
end = time.time()
pandas_duration = end - start
print(pandas_duration)


import pandas as pd

df = pd.read_csv("s3://dask-data/nyc-taxi/2015/yellow_tripdata_2015-01.csv", parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"], quoting=3)

TOTAL_ROWS = len(df)
pandas_df[:TOTAL_ROWS].to_csv(path_or_buf="5.csv")
pandas_df[:TOTAL_ROWS//2].to_csv(path_or_buf="4.csv")
pandas_df[:TOTAL_ROWS//4].to_csv(path_or_buf="3.csv")
pandas_df[:TOTAL_ROWS//8].to_csv(path_or_buf="2.csv")
pandas_df[:TOTAL_ROWS//16].to_csv(path_or_buf="1.csv")

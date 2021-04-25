import pandas as pd
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

# Scale up dataframe
df = pd.concat([df] * 100, axis=0, ignore_index=True)
df = pd.concat([df] * 3, axis=1)

print(df)

# Export to csv
df.to_csv('taxi_1m.csv', index=False)
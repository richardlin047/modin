import os
import csv
import sys
import numpy as np
import time
from importlib import reload

# Change iterations to run function
iters = 5

def benchmark(write_file, read_file):
    print(f"Reading from: {read_file}")
    duration_all = []
    
    start = time.time()
    pandas_df = pd.read_csv(read_file, parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"], quoting=3)
    end = time.time()
    print("Read_csv duration: ", end-start)

    durs = []
    for _ in range(iters):
        start = time.time()
        pandas_count = pandas_df.count()
        end = time.time()
        pandas_duration = end - start
        durs.append(pandas_duration)
    print("Count durations:")
    print(durs)
    duration_all.append(durs)

    
    durs = []
    for _ in range(iters):
        start = time.time()
        pandas_isnull = pandas_df.isnull()
        end = time.time()
        pandas_duration = end - start
        durs.append(pandas_duration)
    print("Isnull durations:")
    print(durs)
    duration_all.append(durs)

    durs = []
    for _ in range(iters):
        start = time.time()
        rounded_trip_distance_pandas = pandas_df["trip_distance"].apply(round)
        end = time.time()
        pandas_duration = end - start
        durs.append(pandas_duration)
    print("Apply durations:")
    print(durs)
    duration_all.append(durs)
    
    durs = []
    for _ in range(iters):
        start = time.time()
        pandas_df["rounded_trip_distance"] = rounded_trip_distance_pandas
        end = time.time()
        # pandas_df.drop("rounded_trip_distance") # reset
        pandas_duration = end - start
        durs.append(pandas_duration)
    print("Assign durations:")
    print(durs)
    duration_all.append(durs)

    durs = []
    for _ in range(iters):
        start = time.time()
        pandas_groupby = pandas_df.groupby(by="rounded_trip_distance").count()
        end = time.time()
        pandas_duration = end - start
        durs.append(pandas_duration)
    print("Groupby durations:")
    print(durs)
    duration_all.append(durs)

    print("Done benchmarking. All durs: ")
    print(duration_all)
    with open(write_file, 'a') as f:
        writer = csv.writer(f)
        writer.writerow(duration_all)


# pass in file_name, num_cpus, num_rows
# Ex: `python3 engine_weak_scale_benchmark.py writing_to.csv 16 5`
# We scaled dataset with cpus where size = 2**(num_cpu-1)/16
# 5.csv is full dataset and 1.csv is 1/16 of dataset
# Optional: Launch engine_benchmark.py using benchmark_launch.py to run everything in one go
if __name__ == "__main__":
    write_file = sys.argv[1] # file writing to
    num_cpus = sys.argv[2] # num_cpu
    num_rows = sys.argv[3] # num_rows for file reading
    os.environ["MODIN_ENGINE"] = sys.argv[4]
    
    os.environ["MODIN_CPUS"] = num_cpus

    print("num_cpus, csv name (rows)")
    print(f"{str(num_cpus)}, {str(num_rows)}")
    read_file = num_rows + ".csv"
    benchmark(write_file, read_file)

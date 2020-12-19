import subprocess
import os

TOTAL_ROWS = 12748986

# File can be modified to run strong scaling
# Run this file using `python3 benchmark_launch.py write_file.csv mpi`
write_file = sys.argv[1] # file writing to
engine = sys.argv[2] # mpi, ray, or dask
try:
    os.remove(name)
except OSError:
    pass

# We scaled dataset with cpus where size = 2**(num_cpu-1)/16
# 5.csv is full dataset and 1.csv is 1/16 of dataset
for i in range(1,6):
    num_cpus = 2**(i-1)
    num_rows = i
    command = ["python3", "engine_benchmark.py", write_file, str(num_cpus), str(num_rows), engine]
    print(f"Running benchmark {i}: {command}")
    subprocess.run(command)
    print("Benchmark {i} completed")

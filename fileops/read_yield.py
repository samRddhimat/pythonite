import psutil
import os
import time
import datetime

# -----------------------------
# Generator function using yield
# -----------------------------
def read_file_generator(file_path):

    with open(file_path, "r", encoding="latin-1") as file:

        for line in file:
            yield line.rstrip("\n")

def append_to_each_line(file_path, append_items):

    temp_file = file_path + ".tmp"

    with open(temp_file, "w") as out_file:
        print(temp_file, file_path)
        
        count = 0
        for original_line, extra in zip(
                read_file_generator(file_path),
                append_items):

            if count == 0:
                out_file.write(f"lineno,cpupct,memmb,ycpupct,ymemmb\n")
                count += 1
                continue
            out_file.write(f"{original_line},{extra[1]},{extra[2]:.2f}\n")
    # Replace original file
    ##    os.replace(temp_file, file_path)

def pop_list(metrics_list):
    for item in metrics_list:
        yield item
# -----------------------------
# Resource monitor
# -----------------------------
process = psutil.Process(os.getpid())

print(process)
file_path = "D:\\Repository\\python\\movie\\movie_metadata.csv"

##file_path = "D:\\Repository\\python\\movie\\mini.csv"
print("Processing file using yield()...\n")

metrics = []
# Consume generator

started = datetime.datetime.now()

for line_number, line in enumerate(read_file_generator(file_path), start=1):

    # Print line
##    print(f"Line {line_number}: {line.strip()}")

    # CPU usage
    cpu_percent = psutil.cpu_percent(interval=0.1)

    # Memory usage (MB)
    memory_mb = process.memory_info().rss / (1024 * 1024)

    metrics.append([line_number,cpu_percent,memory_mb])

    # Simulate processing
#    time.sleep(0.5)

##print(metrics)

file_path = "D:\\Repository\\python\\movie\\metrics.csv"

append_to_each_line(file_path, pop_list(metrics))

print(f"Started at {started} and finished at {datetime.datetime.now()}")
##for l in metrics:
##    print(f"{l[0]},{l[1]},{l[2]}")



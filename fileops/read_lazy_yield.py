import psutil
import os
import time

# -----------------------------
# Generator function using yield
# -----------------------------
def read_file_generator(file_path):

    with open(file_path, "r") as file:

        for line in file:
            yield line

# -----------------------------
# Resource monitor
# -----------------------------
process = psutil.Process(os.getpid())

file_path = "D:\\Repository\\python\\movie\\movie_metadata.csv"

print("Processing file using yield()...\n")

# Consume generator
for line_number, line in enumerate(read_file_generator(file_path), start=1):

    # Print line
    print(f"Line {line_number}: {line.strip()}")

    # CPU usage
    cpu_percent = psutil.cpu_percent(interval=0.1)

    # Memory usage (MB)
    memory_mb = process.memory_info().rss / (1024 * 1024)

    print(f"CPU Usage: {cpu_percent}%")
    print(f"Memory Usage: {memory_mb:.2f} MB")
    print("-" * 40)
    metrics.append([line_number,cpu_percent,memory_mb])

    # Simulate processing
#    time.sleep(0.5)
print(metrics)

for l in metrics:
    print(f"{l[0]},{l[1]},{l[2]}")


with open("D:\Repository\python\movie\metrics.csv","a") as file:
    file.write(f"lineno,cpupct,memmb\n")
    for l in metrics:
##        print(f"{l[0]},{l[1]},{l[2]}")
        file.write(f"{l[0]},{l[1]},{l[2]}\n")

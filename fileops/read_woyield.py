import psutil
import os
import time
import datetime


# File path
file_path = "D:\Repository\python\movie\movie_metadata.csv"

# Current process
process = psutil.Process(os.getpid())

started = datetime.datetime.now()

print("Reading file...\n")

metrics = []
with open(file_path, "r",encoding="utf-16") as file:

    for line_number, line in enumerate(file, start=1):

        # Print file content
##        print(f"Line {line_number}: {line.strip()}")

        # Resource usage
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory_mb = process.memory_info().rss / (1024 * 1024)

##        print(f"CPU Usage: {cpu_percent}%")
##        print(f"Memory Usage: {memory_mb:.2f} MB")
##        print("-" * 40)

        metrics.append([line_number,line[0:15],cpu_percent,f"{memory_mb:.2f}"])
##        metrics+=([line_number,cpu_percent,memory_mb])
        # Simulate processing delay
##        time.sleep(0.1)


##print(metrics)

for l in metrics:
    print(f"{l[0]},{l[1]},{l[2]}")
with open("D:\Repository\python\movie\metrics.csv","w") as file:
    
    for l in metrics:
##        print(f"{l[0]},{l[1]},{l[2]}")
        file.write(f"{l[0]},{l[1]},{l[2]}\n")

print(f"Started at {started} and finished at {datetime.datetime.now()}")

import time

nums = [4, 2, 4, 5, 2, 3, 1, 3]

distinct = []
start = time.perf_counter()
for i in nums:
    # manually check if already present
    found = False
    for j in distinct:
        if i == j:
            found = True
            break
    if not found:
        distinct.append(i)
end = time.perf_counter()

print(f"actual is {nums}, distinct is {distinct}")
print(f"started at {start}, completed at {end}, duration is {end-start} sec(s)");

print("Using Set")
nums = [4, 2, 4, 5, 2, 3, 1, 3]
seen = {}
seen2={2,3,4,5,6,7}
distinct = []

start = time.perf_counter()
for i in nums:
    if i not in seen:
        seen[i] = True
##        print(seen2[i])
        distinct.append(i)
        print(i)
end = time.perf_counter()
print(distinct,"\n", seen, "\n",seen2)

print( f"{5 %2, 5/2, 5//2, 4%10}")

print(f"started at {start}, completed at {end}, duration is {end-start} sec(s)");

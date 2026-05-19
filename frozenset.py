a = frozenset([1,2])
b = frozenset([3,4])

print(a | b)

print(2|3)



#print([5,6]|[7,8])

prefix_map = {frozenset(['raw', 'processed']): 's3://my-pipeline-bucket'}
print(prefix_map[frozenset(['raw', 'processed'])])  # 's3://my-pipeline-bucket'

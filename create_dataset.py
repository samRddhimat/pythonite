import numpy as np
import random

print(reversed('Python'))
elems = ['a','b','c','d','e']
print(len(elems))
print(len(elems)/2)
key1 = [1] *10
key2 = [2] *50#000000

# print(key1, key2)

key = key1 + key2

# print(key)

random.shuffle(key)
# print(key)

value1 = list(np.random.randint(1,100,len(key1)))
value2 = list(np.random.randint(1,100,len(key2)))

value = value1 + value2
random.shuffle(value)
list1 = list(zip(key,value))


print(list1)
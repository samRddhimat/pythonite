def gen():
    for n in range(100):
        yield n

numbers = gen()
print(numbers, type(numbers), type(gen()))

print(next(numbers))

print('next of gen() is ',next(gen()))
print('next of gen() is ',next(gen()))
print(50 in numbers)

print('next of gen() is ',next(gen()))

print(next(numbers))




#for _ in range(5):
#    print("Hello",_)


for _, value in [(1, 'a'), (2, 'b'), (3, 'c')]:
    print(value,_)

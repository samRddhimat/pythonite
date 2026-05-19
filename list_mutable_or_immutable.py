
fruits = ["grapes","banana",'apple',"mango","kiwi"]

frt_bkp = fruits.copy()

frt_bkp.append("orange")

for f in fruits:
    if "a" in f:
        fruits.remove(f)
        fruits = fruits.copy()
print(fruits)

print(frt_bkp)

print(id(fruits),id(frt_bkp))


a = [1,2,3]

a[1] = 1000

print(a)



lst = []
for i in range(1000):
    lst.append(i)
    print(len(lst), lst.__sizeof__())

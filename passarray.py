
def getnames(namearr):
    for i in namearr:
        print(i)

cstr = "hello what's up"
cstr_arr = cstr.split(" ")

getnames(cstr_arr)

cstr_lst = [el for el in cstr_arr if len(el) > 4]

print(cstr_lst)


nums = [1, 2, 3]
print(id(nums))  # e.g., 140735405875200
nums.append(4)
print(id(nums))

print(id(cstr_arr))
cstr_arr.append("nth constant")

print(id(cstr_arr))

print("x"*5)
x = 10
print(id(x), type(x))    # e.g., 140735405850704
x += 0.5
print(id(x), type(x))

print(5/2, 5//2)



l = [1,2,3]

m = l

print(id(l), id(m),end = "\n"*2)
l.append(4)

print(f"l {l}, m {m}")
print(id(l), id(m),end = "\n"*2)


m.append(5)

print(f"l {l}, m {m}")
print(id(l), id(m),end = "\n"*2)

m = m +  [6]

print(f"l {l}, m {m}")
print(id(l), id(m))

l[0] = 100

print(f"l {l}, m {m}")
print(id(l), id(m))

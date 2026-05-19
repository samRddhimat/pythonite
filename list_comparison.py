

a = [77,100]
b = [77,100]

print(a, " ", b)
if ( a == b):
    print("same by ==")
    print(f"id of a {id(a)} and b {id(b)}")
else:
    print("different by ==")
    print(f"id of a {id(a)} and b {id(b)}")


if ( a is b):
    print("same by is")
    print(f"id of a {id(a)} and b {id(b)}")
else:
    print("different by is")
    print(f"id of a {id(a)} and b {id(b)}")


c = 100
d = 100

print(c, " ", d, "\n"*3)

if ( c == d):
    print("same by ==")
    print(f"id of c {id(c)} and d {id(d)}")
else:
    print("different by ==")
    print(f"id of c {id(c)} and d {id(d)}")


if ( c is d):
    print("same by is")
    print(f"id of c {id(c)} and d {id(d)}")
else:
    print("different by is")
    print(f"id of c {id(c)} and d {id(d)}")

print(c, " ", d, "\n"*3)

ax = 2000
fx = ax
fx = fx + 1 

if ( ax == fx):
    print("same by ==")
    print(f"id of ax {id(ax)} anfx fx {id(fx)}")
else:
    print("fxifferent by ==")
    print(f"id of ax {id(ax)} anfx fx {id(fx)}")


if ( ax is fx):
    print("same by is")
    print(f"id of ax {id(ax)} anfx fx {id(fx)}")
else:
    print("fxifferent by is")
    print(f"id of ax {id(ax)} anfx fx {id(fx)}")
    
print(ax, fx)

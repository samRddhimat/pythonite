


def tricky(a, b=[]):
    b.append(a)
    return b

x = tricky(1)
y = tricky(2)
z = tricky(3)

print(x,y,z)

print(id(x), id(y), id(z))

##In Python, default arguments are evaluated only once—at the time the function is defined, not each time it is called.
##The list b=[] is created once when the function is defined.
##That same list is reused across all function calls.

##Why this happens
######Lists are mutable
######Default arguments are shared across calls
######So each call modifies the same list


def tricky_cor(a, b=None):
    if b is None:
        b = []
    b.append(a)
    return b

x = tricky_cor(1)
y = tricky_cor(2)
z = tricky_cor(3)

print(x,y,z)
print(id(x), id(y), id(z))

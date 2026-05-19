def calculate(a, b):
  dict1 = {"sum": a + b, "product": a * b}
  dict2 = {"div": a / b, "mod": a % b}
  return dict1, dict2

def calc_lst(x,y):
    return[x+y, x*y], [x%y] #[c if x/y > x%y else x%y ]

result, res = calculate(3, 4)

print("\n", result, type(result), res, type(res), "\n")

a = set()
a = (2,3)
print(type(a))
def get_user():
    return ("Alice", 30, True)

print(type(get_user()))

name, age, is_active = get_user()

print(name, age, is_active )


x = int(input("enter x"))
y = int(input("enter y \n"))
##print(type([x]),type(x[0]), type(x[1]))
      
result, res = calc_lst(x,y)

print("\n", result, type(result), res, type(res), "\n")

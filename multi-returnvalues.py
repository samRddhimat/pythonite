def calculate(a, b):
    return {
        "sum": a + b,
        "product": a * b
    }

result = calculate(3, 4)

print(result, type(result))

a = set()
a = (2,3)
print(type(a))
def get_user():
    return ("Alice", 30, True)

print(type(get_user()))

name, age, is_active = get_user()

print(name, age, is_active )

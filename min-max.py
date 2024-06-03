def find_min_max(numbers):
    if not numbers:
        return None, None
    
    min_val = numbers[0]
    max_val = numbers[0]
    print(f"min is {min_val} max is {max_val}")
    for num in numbers:
        if(num < min_val):
            print(num, min_val)
            min_val = num
        if(num > max_val):
            max_val = num
    
    return min_val, max_val

numbers = [34,5,1,7,15,0,-1,45,999]

min, max = find_min_max(numbers)

print(f"minimum is {min} and maximum is {max}" )
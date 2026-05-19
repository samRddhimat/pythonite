def mystery(s):
    result = []
    vowels = False

    for i, ch in enumerate(reversed(s)):
        if ch.isalpha():
            if vowels:
                result.append(ch.upper())
            elif ch.lower() in 'aeiou':
                vowels = True

        print(i)

    return " ".join(result)

word = "Monsor-Codes123"

print(mystery(word))
            

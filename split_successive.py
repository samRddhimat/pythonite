def split_on_every_nth_occurrence(s, char, n, bincludeSep = True):
    """Split string `s` into two parts at the n-th occurrence of `char` using yield function."""
    index = -1
    prev_index = -1
    count = 0
    print(type(bincludeSep))
    for i, c in enumerate(s):
        if c == char:
            count += 1
            if count == n:
                if(prev_index != -1):
                    if bincludeSep == True:
                        yield(char.join(s[prev_index+1:i]))
                    else:
                        yield(s[prev_index+1:i])
                else:
                    yield(s[:i])
                index = i
                if index != prev_index:
                    prev_index = index
                    count = 0
                # break
    if count != 0:
        if(bincludeSep):
            yield(char.join(s[prev_index+1:i]))
        else:
            yield(s[prev_index+1:i])
    if index == -1:
        yield(s)  # Not enough occurrences

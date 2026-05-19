

lang = {"c " :20, "c++":20,"c#":18, "python":7}


print(lang.keys(),lang.values())

for sno,(k,v) in enumerate(sorted(lang.items(), key=lambda x:x[0][0], reverse=True), start=1):
    print(sno,k,v)

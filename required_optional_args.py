# -*- coding: utf-8 -*-
"""
Created on Tue Jun 29 15:00:46 2021

@author: Srinivasan
"""
"""
trying *args, **kwargs vs similar method
"""

class reqopt:
    
    def __init__(self,required_args, optargs1= None, optargs2= None):
        self.required_args = required_args
        self.optargs1 = optargs1
        self.optargs2 = optargs2
    
    
    
#print(reqopt(10,'fruits','vegetables',).__dict__)

class argskwargs:
    
    def __init__(self, *args, **kwargs):
        self.required_args = args[0]
        self.optargs1 = kwargs.get("optargs1",None)
        self.optargs2 = kwargs.get("optargs2",None)
#        for i in kwargs:
#            print(kwargs[i])

#        for i in kwargs:#.values():
#            print(kwargs)

#print(argskwargs(50).__dict__)

print(argskwargs(50,optargs1='fruits',optargs2='vegetables',optargs3='juices').__dict__)
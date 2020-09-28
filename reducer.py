#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys
import ast
from operator import lt,gt,eq,le,ge,ne
import pickle
import binascii

current_columns = None
minval = -sys.maxsize - 1
maxval = sys.maxsize 
current_max = minval
current_min = maxval
columns = None
"""
liss= [
    str({'key': ['Book'], 'value': '396585'}),
str({'key': ['Book'], 'value': '168596'}),
str({'key': ['Book'], 'value': '1270652'}),
str({'key': ['Book'], 'value': '631289'}),
str({'key': ['Book'], 'value': '455160'}),
str({'key': ['Book'], 'value': '188784'}),
str({'key': ['Book'], 'value': '277409'}),
str({'key': ['Music'], 'value': '5392'}),
str({'key': ['Book'], 'value': '2774099'})

]
fun = "MAX"
x=5
operation="gt"
"""

key2val={}

fun = sys.argv[1].upper()
x = int(sys.argv[2])
operation=sys.argv[3].lower()

if operation=="lt":
    sign = lt
elif operation=="gt":
    sign = gt
elif operation=="eq":
    sign = eq
elif operation == "lte":
    sign = le
elif operation=="gte":
    sign = ge
elif operation=="ne":
    sign = ne
else:
    print("Invalid comparison operator provided. Using \"greater than\" as default.")
    sign = gt

if fun=='MAX':

    for line in sys.stdin:
        line = line.strip()
        line=ast.literal_eval(line)

        columns,column1 = line['key'],line['value']


        try:
            column1 = int(column1)
        except ValueError:
            continue


        try:
            key2val[str(columns)]=max(key2val[str(columns)],column1)
        except:
            key2val[str(columns)]=column1

elif fun=='MIN':

    for line in sys.stdin:
        line = line.strip()
        line=ast.literal_eval(line)
    
        columns,column1 = line['key'],line['value']
        
        try:
            column1 = int(column1)
        except ValueError:
            continue

        try:
            key2val[str(columns)]=min(key2val[str(columns)],column1)
        except:
            key2val[str(columns)]=column1


elif fun=='SUM':
    
    for line in sys.stdin:
        line = line.strip()
        line=ast.literal_eval(line)
     
        columns,column1 = line['key'],line['value']
      
        try:
            column1 = int(column1)
        except ValueError:
            continue

        try:
            key2val[str(columns)]=key2val[str(columns)]+column1
        except:
            key2val[str(columns)]=column1

elif fun=='COUNT':

    for line in sys.stdin:
        line = line.strip()
        line=ast.literal_eval(line)
       
        columns,column1 = line['key'],line['value']
        
        try:
            column1 = int(column1)
        except ValueError:
            continue

        try:
            key2val[str(columns)]=key2val[str(columns)]+1
        except:
            key2val[str(columns)]=1


for col in key2val.keys():
    if sign(key2val[col],x):
       
        mykey=pickle.loads(binascii.unhexlify(col))
        print("  |  ".join(str(v) for v in mykey),"------",key2val[col])

    
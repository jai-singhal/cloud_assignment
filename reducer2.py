#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys
import ast
from operator import lt,gt,eq,le,ge,ne
import pickle
import binascii

current_columns = None
current_val= None
columns = None
sumval=0
## https://github.com/zonca/python-wordcount-hadoop/blob/master/reducer.py

key2val={}

fun = sys.argv[1].upper()
x = float(sys.argv[2])
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
    
  
    
for line in sys.stdin:
    line = line.strip().split("\t")
    columns,column1 = line[0],line[1]

    try:
        column1 = float(column1)
    except ValueError:
        continue
    if fun=='MAX':
      	if current_columns == columns:
            current_val= max(current_val, column1)
        else:
          	if current_columns:
                  if sign(current_val, x):
                    mykey=pickle.loads(binascii.unhexlify(current_columns))
                    print(f"{str(mykey)}\t{column1}")
                    current_val = column1
                    current_columns = columns
        

    elif fun=='MIN':
        if current_columns == columns:
            current_val= min(current_val, column1)
        else:
          	if current_columns:
                  if sign(current_val, x):
                    mykey=pickle.loads(binascii.unhexlify(current_columns))
                    print(f"{str(mykey)}\t{column1}")
                    current_val = column1
                    current_columns = columns

    elif fun=='SUM':

        if current_columns == columns:
            current_val= sumval+column1
        else:
          	if current_columns:
                  if sign(sumval, x):
                    mykey=pickle.loads(binascii.unhexlify(current_columns))
                    print(f"{str(mykey)}\t{column1}")
                    sumval = column1
                    current_columns = columns

    elif fun=='COUNT':
        if current_columns == columns:
            current_val= sumval+1
        else:
          	if current_columns:
                  if sign(sumval, x):
                    mykey=pickle.loads(binascii.unhexlify(current_columns))
                    print(f"{str(mykey)}\t{column1}")
                    sumval = column1
                    current_columns = columns



    
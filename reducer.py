#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys
import ast
from operator import lt,gt,eq,le,ge

current_columns = None
minval = -sys.maxsize - 1
maxval = sys.maxsize 
current_max = minval
current_min = maxval
columns = None
"""liss= [
    str({'key': ['Book'], 'value': '396585'}),
str({'key': ['Book'], 'value': '168596'}),
str({'key': ['Book'], 'value': '1270652'}),
str({'key': ['Book'], 'value': '631289'}),
str({'key': ['Book'], 'value': '455160'}),
str({'key': ['Book'], 'value': '188784'}),
str({'key': ['Book'], 'value': '277409'}),
str({'key': ['Music'], 'value': '5392'}),

]
"""

list1= []

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


for line in sys.stdin:
    line1 = line.strip()
    line1=ast.literal_eval(line)
    list1.append(line1)

list1= sorted(list1, key = lambda i: i['key'])





if fun=='MAX':

    for line in list1:

        columns,column1 = line['key'],line['value']


        try:
            column1 = int(column1)
        except ValueError:
            continue


        if not current_columns:
            current_columns=columns

        if current_columns == columns:
            current_max= max(current_max, column1)

        else:
            if sign(current_max, x):
                result = [current_columns, current_max]
                print("\t".join(str(v).strip(' []') for v in result))

            current_max = column1
            current_columns = columns

    # do not forget to output the last word if needed!
    if current_columns == columns:
        if sign(current_max, x):
                result = [current_columns, current_max]
                print("\t".join(str(v).strip(' []') for v in result))


elif fun=='MIN':

    for line in list1:
    
        columns,column1 = line['key'],line['value']
        
        try:
            column1 = int(column1)
        except ValueError:
            continue

        if not current_columns:
            current_columns=columns

        if current_columns == columns:
            current_min= min(current_min, column1)

        else:
            if sign(current_min, x):
                result = [current_columns, current_min]
                print("\t".join(str(v).strip(' []') for v in result))

            current_min = column1
            current_columns = columns

    # do not forget to output the last word if needed!
    if current_columns == columns:
        if sign(current_min, x):
                result = [current_columns, current_min]
                print("\t".join(str(v).strip(' []') for v in result))

elif fun=='SUM':
    sumval=0

    for line in list1:
     
        # print(line)
        columns,column1 = line['key'],line['value']
      
        try:
            column1 = int(column1)
        except ValueError:
            continue

        if not current_columns:
            current_columns=columns

        if current_columns == columns:
            sumval= sumval+column1

        else:
            if sign(sumval, x):
                result = [current_columns, sumval]
                print("\t".join(str(v).strip(' []') for v in result))

            sumval = column1
            current_columns = columns

    # do not forget to output the last word if needed!
    if current_columns == columns:
        if sign(sumval, x):
                result = [current_columns, sumval]
                print("\t".join(str(v).strip(' []') for v in result))

elif fun=='COUNT':
    sumval=0

    for line in list1:
       
        columns,column1 = line['key'],line['value']
        #columns, column1 = line.split('\t', 1)
        #column1= column1.strip(' {}')
        try:
            column1 = int(column1)
        except ValueError:
            continue

        if not current_columns:
            current_columns=columns

        if current_columns == columns:
            sumval= sumval +1

        else:
            if sign(sumval, x):
                result = [current_columns, sumval]
                print("\t".join(str(v).strip(' []') for v in result))

            sumval = 1
            current_columns = columns

    # do not forget to output the last word if needed!
    if current_columns == columns:
        if sign(sumval, x):
                result = [current_columns, sumval]
                print("\t".join(str(v).strip(' []') for v in result))

    
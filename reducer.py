#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys
import ast

current_columns = None
minval = -sys.maxsize - 1
maxval = sys.maxsize 
current_max = minval
current_min = maxval
columns = None
liss= [
    str({'key': ['Book'], 'value': '396585'}),
str({'key': ['Book'], 'value': '168596'}),
str({'key': ['Book'], 'value': '1270652'}),
str({'key': ['Book'], 'value': '631289'}),
str({'key': ['Book'], 'value': '455160'}),
str({'key': ['Book'], 'value': '188784'}),
str({'key': ['Book'], 'value': '277409'}),
str({'key': ['Music'], 'value': '5392'}),

]
fun = upper(sys.argv[1])
x = sys.argv[2]

if fun=='MAX':
# input comes from STDIN
    for line in liss:
    #for line in sys.stdin:

        # remove leading and trailing whitespace
        line = line.strip()

        line=ast.literal_eval(line)
        columns,column1 = line['key'],line['value']
        #columns, column1 = line.split('\t', 1)
        #column1= column1.strip(' {}')

        try:
            column1 = int(column1)
        except ValueError:
            continue

        # this IF-switch only works because Hadoop sorts map output
        # by key (here: word) before it is passed to the reducer
        if not current_columns:
            current_columns=columns

        if current_columns == columns:
            current_max= max(current_max, column1)

        else:
            if current_max > x:
                result = [current_columns, current_max]
                print("\t".join(str(v) for v in result))

            current_max = minval
            current_columns = columns

    # do not forget to output the last word if needed!
    if current_columns == columns:
        if current_max > x:
                result = [current_columns, current_max]
                print("\t".join(str(v) for v in result))


elif fun=='MIN':

    for line in sys.stdin:
        line = line.strip()
        line=ast.literal_eval(line)
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
            current_min= min(current_min, column1)

        else:
            if current_min > x:
                result = [current_columns, current_min]
                print("\t".join(str(v) for v in result))

            current_min = maxval
            current_columns = columns

    # do not forget to output the last word if needed!
    if current_columns == columns:
        if current_min > x:
                result = [current_columns, current_min]
                print("\t".join(str(v) for v in result))

elif fun=='SUM':
    sumval=0

    for line in sys.stdin:
        line = line.strip()
        line=ast.literal_eval(line)
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
            sumval= sumval+column1

        else:
            if sumval > x:
                result = [current_columns, sumval]
                print("\t".join(str(v) for v in result))

            sumval = 0
            current_columns = columns

    # do not forget to output the last word if needed!
    if current_columns == columns:
        if sumval > x:
                result = [current_columns, sumval]
                print("\t".join(str(v) for v in result))

elif fun=='COUNT':
    sumval=0

    for line in sys.stdin:
        line = line.strip()
        line=ast.literal_eval(line)
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
            if sumval > x:
                result = [current_columns, sumval]
                print("\t".join(str(v) for v in result))

            sumval = 0
            current_columns = columns

    # do not forget to output the last word if needed!
    if current_columns == columns:
        if sumval > x:
                result = [current_columns, sumval]
                print("\t".join(str(v) for v in result))

    
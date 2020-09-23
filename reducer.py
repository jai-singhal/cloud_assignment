#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys
import ast

current_columns = None
minval = -sys.maxsize - 1
minval = sys.maxsize 
current_max = minval
current_min = maxval
columns = None
fun = 'MAX' #to be changed later, pass as arg
x = 5 #to be changed later, pass as arg


if fun=='MAX':
# input comes from STDIN
    for line in sys.stdin:
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

        if current_columns == cols:
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

        if current_columns == cols:
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

        if current_columns == cols:
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

        if current_columns == cols:
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

    
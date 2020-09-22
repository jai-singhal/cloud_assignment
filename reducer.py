#!/usr/bin/env python
"""reducer.py"""

import os
import pickle
import re
import sys
from operator import itemgetter

current_group = None
current_group_count = 0
word = None

# sys args = [func, col1, op, x]

# input comes from STDIN
for line in sys.stdin:
    product = pickle.loads(line.strip(), encoding="UTF-8")
    # parse the input we got from mapper.py
    count = product["count"]

    # get col1 from sys.arg
    col1 = sys.argv[1] # list of cols which want to group
    group = []
    for col in col1:
        group.append(product["col"])
        
    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_group == group:
        current_group_count += count
    else:
        if current_group:
            product["count"] = current_group_count
            product_pickled = pickle.dumps(product)
            # write result to STDOUT
            print(product_pickled)
        current_group_count = count
        current_group = group

# # do not forget to output the last word if needed!
# if current_group == word:
#     print(product)

{"njas": sajh ...}\n
{"njas": sajh ...}\n
{"njas": sajh ...}\n


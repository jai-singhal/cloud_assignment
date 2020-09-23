#!/usr/bin/env python
"""mapper.py"""

import sys
import json
import pickle
import binascii

tables = ["similar", "categories", "reviews"]
# 80035d7100580500000067726f75707101612e
# input comes from STDIN (standard input)

for product in sys.stdin:
    # remove leading and trailing whitespace
    product = product.strip()
    product = json.loads(product)
    # increase counters
    columuns = pickle.loads(binascii.unhexlify(sys.argv[1].encode()))
    table = sys.argv[2]
    column1 = sys.argv[3]
    
    if table == "products":
        current_table = product
    elif table in tables:
        current_table = product[table]
    else:
        sys.exit(0)
        
    response = {"key": [], "value": 0}
    for columun in columuns:
        if columun not in current_table.keys():
            print("Column: ", columun, "not found")
            continue
        response["key"].append(product[columun])
    
    if column1 not in product.keys():
        sys.exit(0)
    response["value"] = product[column1]
    print(response)



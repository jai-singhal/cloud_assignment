#!/usr/bin/env python
"""mapper.py"""

import sys
import json
import pickle
import binascii

# 80035d7100580500000067726f75707101612e
# input comes from STDIN (standard input)

class Mapper():
    def __init__(self):
        self.SELECT_COLUMNS = pickle.loads(binascii.unhexlify(sys.argv[1].encode()))
        self.TABLE_NAME = sys.argv[2].strip()
        self.COLUMN1 = sys.argv[3].strip()
        self.WHERE_CONDITION = sys.argv[4].strip()
        self.WHERE_COLUMN = sys.argv[5].strip()
        self.Y = sys.argv[6].strip()
        self.TABLES_AVAILABLE = ["products", "similar", "categories", "reviews"]

    def get_table(self, product, table):
        current_table = None
        if table == "product":
            current_table = product
        if table not in product.keys():
            return None
        elif table in self.TABLES_AVAILABLE :
            current_table = product[table]
            # TODO
            if table == "reviews":
                current_table = current_table["review"]
        else:
            return None
        return current_table
    
    def where_cond_eval(self, obj):
        # print(obj , self.Y  )
        if self.WHERE_CONDITION == "gt":
            return obj > self.Y
        elif self.WHERE_CONDITION == "lt":
            return obj < self.Y
        elif self.WHERE_CONDITION == "eq":
            return obj == self.Y  
        
    def generate_keypair(self, row):
        if self.COLUMN1 not in row.keys():
            return None

        if not self.where_cond_eval(row[self.WHERE_COLUMN]):
            return None
        
        response = {"key": [], "value": 0}
        for columun in self.SELECT_COLUMNS:
            if columun not in row.keys():
                print("Column: ", columun, "not found")
                continue
            response["key"].append(row[columun])
        # if self.COLUMN1 not in row.keys():
        #     # sys.exit(0)
        response["value"] = row[self.COLUMN1]
        return response       

    def run(self):        
        for product in sys.stdin:
            product = product.strip()
            product = json.loads(product)
            # print(product)
            current_table = self.get_table(product, self.TABLE_NAME)
            if not current_table:
                continue
            if self.TABLE_NAME ==  "products":
                res = self.generate_keypair(current_table)
                if res:
                    print(res)
            else:
                for row in current_table:
                    res = self.generate_keypair(row)
                    if res:
                        print(res)
                    

if __name__ == "__main__":
    m = Mapper()
    m.run()
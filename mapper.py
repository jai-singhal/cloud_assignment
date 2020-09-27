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
        # assuming always to be product
        self.TABLE_NAME = sys.argv[2].strip()
        self.COLUMN1 = sys.argv[3].strip()
        self.WHERE_CONDITION = sys.argv[4].strip()
        self.WHERE_COLUMN = sys.argv[5].strip()
        self.Y = sys.argv[6].strip()
        self.TABLES_AVAILABLE = ["products", "similar", "categories", "reviews"]

    def where_cond_eval(self, obj):
        # print(obj , self.Y  )
        if self.WHERE_CONDITION == "gt":
            return obj > self.Y
        elif self.WHERE_CONDITION == "lt":
            return obj < self.Y
        elif self.WHERE_CONDITION == "eq":
            return obj == self.Y  
        elif self.WHERE_CONDITION == "gte":
            return obj >= self.Y  
        elif self.WHERE_CONDITION == "lte":
            return obj <= self.Y  
        elif self.WHERE_CONDITION == "neq":
            return obj != self.Y
               
    def generate_keypair(self, row):
        if self.COLUMN1 not in row.keys():
            return None

        response = {"key": [], "value": 0}

        for columun in self.SELECT_COLUMNS:
            newKey = None
            if columun not in row.keys():
                
                if len(row["reviews"]) > 0 and columun in row["reviews"][0].keys():
                    for review in row["reviews"]:
                        if self.WHERE_COLUMN in review.keys():
                            if self.where_cond_eval(review[self.WHERE_COLUMN]):
                                newKey = review[self.WHERE_COLUMN]
                                break
                        
                elif columun == "similar_asin":
                    for asin in row["similar"]:
                        if self.where_cond_eval(asin):
                            newKey = asin
                            break
                        
                elif columun == "category":
                    found = False
                    for categories in row["categories"]:
                        if found:   break
                        for category in categories:
                            catname_id = f"{category['category_name']}[{category['category_id']}]"
                            if self.where_cond_eval(catname_id):
                                newKey = catname_id
                                found = True
                                break
                            
                elif columun == "category_name":
                    found = False
                    for categories in row["categories"]:
                        if found:   break
                        for category in categories:
                            if self.where_cond_eval(category["category_name"]):
                                newKey = category["category_name"]
                                found = True
                                break
                else:  
                    continue
                
            elif columun in row.keys():
                if self.WHERE_COLUMN == columun and not self.where_cond_eval(row[columun]):
                    return None
                newKey = row[columun]
            else:
                return None

            if newKey:
                response["key"].append(newKey)
                
        if self.COLUMN1 in row.keys():
            response["value"] = row[self.COLUMN1]
        else:
            response["value"] = 1
        
        return response       

    def run(self):        
        for product in sys.stdin:
            product = product.strip()
            product = json.loads(product)
            res = self.generate_keypair(product)
            if res:
                print(res)

if __name__ == "__main__":
    m = Mapper()
    m.run()
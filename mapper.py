#!/usr/bin/env python3
"""mapper.py"""
from __future__ import print_function
import sys
import json
import pickle
import binascii

MV_TABLES= ["similar", "categories", "reviews"]

class Mapper():
    def __init__(self, s1, s2, s3, s4, s5, s6, s7):
        self.SELECT_COLUMNS = pickle.loads(binascii.unhexlify(s1.encode()))
        self.TABLE_NAME = s2
        self.COLUMN1 = s3
        self.WHERE_CONDITION = s4
        self.WHERE_COLUMN = s5
        self.Y = s6
        self.FUNC = s7

    def where_cond_eval(self, obj):
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
    
    def get_review_key(self, row, columun):
        newKey = None
        for review in row["reviews"]:
            if self.WHERE_COLUMN in review.keys():
                if self.where_cond_eval(review[self.WHERE_COLUMN]):
                    newKey = review[self.WHERE_COLUMN]
                    break 
        return newKey
    
    def get_category_key(self, row, columun):
        newKey = None
        found = False
        for categories in row["categories"]:
            if found:   break
            for category in categories:
                if columun == "category":
                    try:
                        catname_id = "{0}[{1}]".format(
                            category['category_name'], category['category_id']
                        )
                    except Exception as e:
                        continue
                elif columun == "category_id":
                    catname_id = category['category_id']
                elif columun == "category_name":
                    catname_id = category['category_name']
                    
                if self.where_cond_eval(catname_id):
                    newKey = catname_id
                    found = True
                    break
        return newKey
    
    def get_similar_key(self, row):
        newKey = None
        for asin in row["similar"]:
            if self.where_cond_eval(asin):
                newKey = asin
                break
        return newKey
    
    def generate_key_val_pair(self, row):
        response = {"key": [], "value": 0}
        
        if self.COLUMN1 not in row.keys():
            return None

        for columun in self.SELECT_COLUMNS:
            newKey = None
            if columun not in row.keys():
                if len(row["reviews"]) > 0 and columun in row["reviews"][0].keys():
                    newKey = self.get_review_key(row, columun)
                elif columun == "similar_asin":
                    newKey = self.get_similar_key(row)
                elif columun == "category":
                    newKey = self.get_category_key(row, columun)
                elif columun == "category_name":
                    newKey = self.get_category_key(row, columun)
                else:  continue
                
            elif columun in row.keys():
                if self.WHERE_COLUMN == columun and not self.where_cond_eval(row[columun]): pass
                else:   newKey = row[columun]
            else:   return None
            if newKey is not None:  response["key"].append(newKey)
            else:   return None
            
        if self.COLUMN1 in row.keys():
            if self.FUNC.lower() == "count":
                response["value"] = 1
            else:
                response["value"] = row[self.COLUMN1]
        else:
            response["value"] = 1
        return response       
    
    
    def print_keypair_for_mv(self, rows):
        for row in rows:
            if self.COLUMN1 not in row.keys():
                continue
            if not self.where_cond_eval(row[self.WHERE_COLUMN]):
                continue
            
            response = {"key": [], "value": 0}
            for columun in self.SELECT_COLUMNS:
                if columun not in row.keys():
                    continue
                response["key"].append(row[columun])
                
            key = binascii.hexlify(pickle.dumps(response["key"], protocol=2)).decode()
            response["value"] = row[self.COLUMN1]
            # TODO: should not return from here, doing for spark
            return (key, response['value'])
            
    
    def run(self): 
        for product in sys.stdin:
            product = product.strip("\n")
            try:
                product = json.loads(product)
            except Exception as e:
                continue
            if self.TABLE_NAME == "products":
                res = self.generate_key_val_pair(product)
                if res:
                    key = binascii.hexlify(pickle.dumps(res["key"], protocol=2)).decode()
                    print("%s\t%s" %(key, res['value']))
            elif self.TABLE_NAME in MV_TABLES and self.TABLE_NAME in product.keys():
                row = product[self.TABLE_NAME]
                res = self.print_keypair_for_mv(row)
                if res: print("%s\t%s" %(res[0], res[1]))
            else:
                continue

                
    def run_spark(self, product): 
        try:
            product = product.strip()
            product = json.loads(product)
        except Exception as e:
            return (binascii.hexlify(pickle.dumps([], protocol=2)).decode(), 0)

        if self.TABLE_NAME == "products":
            res = self.generate_key_val_pair(product)
            if res:
                key = binascii.hexlify(pickle.dumps(res["key"], protocol=2)).decode()
                return (key, float(res["value"]))
            
        elif self.TABLE_NAME in MV_TABLES and self.TABLE_NAME in product.keys():
            row = product[self.TABLE_NAME]
            res = self.print_keypair_for_mv(row)
            if res: return (res[0], float(res[1]))

        return (binascii.hexlify(pickle.dumps([], protocol=2)).decode(), 0)

if __name__ == "__main__":
    SELECT_COLUMNS = sys.argv[1].strip()
    TABLE_NAME = sys.argv[2].strip()
    COLUMN1 = sys.argv[3].strip()
    WHERE_CONDITION = sys.argv[4].strip()
    WHERE_COLUMN = sys.argv[5].strip()
    Y = sys.argv[6].strip()
    func = sys.argv[7].strip()
    
    m = Mapper(
        SELECT_COLUMNS,
        TABLE_NAME,
        COLUMN1,
        WHERE_CONDITION,
        WHERE_COLUMN,
        Y,
        func
    )
    m.run()
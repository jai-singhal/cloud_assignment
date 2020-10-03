#!/usr/bin/env python3
"""mapper.py"""
from __future__ import print_function

import binascii
import json
import pickle
import sys
import itertools
from functools import reduce

MV_TABLES= ["reviews", ]
CATEGORIES_COLS = ["category", "category_name", "category_id"]

class Mapper():
    def __init__(self, s1, s2, s3, s4, s5, s6, s7):
        self.SELECT_COLUMNS = pickle.loads(binascii.unhexlify(s1.encode()))
        self.TABLE_NAME = s2
        self.COLUMN1 = s3
        self.WHERE_CONDITION = s4
        self.WHERE_COLUMN = s5
        try:
            self.Y = float(s6)
        except:
            self.Y = s6
        self.FUNC = s7

    def where_cond_eval(self, obj):
        try:
            obj = float(obj)
        except:
            pass
        if isinstance(self.Y, str) or isinstance(obj, str):
            self.Y = str(self.Y)
            obj = str(obj) 
        
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
        keys = []
        categories = self.get_unique_categories(row["categories"], columun)
        for category in categories:
            if self.WHERE_COLUMN in CATEGORIES_COLS:
                if self.where_cond_eval(category):
                    keys.append(category)
            else:
                keys.append(category)
        return keys
    
    def get_similar_key(self, row):
        keys = []
        for asin in row["similar"]:
            if self.WHERE_COLUMN == "similar_asin":
                if self.where_cond_eval(asin):
                    keys.append(asin)
            else:
                keys.append(asin)
        return keys
    
    def cartesian_product(self, t1, t2, t_names):
        keys = []
        for i in itertools.product(t1, t2):
            if self.WHERE_COLUMN == t_names[0]:
                if self.where_cond_eval(i[0]):
                    continue
            
            if self.WHERE_COLUMN == t_names[1]:
                if self.where_cond_eval(i[1]):
                    continue
            keys.append(list(i))
        return keys
    
    @staticmethod
    def get_unique_categories(categories, type_):
        unique_categories = set()
        for row in categories:
            for category in row:
                if type_ == "category":
                    unique_categories.add(
                        "{0}[{1}]".format(
                            category["category_name"],
                            category["category_id"],
                        )
                    )
                elif type_=="category_name":
                    unique_categories.add(
                        category["category_name"],
                    )
                elif type_=="category_id":
                    unique_categories.add(
                        category["category_id"],
                )
                    
        return list(unique_categories)
    
    def check_where(self, objs):
        for obj in objs:
            if self.where_cond_eval(obj):
                return True
        return False
    
    def generate_key_val_pair(self, row):
        if self.WHERE_COLUMN in row.keys():
            if not self.where_cond_eval(row[self.WHERE_COLUMN]):
                return []
        if "similar" in row.keys() and self.WHERE_COLUMN  == "similar_asin":
            if not self.check_where(row["similar"]):
                return []
        
        if "categories" in row.keys() and self.WHERE_COLUMN in CATEGORIES_COLS:
            objs = self.get_unique_categories(row["categories"], self.WHERE_COLUMN)
            if not self.check_where(objs):
                return []
        total_keys = []
        if "similar_asin" in self.SELECT_COLUMNS and \
         "category" in self.SELECT_COLUMNS:
            if "similar" in row.keys() and "categories" in row.keys():
                total_keys.append(self.cartesian_product(
                    row["similar"], 
                    self.get_unique_categories(row["categories"]),
                    ["similar_asin", "category"]
                ))
                self.SELECT_COLUMNS.remove("similar_asin")
                self.SELECT_COLUMNS.remove("category")
        

        for columun in self.SELECT_COLUMNS:
            newKey = None
            if columun not in row.keys():
                if "reviews" in row.keys() and len(row["reviews"]) > 0 and columun in row["reviews"][0].keys():
                    newKey = self.get_review_key(row, columun)
                elif "similar" in row.keys() and columun == "similar_asin":
                    newKey = self.get_similar_key(row)
                elif "categories" in row.keys() and columun == "category":
                    newKey = self.get_category_key(row, columun)
                elif "categories" in row.keys() and columun == "category_name":
                    newKey = self.get_category_key(row, columun)
                elif "categories" in row.keys() and columun == "category_id":
                    newKey = self.get_category_key(row, columun)
                    
                else:  continue
                
            elif columun in row.keys():
                if self.WHERE_COLUMN == columun and not self.where_cond_eval(row[columun]): pass
                else:   newKey = [row[columun],]
            else:   return []
            
            if len(newKey): 
                total_keys.append(newKey)
        
        row_response = []
        for key in itertools.product(*tuple(total_keys)):
            res = {"key": [], "value": 0}
            if not key: continue
            if isinstance(key, tuple):
                for sublist in key:
                    if isinstance(sublist, list):
                        res["key"] += sublist
                    else:
                        res["key"].append(sublist)
            else:
                res["key"] = [key,]

            if self.COLUMN1 in row.keys():
                if self.FUNC.lower() == "count":
                    res["value"] = 1
                else:
                    res["value"] = row[self.COLUMN1]
            else:
                res["value"] = 1
            if res["key"]:
                row_response.append(res)
        # print(row_response)
        return row_response       
    
    
    def print_keypair_for_mv(self, rows):
        to_return = []
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
            if self.COLUMN1 in row.keys():
                if self.FUNC.lower() == "count":
                    response["value"] = 1
                else:
                    response["value"] = row[self.COLUMN1]
            else:
                response["value"] = 1
            # TODO: should not return from here, doing for spark
            to_return.append((key, float(response['value'])))
        return to_return  
    
    def run(self): 
        for product in sys.stdin:
            product = product.strip("\n")
            try:
                product = json.loads(product)
            except Exception as e:
                continue
            if self.TABLE_NAME == "products":
                results = self.generate_key_val_pair(product)
                for res in results:
                    if res:
                        # print(res)
                        key = binascii.hexlify(pickle.dumps(res["key"], protocol=2)).decode()
                        print("%s\t%s" %(key, res['value']))
                        
            elif self.TABLE_NAME in MV_TABLES and self.TABLE_NAME in product.keys():
                row = product[self.TABLE_NAME]
                for res in self.print_keypair_for_mv(row):
                    print("%s\t%s" %(res[0], str(res[1])))
            else:
                continue

                
    def run_spark(self, product): 
        try:
            product = product.strip()
            product = json.loads(product)
        except Exception as e:
            return (binascii.hexlify(pickle.dumps([], protocol=2)).decode(), 0)

        if self.TABLE_NAME == "products":
            results = self.generate_key_val_pair(product)
            to_return  = []
            for res in results:
                if res:
                    key = binascii.hexlify(pickle.dumps(res["key"], protocol=2)).decode()
                    to_return.append((key, float(res["value"])))
            return to_return
            
        elif self.TABLE_NAME in MV_TABLES and self.TABLE_NAME in product.keys():
            row = product[self.TABLE_NAME]
            res = self.print_keypair_for_mv(row)
            if len(res) > 0: return res

        return (binascii.hexlify(pickle.dumps([], protocol=2)).decode(), 0)

if __name__ == "__main__":
    m = Mapper(*pickle.loads(binascii.unhexlify(sys.argv[1].encode())))
    m.run()


# mapper.py 80025d710028582c00000038303032356437313030353830383030303030303633363137343635363736663732373937313031363132657101580800000070726f64756374737102580900000073616c657372616e6b7103580200000065717104580c00000073696d696c61725f6173696e7105580a0000003036353830323135353971065805000000636f756e747107652e 
# reducer_test.py 80025d7100285805000000636f756e7471014b0158030000006774657102652e
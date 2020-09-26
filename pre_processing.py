import re
import json

single_value_keys = ["Id", "ASIN", "title", "group", "salesrank"]
tables = ["similar", "categories", "reviews"]
RATING_REGEX = r"(\d+\-\d+\-\d+)[\s]*cutomer:[\s]*([0-9A-Z]+)[\s]*rating:[\s]*(\d+)[\s]*votes:[\s]*(\d+)[\s]*helpful:\W+(\d+)"

def getSingletonKeyPair(line):
    keyval_pair = line.split(":", 2)
    key = keyval_pair[0].strip("\n \t")
    val = " ".join(keyval_pair[1:]).strip("\n \t")
    return key, val

new_line_count = 0
with open("data/amazon-meta.txt", "r", encoding="utf-8") as fin:
    with open("data/amazon-meta-processed.txt", "w", encoding= "utf-8") as fout:
        product = {}
        while True:
            line = fin.readline()
            if new_line_count > 2:
                break
            
            if not line.strip():
                new_line_count += 1  
                 
            if line == "\n": 
                if product:
                    new_line_count = 0
                    result = json.dumps(product) 
                    fout.write(result)
                    fout.write("\n")
                    product = {}
            else:
                if re.match(r".*:.*", line):
                    key, val = getSingletonKeyPair(line)
                    if key in single_value_keys:
                        product[key.lower()] = val
                    if key == "similar":
                        product[key] = [
                            {"asin": val.strip() } 
                                for val in val.split(" ")[1:] if len(val) > 0
                            ]
                    elif key == "categories":
                        product[key] = []
                        for i in range(int(val)):
                            categories = re.findall(r"\|(\w*)\[(\d+)\]", fin.readline().strip())
                            index = 1
                            cat_arr = []
                            for type_, cat_id in categories:
                                cat_arr.append({
                                    "index": index,
                                    "category_name": type_,
                                    "category_id": cat_id
                                })
                                index += 1
                            product[key].append(cat_arr)  
                         
                    elif key == "reviews":
                        # reviews: total: 2  downloaded: 2  avg rating: 5
                        total, downloaded, avg_rating = re.findall(r"\d*\.\d+|\d+", val.strip())
                        product["reviews"] = {
                            "total": total,
                            "downloaded": downloaded,
                            "avg_rating": avg_rating,
                            "review": []
                        }
                        for rev in range(int(downloaded)):
                            review_tup = re.findall(RATING_REGEX, fin.readline().strip())
                            if not review_tup: continue
                            review_tup = review_tup[0]
                            product["reviews"]["review"].append({
                                "date": review_tup[0],
                                "user_id": review_tup[1],
                                "rating": review_tup[2],
                                "votes": review_tup[3],
                                "helpful": review_tup[4],
                            })
import json
import os
import pickle
import re

from fastapi import FastAPI
from moz_sql_parser import parse
from pydantic import BaseModel
from pyspark import SparkConf, SparkContext
from mapper import Mapper
from utils import *
import time
"""
SELECT <COLUMNS>, FUNC(COLUMN1)
FROM <TABLE>
WHERE <COLUMN1> = Y
GROUP BY <COLUMNS>
HAVING FUNC(COLUMN1)>X
--Here FUNC can be COUNT, MAX, MIN, SUM
""" 

app = FastAPI()
sc = SparkContext("local","PySpark sql run")
# sc.setLogLevel("INFO")
data = sc.textFile("data/amazon-meta-processed.txt")

class Query(BaseModel):
    q: str

def run_spark(parsed):
    func, Y, operation = get_reducer_args(parsed)
    m = Mapper(*get_mapper_args(parsed))
    
    sparkmapp = data.map(
        lambda line: m.run_spark(line)
    ).reduceByKey(
        lambda a, b: reducer_operation(func, a, b)
    ).filter(
        lambda row: having_cond_eval(operation, row, Y)
    )
    results = sparkmapp.collect()
    to_return = []
    # sc.stop()
    for result in results:
        key = pickle.loads(binascii.unhexlify(result[0].encode()))
        if len(key) == 0:   continue
        to_return.append([
            str(key),
            result[1]
        ])
    return {
        "out": to_return,
    }

def run_map_reduce(parsed):
    mapper_output = run_mapper_process(parsed)
    mapper_output_decoded = mapper_output.decode()
    
    with open("reducer_inp.txt", "w") as reducer_in:
        for line in mapper_output_decoded.split("\n"):
            if line:
                reducer_in.write(line)
                reducer_in.write("\n")
          
    # reducer
    reducer_output = run_reducer_process(parsed)
    reducer_output_decoded = reducer_output.decode()
    return reducer_output_decoded

@app.post("/run")
async def main(query: Query):
    parsed = parse(query.q)
    tick = time.time()
    map_red_out = run_map_reduce(parsed)
    tock = time.time()
    mapred_time = tock-tick
    spark_out = run_spark(parsed)
    tick = time.time()
    spark_time = tick-tock
    
    return {
        "map_red": map_red_out,
        "mapred_time": mapred_time,
        "spark_out": spark_out,
        "spark_time": spark_time
    }



# def get_cmd():
#     MAP_REDUCE_CMD = """
#         hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar 
#         -file mapper.py 
#         -mapper /home/cloudera/Downloads/tmp/mapper.py 
#         -file reducer.py 
#         -reducer /home/cloudera/Downloads/tmp/reducer.py 
#         -input data/amazon-meta-processed.txt
#         -output /user/hduser/gutenberg-output/r
#     """
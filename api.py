#!/usr/bin/env python3
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
from datetime import datetime
# http://www.java2s.com/Code/Jar/h/Downloadhadoopstreamingjar.htm

INPUT_FILE_NAME = "data/test.txt"

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
data = sc.textFile(INPUT_FILE_NAME)

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
    return to_return

def run_map_reduce(parsed):
    dt_string = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    mapper_arggs_str = " ".join(str(arg) for arg in get_mapper_args(parsed))
    reducer_arggs_str = " ".join(str(arg) for arg in get_reducer_args(parsed))
    
    MAP_REDUCE_CMD = get_hadoop_steam_cmd(
        mapper_arggs_str, reducer_arggs_str,
        INPUT_FILE_NAME, dt_string
    )
    print(" ".join(MAP_REDUCE_CMD))
    
    pr = Popen(MAP_REDUCE_CMD, stdin=PIPE, stdout=PIPE)
    output, err = pr.communicate()
    
    resultset = []
    with open("output/{0}/part-00000".format(dt_string), "r") as fin:
        for line in fin.readlines():
            resultset.append(line.strip("\n"))
    return resultset

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

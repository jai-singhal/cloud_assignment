#!/usr/bin/env python3
import json
import os
import pickle
import re
import time
from datetime import datetime

from fastapi import FastAPI
from moz_sql_parser import parse
from pydantic import BaseModel
from pyspark import SparkConf, SparkContext

from mapper import Mapper
from utils import *

# http://www.java2s.com/Code/Jar/h/Downloadhadoopstreamingjar.htm

INPUT_FILE_NAME = "data/amazon-meta-processed.txt"
# INPUT_FILE_NAME = "data/amazon-meta-processed.txt"

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


def run_spark_process(parsed):
    func, Y, operation = get_reducer_args(parsed)
    m = Mapper(*get_mapper_args(parsed))
    
    sparkmapp = data.map(
        lambda line: m.run_spark(line)
    ).flatMap(lambda line: split_spark_mapper_results(line)).reduceByKey(
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
        key_str = ""
        for k in key:
            key_str = key_str + str(k) + "  |  " 
            out = result[1]
            try:
                out = int(out)
            except Exception as e:
                pass
        to_return.append("{0}   {1}".format(str(key_str),out))
    return to_return

def mapper_inp():
    to_ret = []
    with open("data/test.txt", "r") as fin:
        for i in range(5):
            line = fin.readline().strip("\n \t")
            to_ret.append(line)
    return to_ret

def run_mapper_process(parsed):
    mapper_cmd = binascii.hexlify(pickle.dumps(get_mapper_args(parsed), protocol = 2)).decode()
    
    map_process_temp = Popen(["cat", "data/test.txt"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    map_pipe_process = Popen(["python3", "mapper.py", mapper_cmd], stdin=map_process_temp.stdout, stdout=PIPE, stderr=PIPE)

    mapper_output, err = map_pipe_process.communicate()
    map_process_temp.stdout.close() 
    list1 = mapper_output.decode().split('\n')
    list2=[]
    for line in list1:
        line=line.split('\t')
        try:
            line[0]=pickle.loads(binascii.unhexlify(line[0].encode()))
        except EOFError:
            pass
        line="".join(str(line))
        list2.append(line)

    return list2

def run_map_reduce(parsed):
    dt_string = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    
    mapper_arggs_str = binascii.hexlify(pickle.dumps(get_mapper_args(parsed), protocol = 2)).decode()
    reducer_arggs_str = binascii.hexlify(pickle.dumps(get_reducer_args(parsed), protocol = 2)).decode()
    #print(reducer_arggs_str)
    MAP_REDUCE_CMD = get_hadoop_steam_cmd(
        mapper_arggs_str, reducer_arggs_str,
        INPUT_FILE_NAME, dt_string
    )
    print(" ".join(MAP_REDUCE_CMD))
    
    pr = Popen(MAP_REDUCE_CMD, stdin=PIPE, stdout=PIPE)
    output, err = pr.communicate()
    
    resultset = []
    fileno = 0
    while True:
        filepath = "output/{0}/part-{1:05d}".format(dt_string, fileno)
        if not os.path.exists(filepath):    break     
        with open(filepath, "r") as fin:
            for line in fin.readlines():
                resultset.append(line.strip("\n"))
        fileno += 1

    return resultset


@app.post("/run")
async def main(query: Query):
    try:
        parsed = parse(query.q)
    except Exception as e:
        return {
            "error": "Query is not appropriate",
            "e": str(e)
        }
    if parsed["from"] not in ["reviews", "products"]:
        return {
            "error": "Table can only be reviews, products",
        }
    # to return dummy out
    map_output=run_mapper_process(parsed)

    try:
        tick = time.time()
        map_red_out = run_map_reduce(parsed)
        tock = time.time()
        mapred_time = tock-tick
    except Exception as e:
        return {
            "error": "Error in executing map reduce Job",
            "e": str(e)
        }
        
    try:
        spark_out = run_spark_process(parsed)
        tick = time.time()
        spark_time = tick-tock  
    except Exception as e:
        return {
            "error": "Error in executing spark Job",
            "e": str(e)
        }
            
    return {
        "query_output": {
            "map_reduce_job": map_red_out,
            "spark_job": spark_out,
        },
        "map_reduce_job_time": "{:.3f} sec".format(mapred_time),
        "spark_job_time": "{:.3f} sec".format(spark_time),
        "map_reduce_job_format":{
            "mapper":{
                "input": mapper_inp()[1:2],
                "output": map_output[0:2]
            },
            "reducer":{
                "input": map_output[0:2],
                "output": map_red_out[0:2]
            }
        },
        "spark_job_format":{
            "transformations":{
                "map()",
                "flatMap()",
                "reduceByKey()"
            },
            "actions":{
                "filter()",
                "collect()"
            }
        }, 
    }

import json
import os
import pickle
import re

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from moz_sql_parser import parse
from pydantic import BaseModel
from pyspark import SparkConf, SparkContext
from mapper import Mapper
from utils import *

"""
SELECT <COLUMNS>, FUNC(COLUMN1)
FROM <TABLE>
WHERE <COLUMN1> = Y
GROUP BY <COLUMNS>
HAVING FUNC(COLUMN1)>X
--Here FUNC can be COUNT, MAX, MIN, SUM
""" 

app = FastAPI()
templates = Jinja2Templates(directory="templates")

class Query(BaseModel):
    q: str

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/run/sql/spark")
async def spark_post(query: Query):
    parsed = parse(query.q)
    print(parsed)
    
    sc = SparkContext("local","PySpark sql run")
    data = sc.textFile("data/test.txt")
    m = Mapper(*get_mapper_args(parsed))
    
    func, op, Y = get_reducer_args(parsed)
    
    sparkmapp = data.map(
        lambda line: m.run_spark(line)
    ).reduceByKey(
        lambda a, b: reducer_operation(func, a, b)
    ).filter(
        lambda key, val: having_cond_eval(op, val, Y)
    )
    
    res = sparkmapp.collect()
    return {
        "out": res,
    }


@app.post("/run/sql/map_reduce")
async def map_reduce_post(query: Query):
    def get_cmd():
        MAP_REDUCE_CMD = """
            hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar 
            -file mapper.py 
            -mapper /home/cloudera/Downloads/tmp/mapper.py 
            -file reducer.py 
            -reducer /home/cloudera/Downloads/tmp/reducer.py 
            -input data/amazon-meta-processed.txt
            -output /user/hduser/gutenberg-output/r
        """
    
    parsed = parse(query.q)
    print(parsed)
    ## mapper
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
    return {
        "mapper_output": mapper_output_decoded,
        "reducer_output": reducer_output_decoded
    }

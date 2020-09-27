import json
import os
import pickle
import re

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from subprocess import Popen, PIPE
from fastapi.responses import HTMLResponse
from utils import *
from moz_sql_parser import parse

app = FastAPI()
templates = Jinja2Templates(directory="templates")
# uvicorn main:app --reload
query = """
SELECT group, sum(salesrank)
FROM products
GROUP BY group
HAVING sum(salesrank)>10
""" 
# {"select": [{"value": "abc"}, {"value": "def"}, {"value": {"sum": "xyx"}}], "from": "reviews", 
#  "groupby": [{"value": "abc"}, {"value": "def"}], "having": {"gt": [{"sum": "xyx"}, 10]}}
'''
 select user_id, COUNT(votes) from reviews WHERE user_id == 'A2CXT3A901DGMP'  group by user_id HAVING COUNT(votes) > 2;
'''
   

class Query(BaseModel):
    q: str

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/run/sql/spark")
async def spark_post(query: Query):
    parsed = parse(query.q)
    print(parsed)
    mapper_cmd  = get_mapper_args(parsed)


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
    mapper_cmd = ["python3", "mapper.py"]
    for arg in get_mapper_args(parsed): mapper_cmd.append(arg)
    
    map_process_temp = Popen(["cat", "data/test.txt"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    print(map_process_temp.stdout)
    map_pipe_process = Popen(mapper_cmd, stdin=map_process_temp.stdout, stdout=PIPE, stderr=PIPE)

    mapper_output, err = map_pipe_process.communicate()
    map_process_temp.stdout.close() 
    
    rc_m = map_pipe_process.returncode
    print("Error in mapper: ", err.decode(), 'return code: ', rc_m)
    
    mapper_output_decoded = mapper_output.decode()
    print("mapper out: ", mapper_output_decoded)
    
    reducer_in = open("reducer_inp.txt", "w")
    for line in mapper_output_decoded.split("\n"):
        if line:
            reducer_in.write(line)
            reducer_in.write("\n")
    
    reducer_in.close()
          
    # reducer
    reducer_cmd = get_reducer_test_cmd(parsed, mapper_output)
    reducer_temp_process = Popen(['cat', 'reducer_inp.txt'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    reducer_pipe_process = Popen(reducer_cmd.split(" "), stdin=reducer_temp_process.stdout, stdout=PIPE)
    reducer_output, err = reducer_pipe_process.communicate()
    reducer_temp_process.stdout.close() 
    
    print("Error in reducer: ", err)
    print("reducer out: ", reducer_output.decode())
    rc_r = reducer_pipe_process.returncode
    
    return {
        "mapper_output": mapper_output_decoded,
        "reducer_output": reducer_output.decode()
    }

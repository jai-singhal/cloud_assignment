import binascii
import json
import os
import pickle
import re

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from subprocess import Popen, PIPE
from fastapi.responses import HTMLResponse

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


@app.post("/run/sql")
async def main(query: Query):
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
    def get_select_cols(select_cols):
        select_cols_parsed = []
        aggregate_fns = {}
        for s_col in select_cols:
            for k, v in s_col.items():
                if isinstance(v, dict):
                    for v1, v2 in v.items():
                        aggregate_fns = {
                            "fun": v1,
                            "col": v2,
                        }
                else:
                    select_cols_parsed.append(v)
        return (select_cols_parsed, aggregate_fns)        
    
    def get_where_cond(parsed):
        (cond, colval), = parsed.items()
        col1 = colval[0]
        val = colval[1]
        if isinstance(val, dict):
            if "literal" in val.keys():
                val = val["literal"]
        return [cond, col1, val]
    
    def get_mapper_test_cmd(parsed):
        select_cols, agg_fn = get_select_cols(parsed["select"])
        select_cols_p = binascii.hexlify(pickle.dumps(select_cols)).decode()
        where_cl = get_where_cond(parsed["where"])

        return (
        f"cat data/test.txt", 
        f"""python3 mapper.py {select_cols_p} {parsed["from"]} {agg_fn["col"]} {where_cl[0]} {where_cl[1]} {where_cl[2]}"""
        )
        
    def get_reducer_test_cmd(parsed, map_out):
        
        select_cols, agg_fn = get_select_cols(parsed["select"])
        (op, op_val), = parsed["having"].items()
        X = op_val[1]
        to_return = f"""python3 reducer.py {agg_fn["fun"]} {X} {op}"""
        return to_return
    
    parsed = parse(query.q)
    print(parsed)
    ## mapper
    mapper_cmd1, mapper_cmd2  = get_mapper_test_cmd(parsed)
    map_process_temp = Popen(mapper_cmd1.split(" "), stdin=PIPE, stdout=PIPE, stderr=PIPE)
    map_pipe_process = Popen(mapper_cmd2.split(" "), stdin=map_process_temp.stdout, stdout=PIPE, stderr=PIPE)

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



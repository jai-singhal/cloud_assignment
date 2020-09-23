import binascii
import json
import os
import pickle
import re

from fastapi import FastAPI
from pydantic import BaseModel
from subprocess import Popen, PIPE

from moz_sql_parser import parse

app = FastAPI()

# uvicorn main:app --reload
query = """
SELECT group, sum(salesrank)
FROM products
GROUP BY group
HAVING sum(salesrank)>10
""" 
# {"select": [{"value": "abc"}, {"value": "def"}, {"value": {"sum": "xyx"}}], "from": "reviews", 
#  "groupby": [{"value": "abc"}, {"value": "def"}], "having": {"gt": [{"sum": "xyx"}, 10]}}


class Query(BaseModel):
    q: str

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
        print(select_cols)
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
        
    def get_mapper_test_cmd(parsed):
        print(parsed["select"])
        select_cols, agg_fn = get_select_cols(parsed["select"])
        select_cols_pickle = binascii.hexlify(pickle.dumps(select_cols)).decode()
        return f"cat data/test.txt", f"""python3 mapper.py {select_cols_pickle} {parsed["from"]} {agg_fn["col"]}"""
        
    def get_reducer_test_cmd(parsed, map_out):
        select_cols, agg_fn = get_select_cols(parsed["select"])
        X = parsed["having"]["gt"][1]
        return f"""echo {map_out}""",  f"""python3 reducer.py {agg_fn["fun"]} {X}"""
        
    parsed = parse(query.q)
    mapper_cmd1, mapper_cmd2  = get_mapper_test_cmd(parsed)
    map_process_temp = Popen(mapper_cmd1.split(" "), stdin=PIPE, stdout=PIPE, stderr=PIPE)
    map_pipe_process = Popen(mapper_cmd2.split(" "), stdin=map_process_temp.stdout, stdout=PIPE)
    map_process_temp.stdout.close() 

    mapper_output, err = map_pipe_process.communicate()
    rc_m = map_pipe_process.returncode
    
    ## reducer
    reducer_cmd1, reducer_cmd2 = get_reducer_test_cmd(parsed, mapper_output)
    reducer_process_tmp = Popen(reducer_cmd1.split(" "), stdin=PIPE, stdout=PIPE, stderr=PIPE)
    reducer_pipe_process = Popen(reducer_cmd2.split(" "), stdin=reducer_process_tmp.stdout, stdout=PIPE)
    
    reducer_process_tmp.stdout.close() 
    reducer_output, err = reducer_pipe_process.communicate()
    rc_r = reducer_pipe_process.returncode
    
    return {
        "reducer_output": reducer_output, 
        "mapper_output": mapper_output
    }

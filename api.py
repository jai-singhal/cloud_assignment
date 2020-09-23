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
SELECT abc, def, sum(xyx)
FROM reviews
GROUP BY abc, def
HAVING sum(xyx)>10
""" 
# {"select": [{"value": "abc"}, {"value": "def"}, {"value": {"sum": "xyx"}}], "from": "reviews", 
#  "groupby": [{"value": "abc"}, {"value": "def"}], "having": {"gt": [{"sum": "xyx"}, 10]}}


@app.post("/run/sql")
async def main(query: str):
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
    def get_select_cols(select):
        select_cols = []
        aggregate_fns = {}
        for k, v in select.items():
            if isinstance(v, dict):
                aggregate_fns = {
                    "fun": v.keys()[0],
                    "col": v.values()[0],
                }
            else:
                select_cols.append(v)
        return select_cols, aggregate_fns        
        
    def get_mapper_test_cmd(parsed):
        select_cols, agg_fn = get_select_cols(parsed["select"])
        select_cols_pickle = binascii.hexlify(pickle.dumps(select_cols))
        return f"""cat data/test.txt |
         python3 mapper.py {select_cols_pickle} {parsed["from"]} {agg_fn["col"]}
        """
        
    def get_reducer_test_cmd(parsed, map_out):
        select_cols, agg_fn = get_select_cols(parsed["select"])
        X = parsed["having"]["gt"][1]
        return f"""echo {map_out} |
         python3 reducer.py {agg_fn["fun"]} {X}
        """
        
    parsed = json.dumps(parse(query))
    
    mapper_cmd = get_mapper_test_cmd(parsed)
    map_process = Popen(mapper_cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    mapper_output, err = map_process.communicate()
    rc_m = map_process.returncode
    
    reducer_cmd = get_reducer_test_cmd(parsed, mapper_output)
    reducer_process = Popen(reducer_cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    reducer_output, err = reducer_process.communicate()
    rc_r = reducer_process.returncode
    
    return {
        "reducer_output": reducer_output, 
        "mapper_output": mapper_output
    }

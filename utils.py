#!/usr/bin/env python3
import binascii
import json
import pickle
from subprocess import PIPE, Popen


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

    
def get_reducer_args(parsed):
    select_cols, agg_fn = get_select_cols(parsed["select"])
    (op, op_val), = parsed["having"].items()
    return [agg_fn["fun"], op_val[1], op]

def get_mapper_args(parsed):
    select_cols, agg_fn = get_select_cols(parsed["select"])
    select_cols_p = binascii.hexlify(pickle.dumps(select_cols, protocol = 2)).decode()
    where_cl = get_where_cond(parsed["where"])
    return [
        select_cols_p, parsed["from"], 
            agg_fn["col"], where_cl[0], 
            where_cl[1], where_cl[2],
            agg_fn["fun"]
    ]


# def run_mapper_process(parsed):
#     mapper_cmd = binascii.hexlify(pickle.dumps(get_mapper_args(parsed), protocol = 2)).decode()
    
#     map_process_temp = Popen(["cat", "data/test.txt"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
#     map_pipe_process = Popen(["python3", "mapper.py", mapper_cmd], stdin=map_process_temp.stdout, stdout=PIPE, stderr=PIPE)

#     mapper_output, err = map_pipe_process.communicate()
#     map_process_temp.stdout.close() 

#     rc_m = map_pipe_process.returncode
#     print("Error in mapper: ", err, 'return code: ', rc_m)
#     return mapper_output

# def run_reducer_process(parsed):
#     reducer_cmd = binascii.hexlify(pickle.dumps(get_reducer_args(parsed), protocol = 2)).decode()
    
#     reducer_temp_process = Popen(['cat', 'reducer_inp.txt'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
#     reducer_pipe_process = Popen(["python3", "reducer_test.py", reducer_cmd], stdin=reducer_temp_process.stdout, stdout=PIPE)
#     reducer_output, err = reducer_pipe_process.communicate()
#     reducer_temp_process.stdout.close() 
#     rc_m = reducer_pipe_process.returncode
#     print("Error in reducer: ", err, 'return code: ', rc_m)
#     return reducer_output

def reducer_operation(func, a, b):
    if func.lower() == "max":
        return max(a, b)
    elif func.lower() == "min":
        return min(a, b)
    elif func.lower() == "sum":
        return a+b
    elif func.lower() == "count":
        return a+b
    return 1

    
def having_cond_eval(cond, row, b):
    a = row[1]
    if cond.lower()== "gt":
        return a > b
    elif cond.lower() == "lt":
        return a < b
    elif cond.lower() == "eq":
        return a == b  
    elif cond.lower() == "gte":
        return a >= b  
    elif cond.lower() == "lte":
        return a <= b  
    elif cond.lower() == "neq":
        return a != b
    
def get_hadoop_steam_cmd(mapper_arggs_str, reducer_arggs_str
                         ,INPUT_FILE_NAME, dt_string):
    MAP_REDUCE_CMD = f"""
        hadoop 
        jar 
        hadoop-streaming/hadoop-streaming.jar 
        -file 
        mapper.py 
        -mapper
        mapper.py {mapper_arggs_str}
        -file 
        reducer_test.py 
        -reducer
        reducer_test.py {reducer_arggs_str}
        -input
        {INPUT_FILE_NAME}
        -output
        output/{dt_string} 
    """
    MAP_REDUCE_CMD = [
        arg.strip("\n ") for arg in 
        MAP_REDUCE_CMD.split("\n")
        if len(arg.strip("\n ")) != 0
    ]
    return MAP_REDUCE_CMD

def split_spark_mapper_results(line):
    if line is None:
        print("line is None")
        return []
    if isinstance(line, tuple):
        return [line,]
    else: return line
    

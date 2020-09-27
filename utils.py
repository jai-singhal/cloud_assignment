import binascii
import json
import pickle


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

    
def get_reducer_test_cmd(parsed, map_out):
    
    select_cols, agg_fn = get_select_cols(parsed["select"])
    (op, op_val), = parsed["having"].items()
    X = op_val[1]
    to_return = f"""python3 reducer.py {agg_fn["fun"]} {X} {op}"""
    return to_return

def get_mapper_args(parsed):
    select_cols, agg_fn = get_select_cols(parsed["select"])
    select_cols_p = binascii.hexlify(pickle.dumps(select_cols)).decode()
    where_cl = get_where_cond(parsed["where"])
    return (select_cols_p, parsed["from"], agg_fn["col"], where_cl[0], where_cl[1], where_cl[2])
from fastapi import FastAPI
from pydantic import BaseModel
import re
import json
import os
from moz_sql_parser import parse

app = FastAPI()

# uvicorn main:app --reload
query = """
SELECT abc, def, sum(xyx)
FROM reviews
GROUP BY abc, def
HAVING sum(xyx)>10
""" 
# sql_regex = re.compile(
#     ("""SELECT\s*([a-zA-Z0-9]+),*\s*([a-zA-Z0-9]*),*\s*(([A-Za-z]+)\(([a-zA-Z0-9]+)\))\s*\n*"""
#     """\s*FROM\s*([a-zA-Z]+)\s*\n*"""
#     """GROUP\s*BY\s*([a-zA-Z0-9]+),*\s*([a-zA-Z0-9]+)*"""
#     """HAVING\s*([A-Za-z]+)\(([a-zA-Z0-9]+)\)>(\d+)"""
# ), re.IGNORECASE)

def queryParam(sqlQuery):
    # sqlQuery = sqlQuery.replace("\n", " ")
    # return re.findall(sql_regex, query)
    parsed = json.dumps(parse(sqlQuery))
    print(parsed)

print(queryParam(query))

@app.post("/run/sql")
async def main(query: str):
    print(query)
    return {"message": "Hello World"}
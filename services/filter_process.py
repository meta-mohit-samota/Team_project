import polars as pl 
from polars import col 
from utils.logger import logger

def apply_filters(df: pl.DataFrame, conditions: dict) -> pl.DataFrame: 
    try: 
        expressions = [eval(expr.replace("and", "&").replace("or", "|")) for expr in conditions["Expressions"]] 
        op = conditions["operator"].lower() 
        if op == "and":
            combined = expressions[0] 
            for expr in expressions[1:]: 
                combined = combined & expr 
        elif op == "or": 
            combined = expressions[0] 
            for expr in expressions[1:]: 
                combined = combined | expr 
        else: 
            raise ValueError("Invalid operator") 
        return df.filter(combined) 
    except Exception as e: 
        logger.error(f"Filter error: {e}") 
        raise
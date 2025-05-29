# import polars as pl 
# from polars import sql 
# from utils.file_reader import read_file 
# from utils.logger import logger

# def join_files(primary_info, secondary_files): 
#     try: 
#         primary_df = read_file(primary_info.filename) 
#         sql.register("primary", primary_df)

#         for secondary in secondary_files:
#                 df = read_file(secondary.file_name)
#                 sql.register(secondary.file_name, df)
#                 join_expr = f"SELECT * FROM primary {secondary.join_type.upper()} JOIN {secondary.file_name} USING ({', '.join(secondary.join_columns)})"
#                 primary_df = sql.execute(join_expr)
#                 sql.register("primary", primary_df)

#         return primary_df
#     except Exception as e:
#         logger.error(f"Join error: {e}")

import polars as pl 
from polars import sql 
from utils.file_reader import read_file 
from utils.logger import logger

def join_files(primary_info, secondary_files): 
    try: 
        primary_df = read_file(primary_info["FileName"]) 
        sql.register("primary", primary_df)

        for secondary in secondary_files:
                df = read_file(secondary.file_name)
                sql.register(secondary.file_name, df)
                join_expr = f"SELECT * FROM primary {secondary.join_type.upper()} JOIN {secondary.file_name} USING ({', '.join(secondary.join_columns)})"
                primary_df = sql.execute(join_expr)
                sql.register("primary", primary_df)

        return primary_df
    except Exception as e:
        logger.error(f"Join error: {e}")
        raise
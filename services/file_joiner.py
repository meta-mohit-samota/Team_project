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















# import polars as pl 
# from polars import sql 
# from utils.file_reader import read_file 
# from utils.logger import logger

# def join_files(primary_info, secondary_files): 
#     try: 
#         primary_df = read_file(primary_info["FileName"]) 
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
#         raise




# import polars as pl
# from polars import sql
# from models.schemas import PrimaryFile, JoinFile
# from utils.file_reader import createDataframe
# from utils.logger import logger


# def join_files(df_map:dict,primary_info: PrimaryFile, secondary_files: list[JoinFile]) -> pl.DataFrame:
#     try:
#         # Read and register the primary file
#         primary_df = df_map[primary_info.filename]
#         sql.register("primary", primary_df)

#         # Join each secondary file in order
#         for secondary in secondary_files:
#             df = df_map[secondary.file_name]
#             sql.register(secondary.file_name, df)

#             join_expr = f"""
#                 SELECT * 
#                 FROM primary 
#                 {secondary.join_type.upper()} JOIN {secondary.file_name}
#                 USING ({', '.join(secondary.join_columns)})
#             """
#             logger.info(f"Executing SQL: {join_expr}")
#             primary_df = sql.execute(join_expr)
#             sql.register("primary", primary_df)  # update for next iteration

#         return primary_df

#     except Exception as e:
#         logger.error(f"Error during file join: {e},in file file_joiner", exc_info=True)
#         return pl.DataFrame()





import polars as pl
from models.schemas import PrimaryFile, JoinFile
from utils.logger import logger


def join_files(df_map: dict, primary_info: PrimaryFile, secondary_files: list[JoinFile]) -> pl.DataFrame:
    try:
        # Clear any previous context (optional but safe in long-running apps)
        # pl.sql.clear()

        # Register the primary dataframe
        primary_df_name = primary_info.filename
        primary_join_columns = primary_info.join_columns

        df1=df_map[primary_df_name]
        
        # pl.sql.register("primary", primary_df)

        # Iteratively join all secondary files
        for secondary in secondary_files:
            df2=df_map[secondary.file_name]
            # pl.sql.register(secondary.file_name, df)

            join_expr = f"""
                SELECT * 
                FROM df1 
                inner JOIN df2
                on df1.id = df2.roll;
            """
            logger.info(f"Executing SQL: {join_expr}")
            joined_df = pl.sql(join_expr)
            # pl.sql.register("primary", joined_df)  # Update primary for next join
            df1 = joined_df

        return joined_df

    except Exception as e:
        logger.error(f"Error during file join: {e}")
        return pl.DataFrame()
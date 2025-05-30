import polars as pl
from typing import List, Dict
from models.schemas import PrimaryFile, JoinFile # Your existing Pydantic schemas
from utils.logger import logger
from utils.constants import JoinType # Our new JoinType enum

def _construct_join_condition(left_cols: List[str], right_cols: List[str]) -> str:
    """
    Constructs the 'ON' clause for SQL joins based on provided column lists.

    Args:
        left_cols (List[str]):  List of join columns from the left DataFrame (df1).
        right_cols (List[str]): List of join columns from the right DataFrame (df2).

    Returns:
        str: A SQL 'ON' clause string (e.g., "df1.colA = df2.colX AND df1.colB = df2.colY").

    Raises:
        ValueError: If the number of columns in left_cols and right_cols do not match.
    """
    # Step 1: Validate input column lists
    if len(left_cols) != len(right_cols):
        error_message = (
            f"Join column mismatch! "
            f"Number of left columns ({len(left_cols)}) does not match "
            f"number of right columns ({len(right_cols)})."
        )
        logger.error(error_message)
        raise ValueError(error_message)

    # Step 2: Build the join condition string iteratively
    join_conditions = []
    for i in range(len(left_cols)):
        condition = f"df1.{left_cols[i]} = df2.{right_cols[i]}"
        join_conditions.append(condition)

    # Step 3: Combine conditions with ' AND '
    return " AND ".join(join_conditions)


def join_dataframes(
    dataframe_map: Dict[str, pl.DataFrame],
    primary_file_info: PrimaryFile,
    secondary_file_list: List[JoinFile]
) -> pl.DataFrame:
    """
    Joins multiple Polars DataFrames based on specified primary and secondary file configurations.

    This function iteratively joins a primary DataFrame with a list of secondary DataFrames
    using SQL queries executed via Polars' SQL context.

    Args:
        dataframe_map (Dict[str, pl.DataFrame]): A dictionary where keys are filenames
                                                  and values are loaded Polars DataFrames.
        primary_file_info (PrimaryFile): Pydantic model containing details for the
                                         primary DataFrame (filename, join columns).
        secondary_file_list (List[JoinFile]): A list of Pydantic models, each detailing
                                              a secondary file for joining (filename,
                                              join type, join columns).

    Returns:
        pl.DataFrame: The final joined Polars DataFrame.

    Raises:
        FileNotFoundError: If a specified filename in `primary_file_info` or
                           `secondary_file_list` is not found in `dataframe_map`.
        ValueError: If an invalid join type is encountered or if join columns mismatch.
        Exception: For any other unexpected errors during the join process.
    """
    # Step 1: Initialize the primary DataFrame
    primary_df_name = primary_file_info.filename
    primary_join_columns = primary_file_info.join_columns

    try:
        current_main_df = dataframe_map[primary_df_name]
        # Register the primary dataframe for SQL operations
        pl.SQLContext({"df1": current_main_df})
        logger.info(f"Registered primary DataFrame '{primary_df_name}' as 'df1'.")
    except KeyError:
        error_msg = f"Primary file '{primary_df_name}' not found in the provided dataframe_map."
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    except Exception as e:
        error_msg = f"An unexpected error occurred while setting up primary DataFrame: {e}"
        logger.error(error_msg)
        raise

    # Step 2: Iteratively join with secondary files
    for file_details in secondary_file_list:
        secondary_df_name = file_details.file_name
        join_type_str = file_details.join_type # This comes as a string from your Pydantic model
        secondary_join_columns = file_details.join_columns

        try:
            # Step 2.1: Retrieve the secondary DataFrame
            secondary_df = dataframe_map[secondary_df_name]
            pl.SQLContext({"df2": secondary_df}, eager_execution=True) # Register as df2 for the join
            logger.info(f"Registered secondary DataFrame '{secondary_df_name}' as 'df2'.")

            # Step 2.2: Validate and get the join type from Enum
            try:
                # Using a match-case equivalent for better readability than if-elif-else for enums
                # This ensures only valid join types are used.
                if join_type_str.upper() == JoinType.INNER.value:
                    sql_join_type = JoinType.INNER.value
                elif join_type_str.upper() == JoinType.LEFT.value:
                    sql_join_type = JoinType.LEFT.value
                elif join_type_str.upper() == JoinType.RIGHT.value:
                    sql_join_type = JoinType.RIGHT.value
                elif join_type_str.upper() == JoinType.FULL.value:
                    sql_join_type = JoinType.FULL.value
                elif join_type_str.upper() == JoinType.CROSS.value:
                    sql_join_type = JoinType.CROSS.value
                else:
                    raise ValueError(f"Invalid join type specified: '{join_type_str}'. "
                                     f"Allowed types are: {', '.join([jt.value for jt in JoinType])}")
            except ValueError as ve:
                logger.error(f"Configuration error for '{secondary_df_name}': {ve}")
                raise

            # Step 2.3: Construct the join 'ON' clause
            # For CROSS JOIN, there is no 'ON' clause.
            on_clause = ""
            if sql_join_type != JoinType.CROSS.value:
                on_clause = _construct_join_condition(primary_join_columns, secondary_join_columns)
                on_clause = f"ON {on_clause}"

            # Step 2.4: Build the SQL query
            join_query = f"""
                SELECT *
                FROM df1
                {sql_join_type} JOIN df2
                {on_clause}
            """
            logger.info(f"Executing SQL join query for '{secondary_df_name}':\n{join_query}")

            # Step 2.5: Execute the SQL query and update the main DataFrame
            joined_df = pl.sql(join_query)

            # Important: Update 'df1' in the SQL context for the next iteration
            # This is crucial for chaining joins correctly.
            # pl.SQLContext({"df1": joined_df}, eager_execution=True)
            current_main_df = joined_df # Also update Python variable for the final return

            logger.info(f"Successfully joined '{primary_df_name}' with '{secondary_df_name}'. "
                        f"Current DataFrame shape: {current_main_df.shape}")

        except KeyError:
            error_msg = (
                f"Secondary file '{secondary_df_name}' not found in the provided dataframe_map. "
                f"Skipping this join."
            )
            logger.warning(error_msg)
            # You could choose to raise here if missing secondary files are critical,
            # but warning and skipping might be more flexible.
            continue # Continue to the next secondary file if one is missing.
        except ValueError as ve:
            logger.error(f"Data validation error during join with '{secondary_df_name}': {ve}")
            raise # Re-raise the ValueError
        except pl.exceptions.ComputeError as ce:
            # Catch Polars-specific errors, e.g., schema mismatch in join keys
            error_msg = (
                f"Polars computation error during join with '{secondary_df_name}': {ce}. "
                f"Please check column types and existence."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg) from ce # Raise a more generic runtime error
        except Exception as e:
            error_msg = (
                f"An unexpected error occurred while joining '{secondary_df_name}': {e}. "
                f"Join process terminated."
            )
            logger.error(error_msg)
            raise # Re-raise the general exception

    logger.info("All specified files joined successfully!")
    return current_main_df
# import polars as pl
# from models.schemas import FilterConditions
# from utils.logger import logger

# def apply_filters(df: pl.DataFrame, conditions: FilterConditions) -> pl.DataFrame:
#     try:
#         expressions = conditions.expressions
#         operator = conditions.operator

#         # Parse expressions into Polars expressions
#         parsed_exprs = [parse_expression(expr) for expr in expressions]

#         if len(parsed_exprs) == 1:
#             # Single expression, apply directly
#             return df.filter(parsed_exprs[0])
#         elif operator == "And":
#             combined = parsed_exprs[0]
#             for expr in parsed_exprs[1:]:
#                 combined &= expr
#             return df.filter(combined)
#         elif operator == "Or":
#             combined = parsed_exprs[0]
#             for expr in parsed_exprs[1:]:
#                 combined |= expr
#             return df.filter(combined)
#         else:
#             raise ValueError("Invalid operator for multiple expressions")

#     except Exception as e:
#         logger.error(f"Error applying filters: {e}", exc_info=True)
#         raise


# def parse_expression(expr: str) -> pl.Expr:
#     """
#     Convert a string expression like 'age > 30' to a Polars expression.
#     Supported operators: ==, !=, >, >=, <, <=
#     """
#     import re

#     pattern = r"(.+?)\s*(==|!=|>=|<=|>|<)\s*(.+)"
#     match = re.match(pattern, expr.strip())
#     if not match:
#         raise ValueError(f"Invalid expression format: '{expr}'")

#     column, op, value = match.groups()
#     column = column.strip()
#     value = value.strip().strip('"').strip("'")

#     try:
#         # Try converting value to a number
#         if "." in value:
#             value = float(value)
#         else:
#             value = int(value)
#     except ValueError:
#         # Leave value as string
#         pass

#     # Build the Polars expression
#     if op == "==":
#         return pl.col(column) == value
#     elif op == "!=":
#         return pl.col(column) != value
#     elif op == ">":
#         return pl.col(column) > value
#     elif op == ">=":
#         return pl.col(column) >= value
#     elif op == "<":
#         return pl.col(column) < value
#     elif op == "<=":
#         return pl.col(column) <= value
#     else:
#         raise ValueError(f"Unsupported operator: '{op}'")


import polars as pl
import re
from typing import Union, Dict, Callable, List
from models.schemas import FilterConditions
from utils.logger import logger
from utils.constants import LogicalOperator, ComparisonOperator

class DataFrameFilterService:
    """
    Service class responsible for applying dynamic filters to Polars DataFrames.
    """

    def __init__(self):
        # Mapping for string operators to Polars expression functions
        self.comparison_operator_map: Dict[ComparisonOperator, Callable[[pl.Expr, Union[str, int, float]], pl.Expr]] = {
            ComparisonOperator.EQUAL: lambda col, val: col == val,
            ComparisonOperator.NOT_EQUAL: lambda col, val: col != val,
            ComparisonOperator.GREATER_THAN: lambda col, val: col > val,
            ComparisonOperator.GREATER_THAN_OR_EQUAL: lambda col, val: col >= val,
            ComparisonOperator.LESS_THAN: lambda col, val: col < val,
            ComparisonOperator.LESS_THAN_OR_EQUAL: lambda col, val: col <= val,
        }

    def _parse_value(self, value_str: str) -> Union[str, int, float]:
        """
        Helper method to attempt converting a string value to int or float.
        """
        try:
            if "." in value_str:
                return float(value_str)
            else:
                return int(value_str)
        except ValueError:
            # If conversion fails, return the original string
            return value_str

    def _parse_single_expression(self, expression_string: str) -> pl.Expr:
        """
        Converts a single string expression (e.g., 'age > 30') into a Polars expression.
        Raises ValueError for invalid expression formats or unsupported operators.
        """
        # Regex to capture column name, operator, and value
        expression_pattern = r"(.+?)\s*(==|!=|>=|<=|>|<)\s*(.+)"
        match = re.match(expression_pattern, expression_string.strip())

        if not match:
            logger.warning(f"Invalid expression format detected: '{expression_string}'")
            raise ValueError(f"Expression '{expression_string}' is not in a valid format (e.g., 'column == value').")

        column_name_raw, operator_str, value_raw = match.groups()
        column_name = column_name_raw.strip()
        # Remove quotes from the value string
        parsed_value = self._parse_value(value_raw.strip().strip('"').strip("'"))

        try:
            # Convert string operator to its Enum member
            comparison_operator = ComparisonOperator(operator_str)
        except ValueError:
            logger.error(f"Unsupported comparison operator found: '{operator_str}' in expression '{expression_string}'")
            raise ValueError(f"Unsupported operator: '{operator_str}'. Supported operators are {', '.join([op.value for op in ComparisonOperator])}.")

        # Use the mapping to get the correct Polars expression function
        operation_function = self.comparison_operator_map.get(comparison_operator)
        if operation_function is None:
            # This case should ideally not be reached if ComparisonOperator Enum is well-defined
            # and comparison_operator_map is complete.
            logger.error(f"Internal error: No mapping found for operator '{comparison_operator.value}'")
            raise NotImplementedError(f"Operation for '{comparison_operator.value}' is not implemented.")

        return operation_function(pl.col(column_name), parsed_value)

    def apply_dataframe_filters(self, dataframe: pl.DataFrame, conditions: FilterConditions) -> pl.DataFrame:
        """
        Applies a set of filter conditions to a Polars DataFrame.

        Args:
            dataframe (pl.DataFrame): The input Polars DataFrame.
            conditions (FilterConditions): An object containing filter expressions
                                           and the logical operator to combine them.

        Returns:
            pl.DataFrame: The filtered DataFrame.

        Raises:
            ValueError: If an expression is invalid or an unsupported operator is used.
            Exception: For any other unexpected errors during filtering.
        """
        if not isinstance(dataframe, pl.DataFrame):
            logger.error(f"Input 'dataframe' is not a Polars DataFrame. Type received: {type(dataframe)}")
            raise TypeError("The 'dataframe' argument must be a Polars DataFrame.")

        try:
            filter_expressions_strings = conditions.expressions
            logical_operator = LogicalOperator(conditions.operator)  # This is now a LogicalOperator Enum member

            # Step 1: Parse all string expressions into Polars expressions
            parsed_polars_expressions: List[pl.Expr] = []
            for expression_str in filter_expressions_strings:
                try:
                    parsed_polars_expressions.append(self._parse_single_expression(expression_str))
                except ValueError as ve:
                    # Catch specific parsing errors and re-raise with more context
                    logger.error(f"Failed to parse expression '{expression_str}': {ve}")
                    raise ValueError(f"Error parsing filter expression '{expression_str}': {ve}") from ve

            if not parsed_polars_expressions:
                logger.info("No filter expressions provided. Returning original DataFrame.")
                return dataframe # No expressions means no filtering

            # Step 2: Combine the parsed Polars expressions based on the logical operator
            combined_expression: pl.Expr
            if len(parsed_polars_expressions) == 1:
                combined_expression = parsed_polars_expressions[0]
            else:
                # Using a dictionary for a "switch-case" like pattern for logical operators
                logical_operation_map = {
                    LogicalOperator.AND: lambda exprs: pl.all_horizontal(exprs) if exprs else pl.lit(True),
                    LogicalOperator.OR: lambda exprs: pl.any_horizontal(exprs) if exprs else pl.lit(False)
                }

                operation_callable = logical_operation_map.get(logical_operator)

                if operation_callable is None:
                    # This case should not be reached if LogicalOperator Enum is properly handled
                    logger.error(f"Unsupported logical operator: '{logical_operator}'")
                    raise ValueError(f"Unsupported logical operator: '{logical_operator}'. "
                                     f"Supported operators are {', '.join([op.value for op in LogicalOperator])}.")

                combined_expression = operation_callable(parsed_polars_expressions)

            # Step 3: Apply the combined filter to the DataFrame
            filtered_dataframe = dataframe.filter(combined_expression)
            logger.info(f"Successfully applied filters to DataFrame. Original rows: {len(dataframe)}, Filtered rows: {len(filtered_dataframe)}")
            return filtered_dataframe

        except ValueError as ve:
            # Catch specific errors related to invalid inputs or logic
            logger.error(f"Validation or logic error during filter application: {ve}", exc_info=True)
            raise # Re-raise for the caller to handle

        except Exception as e:
            # Catch any other unexpected errors
            logger.critical(f"An unexpected error occurred while applying filters: {e}", exc_info=True)
            raise # Re-raise general exceptions
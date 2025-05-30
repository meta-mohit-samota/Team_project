from enum import Enum

class LogicalOperator(Enum):
    """
    Enum for logical operators used to combine filter expressions.
    """
    AND = "And"
    OR = "Or"

class ComparisonOperator(Enum):
    """
    Enum for comparison operators used in filter expressions.
    """
    EQUAL = "=="
    NOT_EQUAL = "!="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="
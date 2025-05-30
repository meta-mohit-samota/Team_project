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

class JoinType(str, Enum):
    """
    ✨ Defines valid SQL join types for robust and readable join operations.
    """
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS" # Although CROSS JOINs don't typically have an ON clause, including for completeness.

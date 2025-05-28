# from pydantic import BaseModel, Field, field_validator 
# from typing import List, Dict, Any

# class JoinFile(BaseModel): 
#     file_name: str = Field(..., min_length=1) 
#     join_columns: List[str] = Field(..., min_items=1) 
#     join_type: str = Field(..., pattern="(?i)^(inner|left|right|outer)$")

# class FileJoinInfo(BaseModel): 
#     primary_file: Dict[str, Any] 
#     secondary_files: List[JoinFile]

# class FilterCondition(BaseModel): 
#     fileName: str = Field(..., min_length=1) 
#     conditions: Dict[str, Any]  # Expressions: List[str], operator: str

#     @field_validator("conditions")
#     def validate_conditions(cls, value):
#         if not isinstance(value.get("Expressions"), list) or not value.get("operator"):
#             raise ValueError("'Expressions' must be a list and 'operator' must be provided.")
#         return value

# class FileProcessingRequest(BaseModel): 
#     files_and_join_info: FileJoinInfo 
#     filter: List[FilterCondition]

# from typing import List, Optional, Literal
# from pydantic import BaseModel, Field, field_validator, root_validator, ValidationError


# class JoinFile(BaseModel):
#     file_name: str = Field(..., alias="File name")
#     join_columns: List[str] = Field(default_factory=list, alias="Join_columns")
#     join_type: Optional[Literal["inner", "outer", "left", "right"]] = "outer"

#     @field_validator("join_columns", mode="before")
#     def validate_join_columns(cls, v):
#         if not isinstance(v, list):
#             raise ValueError("join_columns must be a list")
#         return v


# class PrimaryFile(BaseModel):
#     filename: str = Field(..., alias="Filename")
#     join_columns: List[str] = Field(default_factory=list, alias="Join_columns")

#     @field_validator("join_columns", mode="before")
#     def validate_join_columns(cls, v):
#         if not isinstance(v, list):
#             raise ValueError("join_columns must be a list")
#         return v


# class FilterConditions(BaseModel):
#     expressions: List[str] = Field(..., alias="Expressions")
#     operator: Optional[Literal["And", "Or"]] = Field(None, alias="operator")

#     @field_validator("expressions")
#     def check_expressions_not_empty(cls, v):
#         if not v:
#             raise ValueError("Expressions list cannot be empty")
#         return v

#     @field_validator("operator")
#     def validate_operator_presence(cls, v, values):
#         expressions = values.get("expressions", [])
#         if len(expressions) > 1 and v is None:
#             raise ValueError("Operator is required when more than one expression is provided")
#         if len(expressions) <= 1 and v is not None:
#             raise ValueError("Operator should only be provided when more than one expression exists")
#         return v


# class Filter(BaseModel):
#     file_name: str = Field(..., alias="fileName")
#     conditions: FilterConditions


# class FilesAndJoinInfo(BaseModel):
#     primary_file: PrimaryFile
#     secondary_files: List[JoinFile] = Field(default_factory=list)

#     @root_validator
#     def validate_joins(cls, values):
#         primary = values.get("primary_file")
#         secondaries = values.get("secondary_files", [])
#         if secondaries and not primary:
#             raise ValueError("Primary file must be specified when secondary files are present")
#         return values


# class InputModel(BaseModel):
#     files_and_join_info: FilesAndJoinInfo
#     filter: Optional[List[Filter]] = None

#     @root_validator
#     def validate_filter_and_files(cls, values):
#         files_info = values.get("files_and_join_info")
#         filters = values.get("filter")
#         secondary_files = files_info.secondary_files if files_info else []

#         if not secondary_files and (filters is None or len(filters) == 0):
#             raise ValueError("Filter is required when only one file is provided")

#         return values


from typing import List, Optional, Literal
from pydantic import BaseModel, Field, field_validator


class JoinFile(BaseModel):
    file_name: str = Field(..., alias="File name")
    join_columns: List[str] = Field(default_factory=list, alias="Join_columns")
    join_type: Optional[Literal["inner", "outer", "left", "right"]] = "outer"

    @field_validator("join_columns", mode="before")
    def validate_join_columns(cls, v):
        if not isinstance(v, list):
            raise ValueError("join_columns must be a list")
        return v


class PrimaryFile(BaseModel):
    filename: str = Field(..., alias="Filename")
    join_columns: List[str] = Field(default_factory=list, alias="Join_columns")

    @field_validator("join_columns", mode="before")
    def validate_join_columns(cls, v):
        if not isinstance(v, list):
            raise ValueError("join_columns must be a list")
        return v


class FilterConditions(BaseModel):
    expressions: List[str] = Field(..., alias="Expressions")
    operator: Optional[Literal["And", "Or"]] = Field(None, alias="operator")

    @field_validator("expressions")
    def check_expressions_not_empty(cls, v):
        if not v:
            raise ValueError("Expressions list cannot be empty")
        return v

    @field_validator("operator")
    def validate_operator_presence(cls, v, values):
        expressions = values.get("expressions", [])
        if len(expressions) > 1 and v is None:
            raise ValueError("Operator is required when more than one expression is provided")
        if len(expressions) <= 1 and v is not None:
            raise ValueError("Operator should not be provided when there is only one expression")
        return v


class Filter(BaseModel):
    file_name: str = Field(..., alias="fileName")
    conditions: FilterConditions


class FilesAndJoinInfo(BaseModel):
    primary_file: PrimaryFile
    secondary_files: List[JoinFile] = Field(default_factory=list)

    @field_validator("primary_file")
    def validate_joins(cls, values):
        primary = values.get("primary_file")
        secondaries = values.get("secondary_files", [])
        if secondaries and not primary:
            raise ValueError("Primary file must be specified when secondary files are present")
        return values


class InputModel(BaseModel):
    files_and_join_info: FilesAndJoinInfo
    filter: Optional[List[Filter]]

    @field_validator("files_and_join_info")
    def validate_filter_and_files(cls, values):
        files_info = values.get("files_and_join_info")
        filters = values.get("filter")
        secondary_files = files_info.secondary_files if files_info else []

        if not secondary_files and (not filters or len(filters) == 0):
            raise ValueError("Filter is required when only one file is provided")
        return values
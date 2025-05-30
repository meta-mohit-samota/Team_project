from typing import List, Optional, Literal
from pydantic import BaseModel, Field, model_validator

# Secondary file to be joined with primary file
class JoinFile(BaseModel):
    file_name: str = Field(..., alias="File name")
    join_columns: List[str] = Field(default_factory=list, alias="Join_columns")
    join_type: Optional[Literal["inner", "outer", "left", "right"]] = "outer"

    @model_validator(mode="after")
    def check_join_columns(self):
        if not isinstance(self.join_columns, list):
            raise ValueError("join_columns must be a list")
        return self

    model_config = {"populate_by_name" : True}

# Primary file to which secondary files will be joined
class PrimaryFile(BaseModel):
    filename: str = Field(..., alias="Filename")
    join_columns: List[str] = Field(default_factory=list, alias="Join_columns")

    @model_validator(mode="after")
    def check_join_columns(self):
        if not isinstance(self.join_columns, list):
            raise ValueError("join_columns must be a list")
        return self

    model_config = {"populate_by_name" : True}


class FilterConditions(BaseModel):
    expressions: List[str] = Field(..., alias="Expressions")
    operator: Optional[Literal["And", "Or"]] = Field(None, alias="operator")

    @model_validator(mode="after")
    def check_logic(self):
        if not self.expressions:
            raise ValueError("Expressions list cannot be empty")
        if len(self.expressions) > 1 and self.operator is None:
            raise ValueError("Operator is required when more than one expression is provided")
        if len(self.expressions) == 1 and self.operator is not None:
            raise ValueError("Operator should not be provided for a single expression")
        return self

    model_config = {"populate_by_name" : True}


class Filter(BaseModel):
    file_name: str = Field(..., alias="fileName")
    conditions: FilterConditions

    model_config = {"populate_by_name" : True}


class FilesAndJoinInfo(BaseModel):
    primary_file: PrimaryFile
    secondary_files: Optional[List[JoinFile]] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_secondary_requires_primary(self):
        if self.secondary_files and not self.primary_file:
            raise ValueError("Primary file must be specified when secondary files are present")
        return self

    model_config = {"populate_by_name" : True}


class InputModel(BaseModel):
    files_and_join_info: FilesAndJoinInfo
    filter: Optional[List[Filter]]

    @model_validator(mode="after")
    def validate_files_and_filters(self):
        if not self.files_and_join_info.secondary_files and not self.filter:
            raise ValueError("Filter is required when only one file is provided")
        return self

    model_config = {"populate_by_name" : True}
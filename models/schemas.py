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


# from typing import List, Optional, Literal
# from pydantic import BaseModel, Field, field_validator, model_validator


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
#             raise ValueError("Operator should not be provided when there is only one expression")
#         return v


# class Filter(BaseModel):
#     file_name: str = Field(..., alias="fileName")
#     conditions: FilterConditions


# class FilesAndJoinInfo(BaseModel):
#     primary_file: PrimaryFile
#     secondary_files: List[JoinFile] = Field(default_factory=list)

#     @model_validator(mode="after")
#     def validate_joins(cls, model):
#         if model.secondary_files and not model.primary_file:
#             raise ValueError("Primary file must be specified when secondary files are present")
#         return model


# class InputModel(BaseModel):
#     files_and_join_info: FilesAndJoinInfo
#     filter: Optional[List[Filter]]

#     @model_validator(mode="after")
#     def validate_filter_and_files(cls, model):
#         secondary_files = model.files_and_join_info.secondary_files
#         filters = model.filter

#         if not secondary_files and (not filters or len(filters) == 0):
#             raise ValueError("Filter is required when only one file is provided")
#         return model

# from typing import List, Optional, Literal
# from pydantic import BaseModel, Field, field_validator, model_validator


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

#     @model_validator(mode="after")
#     def validate_operator_presence(cls, values):
#         expressions = values.expressions
#         op = values.operator
#         if len(expressions) > 1 and op is None:
#             raise ValueError("Operator is required when more than one expression is provided")
#         if len(expressions) <= 1 and op is not None:
#             raise ValueError("Operator should not be provided when only one expression is present")
#         return values


# class Filter(BaseModel):
#     file_name: str = Field(..., alias="fileName")
#     conditions: FilterConditions


# class FilesAndJoinInfo(BaseModel):
#     primary_file: PrimaryFile
#     secondary_files: List[JoinFile] = Field(default_factory=list)

#     @model_validator(mode="after")
#     def validate_joins(cls, model):
#         if model.secondary_files and not model.primary_file:
#             raise ValueError("Primary file must be specified when secondary files are present")
#         return model


# class InputModel(BaseModel):
#     files_and_join_info: FilesAndJoinInfo
#     filter: Optional[List[Filter]]

#     @model_validator(mode="after")
#     def validate_filter_and_files(cls, model):
#         secondary_files = model.files_and_join_info.secondary_files
#         filters = model.filter

#         if not secondary_files and (not filters or len(filters) == 0):
#             raise ValueError("Filter is required when only one file is provided")
#         return model


from typing import List, Optional, Literal
from pydantic import BaseModel, Field, model_validator


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

# from typing import List, Literal, Optional
# from pydantic import BaseModel, Field, field_validator, root_validator

# class PrimaryFileInfo(BaseModel):
#     """
#     📁 Defines the structure for the primary file.
#     """
#     filename: str = Field(..., description="Name of the primary file.", alias="Filename")
#     join_columns: List[str] = Field(
#         default_factory=list,
#         description="List of columns to use for joining.",
#         alias="Join_columns"
#     )

#     class Config:
#         populate_by_name = True
#         # Allow extra fields if your actual JSON might have them and you want to ignore them
#         # extra = "ignore"


# class SecondaryFileInfo(BaseModel):
#     """
#     📄 Defines the structure for each secondary file.
#     """
#     filename: str = Field(..., description="Name of the secondary file.", alias="File name") # Note the space in alias
#     join_columns: List[str] = Field(
#         default_factory=list,
#         description="List of columns to use for joining this secondary file.",
#         alias="Join_columns"
#     )
#     join_type: Literal["inner", "outer", "left", "right"] = Field(
#         "outer", # Default join type is "outer"
#         description="Type of join (inner, outer, left, right).",
#         alias="join_type"
#     )

#     @field_validator("join_type", mode="before")
#     @classmethod
#     def standardize_join_type(cls, value: str) -> str:
#         """
#         ⚙️ Standardizes the join_type to lowercase before validation.
#         This allows inputs like "Inner" or "LEFT" to be accepted.
#         """
#         if isinstance(value, str):
#             lower_value = value.lower()
#             if lower_value in ["inner", "outer", "left", "right"]:
#                 return lower_value
#         # Let Pydantic's Literal validator raise the more specific error if it's not one of the allowed values
#         # after this attempt to standardize. If it's not a string, Pydantic will also catch that.
#         # However, for explicit error message for wrong type:
#         raise ValueError(f"Invalid join_type '{value}'. Must be one of 'inner', 'outer', 'left', 'right'.")


#     class Config:
#         populate_by_name = True


# class FilesAndJoinInfo(BaseModel):
#     """
#     🔗 Groups primary and secondary file information.
#     """
#     primary_file: PrimaryFileInfo = Field(..., description="Information about the primary file.", alias="primary_file")
#     secondary_files: List[SecondaryFileInfo] = Field(
#         default_factory=list, # Defaults to an empty list if not provided
#         description="List of secondary files and their join information.",
#         alias="secondary_files"
#     )

#     class Config:
#         populate_by_name = True


# class FilterConditions(BaseModel):
#     """
#     🔍 Defines the expressions and logical operator for filtering.
#     """
#     expressions_list: List[str] = Field(
#         ...,
#         min_length=1, # Must have at least one expression
#         description="List of filter expressions (e.g., 'age > 30').",
#         alias="Expressions"
#     )
#     # Operator is Optional, defaults to None. Validation logic is in root_validator.
#     operator_logic: Optional[Literal["And", "Or"]] = Field(
#         None,
#         description="Logical operator ('And' or 'Or') to combine expressions.",
#         alias="operator"
#     )

#     @root_validator(skip_on_failure=True)
#     @classmethod
#     def check_operator_logic(cls, values):
#         """
#         🚦 Validates operator logic:
#         1. If multiple expressions, 'operator_logic' is required.
#         2. If one or zero expressions, 'operator_logic' must NOT be provided (should be None).
#         """
#         # 'values' contains the already validated field values by their Python names
#         expr_list = values.get("expressions_list") # Already validated for min_length=1
#         op_logic = values.get("operator_logic")

#         if expr_list: # Will always be true because of min_length=1
#             if len(expr_list) > 1:
#                 if not op_logic:
#                     raise ValueError(
#                         "**Validation Error!** 🛑 Operator ('And'/'Or') is **required** "
#                         "when there are multiple expressions."
#                     )
#             elif len(expr_list) == 1: # Only one expression
#                 if op_logic:
#                     raise ValueError(
#                         "**Validation Error!** 🛑 Operator should **not** be provided "
#                         "for a single expression."
#                     )
#         return values

#     class Config:
#         populate_by_name = True


# class FilterInfo(BaseModel):
#     """
#     🎯 Defines filter conditions for a specific file.
#     """
#     file_name_to_filter: str = Field(..., description="Name of the file to apply this filter to.", alias="fileName")
#     filter_conditions_obj: FilterConditions = Field(..., description="The conditions for filtering.", alias="conditions")

#     class Config:
#         populate_by_name = True


# class APIRequest(BaseModel):
#     """
#     📬 The main request model for your API.
#     """
#     files_config: FilesAndJoinInfo = Field(
#         ...,
#         description="Information about files to be processed and how they join.",
#         alias="files_and_join_info"
#     )
#     filter_rules: List[FilterInfo] = Field(
#         default_factory=list, # Defaults to an empty list if not provided
#         description="Optional list of filters to apply to the files.",
#         alias="filter"
#     )

#     @root_validator(skip_on_failure=True)
#     @classmethod
#     def validate_request_logic(cls, values):
#         """
#         🧠 Root validator for complex cross-field validation for the entire request.
#         1. Filter is mandatory if only one file is uploaded.
#         2. Ensures 'fileName' in filter_rules refers to an actual uploaded file.
#         """
#         files_info_obj = values.get("files_config")
#         current_filter_rules = values.get("filter_rules") # Will be [] if not provided, not None

#         # This check might be redundant if files_config is non-optional, but good for safety.
#         if not files_info_obj:
#             # This would typically be caught by Pydantic if files_config is a required field.
#             # If files_config could be None, this is where you'd handle it.
#             # For now, assuming files_config is mandatory.
#             return values

#         # --- Calculate total number of files ---
#         # primary_file is mandatory within FilesAndJoinInfo
#         num_primary_files = 1
#         num_secondary_files = len(files_info_obj.secondary_files)
#         total_files = num_primary_files + num_secondary_files

#         # --- Rule 1: Filter is mandatory if only one file is uploaded. ---
#         # "Must throw error if one file is given and no filter is there"
#         if total_files == 1:
#             if not current_filter_rules: # current_filter_rules is an empty list
#                 raise ValueError(
#                     "**Validation Error!** 🛑 Filter is **mandatory** when only one file "
#                     "(the primary file) is uploaded. Please provide filter conditions."
#                 )
#         # If total_files > 1, filters are optional (i.e., current_filter_rules can be an empty list).

#         # --- Rule 2: Join is only optional until we upload two or more files. ---
#         # This is implicitly handled by the structure:
#         # - If total_files == 1, then files_info_obj.secondary_files must be empty.
#         # - If total_files > 1, then files_info_obj.secondary_files is non-empty,
#         #   and join information (like join_type) becomes relevant via SecondaryFileInfo.

#         # --- Additional Check: Ensure 'fileName' in FilterInfo corresponds to an uploaded file. ---
#         if current_filter_rules: # Only perform this check if filters are actually provided
#             uploaded_filenames = {files_info_obj.primary_file.filename}
#             for sec_file in files_info_obj.secondary_files:
#                 uploaded_filenames.add(sec_file.filename)

#             for f_info_item in current_filter_rules:
#                 if f_info_item.file_name_to_filter not in uploaded_filenames:
#                     raise ValueError(
#                         f"**Validation Error!** 🛑 Filename '{f_info_item.file_name_to_filter}' "
#                         f"in the filter rules does **not match any uploaded file**. "
#                         f"Uploaded files are: {', '.join(sorted(list(uploaded_filenames)))}."
#                     )
#         return values

#     class Config:
#         populate_by_name = True
#         # Use an example for documentation purposes if you generate OpenAPI schema
#         # examples = { ... }


# from typing import List, Literal, Optional, Set
# from pydantic import BaseModel, Field, field_validator, model_validator

# class PrimaryFileInfo(BaseModel):
#     filename: str = Field(..., description="Name of the primary file.", alias="Filename")
#     join_columns: List[str] = Field(default_factory=list, description="List of columns to use for joining.", alias="Join_columns")

#     model_config = {
#         "populate_by_name": True
#     }

# class SecondaryFileInfo(BaseModel):
#     filename: str = Field(..., description="Name of the secondary file.", alias="File name")
#     join_columns: List[str] = Field(default_factory=list, description="List of columns to use for joining this secondary file.", alias="Join_columns")
#     join_type: Literal["inner", "outer", "left", "right"] = Field("outer", description="Type of join.", alias="join_type")

#     @field_validator("join_type", mode="before")
#     @classmethod
#     def standardize_join_type(cls, value: str) -> str:
#         if isinstance(value, str):
#             lower = value.lower()
#             if lower in {"inner", "outer", "left", "right"}:
#                 return lower
#         raise ValueError(f"Invalid join_type '{value}'. Must be one of 'inner', 'outer', 'left', 'right'.")

#     model_config = {
#         "populate_by_name": True
#     }

# class FilesAndJoinInfo(BaseModel):
#     primary_file: PrimaryFileInfo = Field(..., alias="primary_file")
#     secondary_files: Optional[List[SecondaryFileInfo]] = Field(default_factory=list, alias="secondary_files")

#     model_config = {
#         "populate_by_name": True
#     }

# class FilterConditions(BaseModel):
#     expressions_list: List[str] = Field(..., min_length=1, alias="Expressions")
#     operator_logic: Optional[Literal["And", "Or"]] = Field(None, alias="operator")

#     @model_validator(mode="after")
#     def check_operator_logic(self):
#         expr_list = self.expressions_list
#         op = self.operator_logic

#         if len(expr_list) > 1 and not op:
#             raise ValueError(
#                 "**Validation Error!** 🛑 Operator ('And'/'Or') is **required** "
#                 "when there are multiple expressions."
#             )
#         elif len(expr_list) == 1 and op:
#             raise ValueError(
#                 "**Validation Error!** 🛑 Operator should **not** be provided "
#                 "for a single expression."
#             )
#         return self

#     model_config = {
#         "populate_by_name": True
#     }

# class FilterInfo(BaseModel):
#     file_name_to_filter: str = Field(..., alias="fileName")
#     filter_conditions_obj: FilterConditions = Field(..., alias="conditions")

#     model_config = {
#         "populate_by_name": True
#     }

# class APIRequest(BaseModel):
#     files_config: FilesAndJoinInfo = Field(..., alias="files_and_join_info")
#     filter_rules: Optional[List[FilterInfo]] = Field(default_factory=list, alias="filter")

#     @model_validator(mode="after")
#     def validate_request_logic(self):
#         files_info = self.files_config
#         filters = self.filter_rules or []

#         # Rule 1: Filter is required if only one file is uploaded
#         total_files = 1 + len(files_info.secondary_files)
#         if total_files == 1 and not filters:
#             raise ValueError(
#                 "**Validation Error!** 🛑 Filter is **mandatory** when only one file "
#                 "(the primary file) is uploaded."
#             )

#         # Rule 2: Validate fileName in filters match uploaded files
#         uploaded: Set[str] = {files_info.primary_file.filename}
#         uploaded.update(sec.filename for sec in files_info.secondary_files)

#         for filt in filters:
#             if filt.file_name_to_filter not in uploaded:
#                 raise ValueError(
#                     f"**Validation Error!** 🛑 Filter file '{filt.file_name_to_filter}' "
#                     f"does not match any uploaded files: {', '.join(sorted(uploaded))}."
#                 )

#         return self

#     model_config = {
#         "populate_by_name": True
#   }
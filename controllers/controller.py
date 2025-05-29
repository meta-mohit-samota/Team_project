# from fastapi import APIRouter, HTTPException 
# from models.schemas import InputModel
# # from models.schemas import APIRequest
# from services.file_joiner import join_files 
# from services.filter_process import apply_filters 
# from utils.file_reader import read_file 
# # from utils.path_util import get_output_path 
# from utils.path_util import INPUT_DIR, OUTPUT_DIR
# from utils.logger import logger
# from utils.fileName_generator import generate_filename
# import json

# router = APIRouter()

# @router.post("/process") 
# def process_files(request: InputModel): 
#     try: 
#         join_result = join_files(request.files_and_join_info.primary_file, request.files_and_join_info.secondary_files)

#         for condition in request.filter:
#             df = read_file(condition.file_name)
#             filtered_df = apply_filters(df, condition.conditions)
#             # output_path = get_output_path(f"filtered_{condition.fileName}")
#             output_path = OUTPUT_DIR / generate_filename()
#             filtered_df.write_csv(output_path)

#         # logger.info(json.dumps(request,indent=2))
#         # # for key,val in request.json.items():
#         # #     logger.info(key+"\t"+val+"\n")
#         # return {json.dumps(request,indent=2)}

#         return {"message": "Files processed successfully"}
#     except Exception as e:
#         logger.error(f"API error: {e}")
#         raise HTTPException(status_code=500, detail=str(e))


from fastapi import APIRouter, HTTPException
from models.schemas import InputModel
from services.file_joiner import join_files
from services.filter_process import apply_filters
from utils.file_reader import createDataframe # create datafame
from utils.path_util import getFullOutputPath
from utils.logger import logger
from utils.fileName_generator import generate_filename

router = APIRouter()

@router.post("/process")
def process_files(request: InputModel):
    try:
        
        # if request.filter:
        #     for condition in request.filter:
                # df = read_file(request.file_name)
                # logger.info(df)
                # filtered_df = apply_filters(df, condition.conditions)
                # output_path = OUTPUT_DIR / generate_filename()
                # filtered_df.write_csv(output_path)

        # logger.info(request.files_and_join_info.primary_file)
        # logger.info(request.files_and_join_info.secondary_files)
        # logger.info(request.filter)
        # # logger.info(request.model_config)
        # logger.info(request.files_and_join_info.primary_file.filename)
        # logger.info(request.files_and_join_info.primary_file.join_columns)
        
        # for file_name in request.filter:
        #     logger.info(file_name.file_name)
        #     logger.info(file_name.conditions)
        #     logger.info(file_name.conditions.expressions)
        
        # for fil in request.files_and_join_info.secondary_files:
        #     logger.info(fil.file_name)
        #     logger.info(fil.join_type)
        #     logger.info(fil.join_columns)

        primary_file = request.files_and_join_info.primary_file.filename
        df_map = {primary_file:createDataframe(primary_file) }
        if request.files_and_join_info.secondary_files:
            for secondary_file_details in request.files_and_join_info.secondary_files:
                df_map[secondary_file_details.file_name] = createDataframe(secondary_file_details.file_name)
        
    
        if request.filter:
            for file_details in request.filter:
                filter_file_name = file_details.file_name
                if(filter_file_name not in df_map):
                    logger.error(" This file is not in join files ")
                df_map[filter_file_name] = apply_filters(df_map[filter_file_name],file_details.conditions)

        logger.info("After applying filter")
        logger.info(df_map)

        final_processed_df = join_files(df_map,request.files_and_join_info.primary_file,request.files_and_join_info.secondary_files)
        logger.info(final_processed_df)
        final_processed_df.collect().write_csv(getFullOutputPath())







        return {"message": request}
    except Exception as e:
        logger.error(f"API error: {e}", exc_info=True)
        return {"error came":"yes"}
        # raise HTTPException(status_code=500, detail=str(e))
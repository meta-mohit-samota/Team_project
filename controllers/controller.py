from fastapi import APIRouter,HTTPException
from models.schemas import InputModel
from services.file_joiner import join_files
# from services.filter_process import apply_filters
# from utils.file_reader import createDataframe # create datafame
from utils.file_reader import load_data_from_file
from utils.path_util import getFullOutputPath
from utils.logger import logger
from services.filter_process import DataFrameFilterService

router = APIRouter()

@router.post("/process")
def process_files(request: InputModel):
    try:
        # reading dataframes from source and storing in a dicitonary
        try:
            primary_file = request.files_and_join_info.primary_file.filename
            # df_map = {primary_file:createDataframe(primary_file) }
            df_map = {primary_file:load_data_from_file(primary_file) }
            if request.files_and_join_info.secondary_files:
                for secondary_file_details in request.files_and_join_info.secondary_files:
                    # df_map[secondary_file_details.file_name] = createDataframe(secondary_file_details.file_name)
                    df_map[secondary_file_details.file_name] = load_data_from_file(secondary_file_details.file_name)
            logger.info(df_map)
        except Exception as e:
            logger.error("Got error while reading files.")
            raise HTTPException(status_code=500,detail=str(e))    
        
    
        # Applied filter for specified column in the given dataframe (if given)
        try:
            filter_service = DataFrameFilterService()
            if request.filter:
                for file_details in request.filter:
                    filter_file_name = file_details.file_name
                    if(filter_file_name not in df_map):
                        logger.error(" This file is not in join files ")
                    # df_map[filter_file_name] = apply_filters(df_map[filter_file_name],file_details.conditions)
                    
                    df_map[filter_file_name] = filter_service.apply_dataframe_filters(df_map[filter_file_name],file_details.conditions)
                logger.info("Filters applied.")
        except Exception as e:
            logger.error("Got error while applying filter.")
            raise HTTPException(status_code=500,detail=str(e))

        # Joined files after applying filters (if given) as per types and columns
        try:
            final_processed_df = join_files(df_map,request.files_and_join_info.primary_file,request.files_and_join_info.secondary_files)
            logger.info(final_processed_df)
        except Exception as e:
            logger.error("Got error while joining file.")
            raise HTTPException(status_code=500,detail=str(e))

        # Written processed dataframe to csv file
        try:
            final_processed_df.collect().write_csv(getFullOutputPath())
        except Exception as e:
            raise HTTPException(status_code=500,detail=str(e))
        
        # Returning the output file path
        return {"Output_File_Path": getFullOutputPath()}
    
    except Exception as e:
        logger.error(f"API error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
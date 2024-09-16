# main.py

import logging
import traceback
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import os

from extractor import PDFExtractor
from database import DatabaseManager
from utils import load_yaml_config
import config

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExtractionRequest(BaseModel):
    folder_path: str
    yaml_path: str
    table_name: Optional[str] = config.TABLE_NAME

@app.post("/extract")
async def extract_data(request: ExtractionRequest):
    folder_path = request.folder_path
    yaml_path = request.yaml_path
    table_name = request.table_name

    # Initialize DatabaseManager early to log any potential errors
    db_manager = DatabaseManager(
        server=config.SERVER,
        database=config.DATABASE
    )

    db_manager.ensure_activity_log_table()
    db_manager.ensure_error_log_table()
    try:
        # Validate folder path
        if not os.path.isdir(folder_path):
            raise ValueError("Invalid folder path.")
        # Validate YAML path
        if not os.path.isfile(yaml_path):
            raise ValueError("Invalid YAML file path.")

        # Load YAML configuration
        fields_config = load_yaml_config(yaml_path)

        # Initialize PDFExtractor
        pdf_extractor = PDFExtractor(
            poppler_path=config.POPPLER_PATH,
            tesseract_cmd=config.TESSERACT_CMD
        )

        # Ensure the table exists
        if not db_manager.table_exists(table_name):
            db_manager.create_table(table_name, fields_config)
            logger.info(f"Table '{table_name}' created.")

        # Process PDFs in the folder
        for filename in os.listdir(folder_path):
            if filename.lower().endswith('.pdf'):
                pdf_file_path = os.path.join(folder_path, filename)
                logger.info(f"Processing file: {filename}")
                try:
                    extracted_data = pdf_extractor.extract_data_from_pdf(pdf_file_path, fields_config)
                    extracted_data['file_name'] = filename
                    db_manager.insert_data(table_name, extracted_data, fields_config)
                    # Log success activity
                    success_message = f"Successfully processed file {filename}"
                    db_manager.log_activity(
                        file_name=filename,
                        status='Success',
                        message=success_message,
                        function_name='extract_data'
                    )
                except Exception as e:
                    error_message = f"Error processing file {filename}"
                    error_details = traceback.format_exc()
                    logger.error(error_message)
                    # Log error to ActivityLog
                    db_manager.log_activity(
                        file_name=filename,
                        status='Failed',
                        message=error_message,
                        function_name='extract_data',
                        details=error_details
                    )
                    # Log error to ErrorLog
                    db_manager.log_error(
                        error_message=error_message,
                        error_details=error_details,
                        function_name='extract_data',
                        file_name=filename
                    )
                    continue  # Continue with the next file

    except Exception as e:
        error_message = "An error occurred during processing"
        error_details = traceback.format_exc()
        logger.error(error_message)
        # Log error to ActivityLog
        db_manager.log_activity(
            file_name=None,
            status='Failed',
            message=error_message,
            function_name='extract_data',
            details=error_details
        )
        # Log error to ErrorLog
        db_manager.log_error(
            error_message=error_message,
            error_details=error_details,
            function_name='extract_data'
        )
        raise HTTPException(status_code=500, detail=error_message)

    finally:
        db_manager.close()
    return {"status": "success", "message": "Processing completed."}

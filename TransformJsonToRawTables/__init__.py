import logging
import json
import os
import io
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Tuple
from urllib.parse import urlparse

import pandas as pd
import pyarrow
import pendulum

import azure.functions as func
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

# Global Constants
STORAGE_ACCOUNT_URL = os.environ.get("STORAGE_ACCOUNT_URL")
OUTPUT_CONTAINER_NAME = "processed-data"
DOCUMENT_DATA_PATH = "document_data"
TASK_DATA_PATH = "task_data"
INPUT_CONTAINER_NAME = "analysis-results"  # Expected input container name

# Target schemas
DOCUMENT_SCHEMA = [
    "TenantID", "ItemID_SP", "file_name", "file_extension", "URLDocumento", 
    "AutoreNome", "Modified_Date", "CodiceProgetto", "DateCreated_SP", 
    "Deadline", "DataRevisione", "StatoRevisione", "Priority", "ReviewFeedback",
    "descriptive_summary", "technical_summary", 
    "ScoreClarityAI", "JustificationClarityAI",
    "ScoreEvidenceAI", "JustificationEvidenceAI",
    "ScoreLogicAI", "JustificationLogicAI",
    "ScoreStyleAI", "JustificationStyleAI",
    "ScoreDepthAI", "JustificationDepthAI",
    "recommendations", "DataAnalisiUTC",
    "sharepoint_site_id", "m365_group_id", "planner_plan_id"
]

TASK_SCHEMA = [
    "TenantID", "ItemID_SP_SourceDoc", "task_id", "task_title", "employee_name",
    "assignee_id", "planTitle", "bucketName", "createdDateTime", "dueDateTime",
    "percentComplete", "completedDateTime", "priority", "description", "DataAnalisiUTC"
]

# Field mappings for document data
DOC_FIELD_MAPPING = {
    "TenantID": ["metadata", "tenant_id"],
    "ItemID_SP": ["metadata", "item_id"],
    "file_name": ["metadata", "file_name"],
    "file_extension": ["file_extension"],
    "URLDocumento": None,  # Now handled explicitly in extract_document_data
    "AutoreNome": ["sharepoint_custom_metadata", "Autore_x002f_i", 0, "LookupValue"],
    "Modified_Date": ["sharepoint_internal_metadata", "Modified"],
    "CodiceProgetto": ["sharepoint_custom_metadata", "CodiceProgetto"],
    "DateCreated_SP": ["sharepoint_internal_metadata", "Created"],
    "Deadline": ["sharepoint_custom_metadata", "Deadline"],
    "DataRevisione": ["sharepoint_custom_metadata", "DataRevisione"],
    "StatoRevisione": ["sharepoint_custom_metadata", "StatoRevisione"],
    "Priority": ["sharepoint_custom_metadata", "Priorit_x00e0_"],
    "ReviewFeedback": ["sharepoint_custom_metadata", "ReviewFeedback"],
    "descriptive_summary": ["analysis_output", "descriptive_summary"],
    "technical_summary": ["analysis_output", "technical_summary"],
    "recommendations": ["analysis_output", "recommendations"],
    "DataAnalisiUTC": ["metadata", "processed_at_utc"],
    "sharepoint_site_id": ["sharepoint_site_id"],
    "m365_group_id": ["m365_group_id"],
    "planner_plan_id": ["planner_plan_id"]
}

# Field mappings for task data (paths within task object)
TASK_FIELD_MAPPING = {
    "task_id": ["id"],
    "task_title": ["title"],
    "employee_name": None,  # Not directly available
    "assignee_id": None,    # Not directly available
    "planTitle": ["planTitle"],
    "bucketName": ["bucketName"],
    "createdDateTime": ["createdDateTime"],
    "dueDateTime": ["dueDateTime"],
    "percentComplete": ["percentComplete"],
    "completedDateTime": ["completedDateTime"],
    "priority": ["priority"],
    "description": ["description"]
}

async def main(event: func.EventGridEvent):
    """
    Main entry point for the Azure Function.
    
    Processes a JSON blob from an Event Grid event, transforms it into document and task DataFrames,
    and writes them as Parquet files to Azure Blob Storage.
    
    Args:
        event: Azure Event Grid event containing blob creation notification
    """
    # Initialize variables
    tenant_id = None
    item_id = None
    parsed_data = None
    df_document = None
    df_tasks = None
    blob_url = None
    
    try:
        # Get event data
        event_data = event.get_json()
        if event_data is None:
            logging.error("Event data is None or empty")
            return
            
        logging.info(f"Received Event Grid event: {json.dumps(event_data, indent=2)}")
        
        # Extract blob URL from event data - directly at top level (standard for blob events)
        blob_url = event_data.get('url') or event_data.get('blobUrl') 
        
        # --- ADD FILTERING ---
        if not blob_url:
            logging.error("Event data does not contain a 'url' or 'blobUrl'. Skipping.")
            # Returning None or completing gracefully is usually better than raising an error for non-matching events
            return 

        # Parse the URL to check the container and file extension
        try:
            parsed_url = urlparse(blob_url)
            path_parts = parsed_url.path.lstrip('/').split('/')
            if len(path_parts) < 2:
                logging.warning(f"Could not parse container/blob name from URL: {blob_url}. Skipping.")
                return

            container_name = path_parts[0]
            blob_name = '/'.join(path_parts[1:])

            # Check if the blob is in the expected container and has a .json extension
            if container_name != INPUT_CONTAINER_NAME:
                logging.info(f"Ignoring event for blob in container '{container_name}'. Expected '{INPUT_CONTAINER_NAME}'.")
                return
            if not blob_name.lower().endswith('.json'):
                logging.info(f"Ignoring event for non-JSON blob: {blob_name}")
                return

            logging.info(f"Processing relevant blob event for: {container_name}/{blob_name}")

        except Exception as parse_err:
            logging.error(f"Error parsing blob URL '{blob_url}' for filtering: {parse_err}. Skipping event.", exc_info=True)
            return
        # --- END FILTERING ---
            
        # Initialize Azure credential
        credential = DefaultAzureCredential()
        
        # Use async context manager for BlobServiceClient
        async with BlobServiceClient(account_url=STORAGE_ACCOUNT_URL, credential=credential) as blob_service_client:
            # Get blob client for input blob
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            
            # Download and parse blob content with specific error handling
            try:
                download_stream = await blob_client.download_blob()
                blob_content_bytes = await download_stream.readall()
                blob_content = blob_content_bytes.decode('utf-8')
                parsed_data = json.loads(blob_content)
                logging.info(f"Successfully parsed JSON content from blob {blob_name}")
            except ResourceNotFoundError as e:
                error_msg = f"Blob not found: {container_name}/{blob_name}"
                logging.error(error_msg)
                return
            except HttpResponseError as e:
                error_msg = f"HTTP error accessing blob: {container_name}/{blob_name}. Status: {e.status_code}, Reason: {e.reason}"
                logging.error(error_msg)
                return
            except json.JSONDecodeError as e:
                error_msg = f"Failed to parse JSON from blob {container_name}/{blob_name}: {str(e)}"
                logging.error(error_msg)
                return
            except Exception as e:
                error_msg = f"Error reading blob content from {container_name}/{blob_name}: {str(e)}"
                logging.error(error_msg)
                return
            
            # Extract key identifiers
            tenant_id = get_nested_value(parsed_data, ["metadata", "tenant_id"]) or \
                        get_nested_value(parsed_data, ["tenant_id_context"])
            
            item_id = get_nested_value(parsed_data, ["metadata", "item_id"]) or \
                    get_nested_value(parsed_data, ["item_id"])
            
            if not tenant_id or not item_id:
                logging.warning(f"Missing essential identifiers - TenantID: {tenant_id}, ItemID_SP: {item_id}")
            
            # Process document data (now passing blob_url)
            logging.info(f"Tenant {tenant_id}, Item {item_id}: Extracting document data")
            doc_data = extract_document_data(parsed_data, tenant_id, item_id, blob_url)
            df_document = pd.DataFrame([doc_data])
            
            # Apply explicit type conversions for document data
            df_document = apply_document_types(df_document, tenant_id, item_id)
            
            # Ensure correct column order
            df_document = df_document.reindex(columns=DOCUMENT_SCHEMA)
            
            # Process task data if available
            logging.info(f"Tenant {tenant_id}, Item {item_id}: Extracting task data")
            tasks_data = extract_tasks_data(parsed_data, tenant_id, item_id)
            df_tasks = pd.DataFrame(tasks_data if tasks_data else [], columns=TASK_SCHEMA)
            
            # Apply explicit type conversions for task data
            if not df_tasks.empty:
                df_tasks = apply_task_types(df_tasks, tenant_id, item_id)
            
            # Generate output paths and filenames
            current_date = pendulum.now('UTC')
            year = current_date.year
            month = current_date.month
            
            unique_id = str(uuid.uuid4())
            
            # Container paths
            doc_container = OUTPUT_CONTAINER_NAME
            task_container = OUTPUT_CONTAINER_NAME
            
            # Blob paths within containers
            doc_blob_path = f"{DOCUMENT_DATA_PATH}/tenant_id={tenant_id}/anno={year}/mese={month}/doc_{item_id}_{unique_id}.parquet"
            task_blob_path = f"{TASK_DATA_PATH}/tenant_id={tenant_id}/anno={year}/mese={month}/tasks_{item_id}_{unique_id}.parquet"
            
            # Write document DataFrame to Parquet
            try:
                logging.info(f"Tenant {tenant_id}, Item {item_id}: Writing document data to {doc_container}/{doc_blob_path}")
                await write_parquet_to_blob(
                    df_document, 
                    blob_service_client, 
                    doc_container, 
                    doc_blob_path
                )
                logging.info(f"Tenant {tenant_id}, Item {item_id}: Document data successfully written")
            except Exception as e:
                logging.error(f"Tenant {tenant_id}, Item {item_id}: Failed to write document data: {str(e)}", exc_info=True)
                return
            
            # Write tasks DataFrame to Parquet if not empty
            if not df_tasks.empty:
                try:
                    logging.info(f"Tenant {tenant_id}, Item {item_id}: Writing task data to {task_container}/{task_blob_path}")
                    await write_parquet_to_blob(
                        df_tasks, 
                        blob_service_client, 
                        task_container, 
                        task_blob_path
                    )
                    logging.info(f"Tenant {tenant_id}, Item {item_id}: Task data successfully written")
                except Exception as e:
                    logging.error(f"Tenant {tenant_id}, Item {item_id}: Failed to write task data: {str(e)}", exc_info=True)
                    return
            else:
                logging.info(f"Tenant {tenant_id}, Item {item_id}: No task data to write")
                
    except Exception as e:
        logging.error(f"Error processing event: {str(e)}", exc_info=True)
    finally:
        logging.info("Completed processing of event")


def get_nested_value(data: Dict[str, Any], path: List[Any], default: Any = None) -> Any:
    """
    Safely navigate a nested dictionary using a list of keys.
    
    Args:
        data: Dictionary to navigate
        path: List of keys to follow
        default: Default value to return if path not found
        
    Returns:
        The value at the specified path or default if not found
    """
    try:
        value = data
        for key in path:
            if isinstance(value, dict) and key in value:
                value = value[key]
            elif isinstance(value, list) and isinstance(key, int) and 0 <= key < len(value):
                value = value[key]
            else:
                return default
        return value
    except (KeyError, TypeError, IndexError):
        return default


def join_array_to_string(array: Optional[List[str]], delimiter: str = "; ") -> Optional[str]:
    """
    Join an array of strings into a single string.
    
    Args:
        array: List of strings to join
        delimiter: Delimiter to use for joining
        
    Returns:
        Joined string or None if input is None or empty
    """
    if not array:
        return None
    
    if not isinstance(array, list):
        return str(array) if array is not None else None
        
    return delimiter.join(array)


def extract_document_data(data: Dict[str, Any], tenant_id: str, item_id: str, blob_url: str) -> Dict[str, Any]:
    """
    Extract and transform document-level data from the parsed JSON.
    
    Args:
        data: The parsed JSON data
        tenant_id: The tenant ID
        item_id: The document item ID
        blob_url: The URL of the blob being processed
    
    Returns:
        Dictionary containing the transformed document data
    """
    # Initialize document data with default values
    doc_data = {field: None for field in DOCUMENT_SCHEMA}
    
    # Set primary key fields
    doc_data["TenantID"] = tenant_id
    doc_data["ItemID_SP"] = item_id
    
    # Set URLDocumento directly from the blob URL
    doc_data["URLDocumento"] = blob_url
    
    # Extract fields using mapping
    for target_field, source_path in DOC_FIELD_MAPPING.items():
        try:
            if source_path is None:
                continue
                
            # Get value from nested path
            value = get_nested_value(data, source_path)
            
            # Apply transformations based on field type
            if target_field in ["descriptive_summary", "technical_summary", "recommendations"]:
                value = join_array_to_string(value)
            
            doc_data[target_field] = value
            
        except Exception as e:
            logging.warning(f"Tenant {tenant_id}, Item {item_id}: Error extracting field {target_field}: {str(e)}")
    
    # --- START MODIFIED SCORE PROCESSING ---
    logging.info(f"Tenant {tenant_id}, Item {item_id}: Processing fixed AI scores...")
    scores_dict = get_nested_value(data, ["analysis_output", "technical_scores"], {})

    # Define the fixed keys expected from the updated prompt
    fixed_score_keys = {
        "Clarity": ("ScoreClarityAI", "JustificationClarityAI"),
        "Evidence": ("ScoreEvidenceAI", "JustificationEvidenceAI"),
        "Logic": ("ScoreLogicAI", "JustificationLogicAI"),
        "Style": ("ScoreStyleAI", "JustificationStyleAI"),
        "Depth": ("ScoreDepthAI", "JustificationDepthAI")
    }

    if isinstance(scores_dict, dict):
        for key, (score_col, just_col) in fixed_score_keys.items():
            score_data = scores_dict.get(key) # Get the nested object for the key
            if isinstance(score_data, dict):
                doc_data[score_col] = score_data.get("score")
                doc_data[just_col] = score_data.get("justification")
                logging.debug(f"Tenant {tenant_id}, Item {item_id}: Extracted score/justification for key '{key}'")
            else:
                # Log if the expected key or its nested structure is missing
                logging.warning(f"Tenant {tenant_id}, Item {item_id}: Expected score data for key '{key}' not found or not a dict in technical_scores. Setting {score_col} and {just_col} to None.")
                doc_data[score_col] = None
                doc_data[just_col] = None
    else:
        logging.warning(f"Tenant {tenant_id}, Item {item_id}: 'analysis_output.technical_scores' is not a dictionary or is missing. Skipping all score extraction.")
        # Ensure all score and justification columns are None
        for score_col, just_col in fixed_score_keys.values():
             doc_data[score_col] = None
             doc_data[just_col] = None
    # --- END MODIFIED SCORE PROCESSING ---
    
    return doc_data


def extract_tasks_data(data: Dict[str, Any], tenant_id: str, item_id: str) -> List[Dict[str, Any]]:
    """
    Extract and transform task data from the parsed JSON.
    
    Args:
        data: The parsed JSON data
        tenant_id: The tenant ID
        item_id: The document item ID
    
    Returns:
        List of dictionaries containing the transformed task data
    """
    tasks_data = []
    
    # Get the processed_at_utc timestamp for DataAnalisiUTC
    data_analisi_utc = get_nested_value(data, ["metadata", "processed_at_utc"])
    
    # Get tasks array
    tasks = get_nested_value(data, ["employee_planner_tasks"], [])
    if not isinstance(tasks, list):
        logging.warning(f"Tenant {tenant_id}, Item {item_id}: employee_planner_tasks is not a list or is missing")
        return tasks_data
    
    for task in tasks:
        try:
            # Initialize task data with default values
            task_data = {field: None for field in TASK_SCHEMA}
            
            # Set primary key fields
            task_data["TenantID"] = tenant_id
            task_data["ItemID_SP_SourceDoc"] = item_id
            task_data["DataAnalisiUTC"] = data_analisi_utc
            
            # Extract fields using mapping
            for target_field, source_path in TASK_FIELD_MAPPING.items():
                if source_path is None:
                    continue
                    
                # Get value from nested path within task object
                value = get_nested_value(task, source_path)
                task_data[target_field] = value
            
            tasks_data.append(task_data)
            
        except Exception as e:
            logging.warning(f"Tenant {tenant_id}, Item {item_id}: Error processing task: {str(e)}")
    
    return tasks_data


def apply_document_types(df: pd.DataFrame, tenant_id: str, item_id: str) -> pd.DataFrame:
    """
    Apply appropriate data types to document DataFrame columns.
    
    Args:
        df: DataFrame to process
        tenant_id: The tenant ID for logging context
        item_id: The document item ID for logging context
        
    Returns:
        DataFrame with corrected data types
    """
    try:
        # Date/time fields
        datetime_fields = ["Modified_Date", "DateCreated_SP", "DataAnalisiUTC"]
        for field in datetime_fields:
            df[field] = pd.to_datetime(df[field], errors='coerce')
        
        # Date fields
        date_fields = ["Deadline", "DataRevisione"]
        for field in date_fields:
            df[field] = pd.to_datetime(df[field], errors='coerce').dt.date
        
        # Numeric fields
        score_fields = ["ScoreClarityAI", "ScoreEvidenceAI", "ScoreLogicAI", "ScoreStyleAI", "ScoreDepthAI"]
        for field in score_fields:
            df[field] = pd.to_numeric(df[field], errors='coerce').astype('float64')
        
        # String fields (ensure strings for all text fields)
        string_fields = [
            "TenantID", "ItemID_SP", "file_name", "file_extension", "URLDocumento", 
            "AutoreNome", "CodiceProgetto", "StatoRevisione", "Priority", "ReviewFeedback",
            "descriptive_summary", "technical_summary", "recommendations",
            "sharepoint_site_id", "m365_group_id", "planner_plan_id",
            "JustificationClarityAI", "JustificationEvidenceAI", "JustificationLogicAI", 
            "JustificationStyleAI", "JustificationDepthAI"
        ]
        for field in string_fields:
            if field in df.columns: # Check if column exists before applying
                # Only convert non-None values to string
                df[field] = df[field].apply(lambda x: str(x) if x is not None else None)
    
    except Exception as e:
        logging.warning(f"Tenant {tenant_id}, Item {item_id}: Error applying document data types: {str(e)}")
    
    return df


def apply_task_types(df: pd.DataFrame, tenant_id: str, item_id: str) -> pd.DataFrame:
    """
    Apply appropriate data types to task DataFrame columns.
    
    Args:
        df: DataFrame to process
        tenant_id: The tenant ID for logging context
        item_id: The document item ID for logging context
        
    Returns:
        DataFrame with corrected data types
    """
    try:
        # Date/time fields
        datetime_fields = ["createdDateTime", "completedDateTime", "DataAnalisiUTC"]
        for field in datetime_fields:
            df[field] = pd.to_datetime(df[field], errors='coerce')
        
        # Date fields
        date_fields = ["dueDateTime"]
        for field in date_fields:
            df[field] = pd.to_datetime(df[field], errors='coerce').dt.date
        
        # Numeric fields
        numeric_fields = ["percentComplete", "priority"]
        for field in numeric_fields:
            df[field] = pd.to_numeric(df[field], errors='coerce').astype('Int64')
        
        # String fields
        string_fields = [
            "TenantID", "ItemID_SP_SourceDoc", "task_id", "task_title", 
            "employee_name", "assignee_id", "planTitle", "bucketName", "description"
        ]
        for field in string_fields:
            # Only convert non-None values to string
            df[field] = df[field].apply(lambda x: str(x) if x is not None else None)
            
    except Exception as e:
        logging.warning(f"Tenant {tenant_id}, Item {item_id}: Error applying task data types: {str(e)}")
    
    return df


async def write_parquet_to_blob(
    df: pd.DataFrame, 
    blob_service_client: BlobServiceClient, 
    container_name: str, 
    blob_name: str
) -> None:
    """
    Write a DataFrame to a Parquet file in Azure Blob Storage.
    
    Args:
        df: The DataFrame to write
        blob_service_client: The BlobServiceClient instance
        container_name: The container name
        blob_name: The blob path within the container
    """
    # Create in-memory buffer
    buffer = io.BytesIO()
    
    # Write DataFrame to buffer
    df.to_parquet(buffer, engine='pyarrow', index=False)
    
    # Reset buffer position
    buffer.seek(0)
    
    # Get blob client directly
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )
    
    # Upload the buffer to the blob
    await blob_client.upload_blob(buffer.getvalue(), overwrite=True)

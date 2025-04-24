# --- Start of ProcessQueuedDocument/__init__.py ---
# --- Version: Azure Document Intelligence Integration (Gemini Analysis) ---

import logging
import json
import os
import datetime
import io
import ast  # Still needed for potential future use, but not strictly required by removed code
from typing import Dict, Any, Optional, List, Tuple, Union
import asyncio # Import asyncio for potential sleeps
import urllib.parse

import azure.functions as func
import httpx
import pendulum # For timezone-aware datetime handling
# Removed Anthropic import
import google.generativeai as genai
from google.generativeai.types import GenerationConfig
from google.api_core import exceptions as google_api_exceptions

from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import BlobServiceClient
# Add Azure Document Intelligence imports
from azure.ai.documentintelligence.aio import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
from azure.ai.documentintelligence.models import DocumentContentFormat # Correct import for v1.0.2
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import HttpResponseError, ClientAuthenticationError # Added ClientAuthenticationError

# --- Configuration ---
GRAPH_API_ENDPOINT = os.environ.get("GRAPH_API_ENDPOINT", "https://graph.microsoft.com/v1.0")
GRAPH_SCOPES = os.environ.get("GRAPH_API_SCOPE", "https://graph.microsoft.com/.default").split()
RESULTS_CONTAINER_NAME = os.environ.get("RESULTS_CONTAINER_NAME", "analysis-results")
LOCK_CONTAINER_NAME = os.environ.get("LOCK_CONTAINER_NAME", "analysis-locks")
# Lease duration in seconds (must be between 15 and 60, or -1 for infinite)
# Choose a duration longer than your expected max processing time.
DEFAULT_LEASE_DURATION = 60 # seconds
# Removed Anthropic API configuration
# Document Intelligence configuration
AZURE_DOCUMENTINTELLIGENCE_ENDPOINT = os.environ.get("AZURE_DOCUMENTINTELLIGENCE_ENDPOINT")
AZURE_DOCUMENTINTELLIGENCE_KEY = os.environ.get("AZURE_DOCUMENTINTELLIGENCE_KEY")
DOC_INTELLIGENCE_MODEL = os.environ.get("DOC_INTELLIGENCE_MODEL", "prebuilt-layout")
# Character limits for analysis (adjust as needed, rough estimate)
MAX_CHARS_FOR_ANALYSIS = int(os.environ.get("MAX_CHARS_FOR_ANALYSIS", "150000"))
# Idempotency check window
IDEMPOTENCY_WINDOW_MINUTES = int(os.environ.get("IDEMPOTENCY_WINDOW_MINUTES", "10"))
# Google Generative AI configuration for both models
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GEMINI_PROMPT_MODEL = os.environ.get("GEMINI_PROMPT_MODEL", "gemini-1.5-flash-latest")
GEMINI_ANALYSIS_MODEL = os.environ.get("GEMINI_ANALYSIS_MODEL", "gemini-1.5-flash-latest")

# --- Global Clients ---
credential: Optional[DefaultAzureCredential] = None
blob_service_client: Optional[BlobServiceClient] = None
# Removed anthropic_client
doc_intelligence_client: Optional[DocumentIntelligenceClient] = None
gemini_model: Optional[genai.GenerativeModel] = None
gemini_analysis_model: Optional[genai.GenerativeModel] = None

# --- Supported File Extensions ---
SUPPORTED_FILE_EXTENSIONS = [
    ".pdf", ".docx", ".doc", ".pptx", ".ppt", ".xlsx", ".xls"
]

# --- Initialization ---
try:
    # Azure Identity
    credential_init_attempts = 3
    for attempt in range(credential_init_attempts):
        try:
            credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
            logging.info(f"DefaultAzureCredential initialized (attempt {attempt+1}).")
            break # Success
        except ClientAuthenticationError as cred_auth_err:
            logging.warning(f"Attempt {attempt+1}/{credential_init_attempts} failed to initialize DefaultAzureCredential (Auth Error): {cred_auth_err}")
            if attempt == credential_init_attempts - 1:
                 logging.error("Failed to initialize DefaultAzureCredential after multiple attempts.")
                 credential = None
            else:
                asyncio.sleep(2) # Wait before retry
        except Exception as cred_err:
             logging.warning(f"Attempt {attempt+1}/{credential_init_attempts} failed to initialize DefaultAzureCredential (General Error): {cred_err}")
             if attempt == credential_init_attempts - 1:
                  logging.error("Failed to initialize DefaultAzureCredential after multiple attempts.")
                  credential = None
             else:
                asyncio.sleep(2)

    # Blob Storage Client
    storage_connection_string = os.environ.get("AzureWebJobsStorage")
    if not storage_connection_string:
        logging.warning("AzureWebJobsStorage env var not set. Blob operations will fail.")
    else:
        try:
            blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
            logging.info("Blob Service Client initialized.")
        except Exception as blob_err:
            logging.error(f"Failed to initialize Blob Service Client: {blob_err}")
            blob_service_client = None

    # Removed Anthropic Client initialization

    # Document Intelligence Client
    if AZURE_DOCUMENTINTELLIGENCE_ENDPOINT:
        try:
            if AZURE_DOCUMENTINTELLIGENCE_KEY:
                di_credential = AzureKeyCredential(AZURE_DOCUMENTINTELLIGENCE_KEY)
                logging.info("Using API Key for Document Intelligence.")
            elif credential:
                di_credential = credential
                logging.info("Using DefaultAzureCredential for Document Intelligence.")
            else:
                logging.error("Cannot initialize Document Intelligence Client: No valid key found and DefaultAzureCredential failed.")
                di_credential = None

            if di_credential:
                 doc_intelligence_client = DocumentIntelligenceClient(
                     endpoint=AZURE_DOCUMENTINTELLIGENCE_ENDPOINT,
                     credential=di_credential
                 )
                 logging.info(f"Document Intelligence client initialization attempted for endpoint: {AZURE_DOCUMENTINTELLIGENCE_ENDPOINT}")
            else:
                 doc_intelligence_client = None

        except Exception as doc_intel_err:
            logging.error(f"Failed to initialize Document Intelligence client: {doc_intel_err}", exc_info=True)
            doc_intelligence_client = None
    else:
        logging.warning("AZURE_DOCUMENTINTELLIGENCE_ENDPOINT not set. Document Intelligence features unavailable.")
        doc_intelligence_client = None

    # Google Generative AI (Gemini) Client - Initialize both models
    if not GOOGLE_API_KEY:
        logging.warning("GOOGLE_API_KEY env var not set. Gemini features unavailable.")
        gemini_model = None
        gemini_analysis_model = None
    else:
        try:
            # Configure the API with the key
            genai.configure(api_key=GOOGLE_API_KEY)
            
            # Initialize prompt generation model
            try:
                gemini_model = genai.GenerativeModel(f"models/{GEMINI_PROMPT_MODEL}")
                logging.info(f"Gemini prompt model initialized: {GEMINI_PROMPT_MODEL}")
            except Exception as prompt_model_err:
                logging.error(f"Failed to initialize Gemini prompt model: {prompt_model_err}", exc_info=True)
                gemini_model = None
            
            # Initialize analysis model
            try:
                gemini_analysis_model = genai.GenerativeModel(f"models/{GEMINI_ANALYSIS_MODEL}")
                logging.info(f"Gemini analysis model initialized: {GEMINI_ANALYSIS_MODEL}")
            except Exception as analysis_model_err:
                logging.error(f"Failed to initialize Gemini analysis model: {analysis_model_err}", exc_info=True)
                gemini_analysis_model = None
                
        except Exception as gemini_err:
            logging.error(f"Failed to initialize Google Generative AI client: {gemini_err}", exc_info=True)
            gemini_model = None
            gemini_analysis_model = None

except Exception as e:
    logging.error(f"Critical error during global initialization: {e}", exc_info=True)
    credential = None
    blob_service_client = None
    # Removed anthropic_client reference
    doc_intelligence_client = None
    gemini_model = None
    gemini_analysis_model = None


# --- Helper Functions ---
async def get_graph_token() -> Optional[str]:
    """Authenticates and gets an access token for Microsoft Graph."""
    if not credential:
        logging.error("Graph Token: DefaultAzureCredential not initialized.")
        return None
    try:
        logging.debug("Graph Token: Attempting to get token...")
        token = await credential.get_token(*GRAPH_SCOPES)
        logging.debug("Graph Token: Successfully obtained Graph token.")
        return token.token
    except Exception as auth_err:
        logging.error(f"Graph Token: Failed to obtain Graph token: {auth_err}", exc_info=True)
        return None


async def fetch_file_content(drive_id: str, item_id: str, access_token: str) -> bytes:
    """Fetches file content from Microsoft Graph API."""
    graph_url = f"{GRAPH_API_ENDPOINT}/drives/{drive_id}/items/{item_id}/content"
    headers = {'Authorization': f'Bearer {access_token}'}
    async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as client:
        try:
            logging.debug(f"Fetching file content from {graph_url}")
            response = await client.get(graph_url, headers=headers)
            response.raise_for_status()
            logging.debug(f"File download successful (Status: {response.status_code})")
            return await response.aread()
        except httpx.HTTPStatusError as exc:
            logging.error(f"HTTP error fetching file {item_id}: {exc.response.status_code} - {exc.response.text}")
            raise Exception(f"Graph API error fetching file: {exc.response.status_code}") from exc
        except Exception as exc:
            logging.error(f"Error fetching file {item_id}: {exc}", exc_info=True)
            raise Exception(f"Failed to fetch file content: {exc}") from exc


async def store_result(analysis_result: Dict[str, Any], item_id: str, tenant_id: str) -> bool:
    """Store analysis result in Azure Blob Storage, incorporating tenant ID."""
    if not blob_service_client:
        logging.error("Blob Service Client not available. Cannot store result.")
        return False
    if not item_id or item_id == "unknown":
        logging.error("Invalid item_id for storing results. Cannot proceed.")
        return False
    try:
        container_client = blob_service_client.get_container_client(RESULTS_CONTAINER_NAME)
        try:
            await container_client.get_container_properties()
            logging.info(f"Container '{RESULTS_CONTAINER_NAME}' already exists.")
        except HttpResponseError as ex:
            if ex.status_code == 404:
                try:
                    await container_client.create_container()
                    logging.info(f"Container '{RESULTS_CONTAINER_NAME}' created.")
                except Exception as create_err:
                     logging.error(f"Failed to create container '{RESULTS_CONTAINER_NAME}': {create_err}", exc_info=True)
                     return False
            else:
                 logging.error(f"Error checking container '{RESULTS_CONTAINER_NAME}' existence: {ex}", exc_info=True)
                 return False

        safe_item_id = "".join(c for c in item_id if c.isalnum() or c in ('-', '_')).rstrip()[:100]
        timestamp = pendulum.now('UTC').strftime("%Y%m%d%H%M%S")
        result_blob_name = f"{tenant_id}/{safe_item_id}_{timestamp}.json"
        logging.info(f"Storing analysis result to: container='{RESULTS_CONTAINER_NAME}', blob='{result_blob_name}'")

        result_blob_client = blob_service_client.get_blob_client(container=RESULTS_CONTAINER_NAME, blob=result_blob_name)
        analysis_result["tenant_id_context"] = tenant_id
        analysis_result["item_id"] = item_id
        analysis_result["blob_path"] = f"{RESULTS_CONTAINER_NAME}/{result_blob_name}"
        
        # Add logging to check what's in the dictionary before serialization
        logging.info(f"Store Result Check: Keys in dictionary before serialization for blob '{result_blob_name}': {list(analysis_result.keys())}")
        if "sharepoint_metadata" in analysis_result:
            if analysis_result["sharepoint_metadata"]:
                logging.info(f"Store Result Check: SharePoint metadata is present with {len(analysis_result['sharepoint_metadata'])} fields")
            else:
                logging.info("Store Result Check: SharePoint metadata is present but empty")
        else:
            logging.warning("Store Result Check: SharePoint metadata key is missing from the result dictionary")

        def default_serializer(obj):
            if isinstance(obj, (datetime.datetime, datetime.date)):
                return pendulum.instance(obj).isoformat()
            if isinstance(obj, pendulum.Interval):
                 return str(obj)
            try:
                return json.JSONEncoder.default(None, obj)
            except TypeError:
                return str(obj)

        try:
            result_json = json.dumps(analysis_result, indent=2, default=default_serializer)
        except Exception as json_err:
            logging.error(f"Failed to serialize result to JSON for item {item_id}: {json_err}", exc_info=True)
            error_json = json.dumps({
                 "status": "serialization_error",
                 "error": f"Failed to serialize result: {str(json_err)}",
                 "item_id": item_id,
                 "tenant_id_context": tenant_id,
                 "blob_path": f"{RESULTS_CONTAINER_NAME}/{result_blob_name}"
            })
            await result_blob_client.upload_blob(error_json.encode('utf-8'), overwrite=True)
            return False

        await result_blob_client.upload_blob(result_json.encode('utf-8'), overwrite=True)
        logging.info(f"Successfully stored result to blob: {RESULTS_CONTAINER_NAME}/{result_blob_name}")

        try:
            properties = await result_blob_client.get_blob_properties()
            logging.info(f"Verified blob exists: size={properties.size} bytes, etag={properties.etag}")
        except Exception as verify_err:
            logging.warning(f"Could not verify blob existence after upload: {verify_err}")

        try:
            await asyncio.sleep(1.0)
            logging.info("Added safety delay after blob upload.")
        except Exception as delay_err:
            logging.warning(f"Could not add safety delay after blob storage: {delay_err}")

        return True
    except Exception as storage_error:
        logging.error(f"Failed to store analysis result for item {item_id}: {storage_error}", exc_info=True)
        return False


async def check_recent_analysis_exists(
    blob_service_client: Optional[BlobServiceClient],
    container_name: str,
    tenant_id: str,
    item_id: str,
    time_window_minutes: int
) -> bool:
    """ Check if a recent analysis exists for the given document. """
    if not blob_service_client:
        logging.warning("Idempotency Check: Blob Service Client not available.")
        return False
    if not item_id or item_id == "unknown":
        logging.warning("Idempotency Check: Invalid item_id.")
        return False
    try:
        safe_item_id = "".join(c for c in item_id if c.isalnum() or c in ('-', '_')).rstrip()[:100]
        prefix = f"{tenant_id}/{safe_item_id}_"
        logging.info(f"Idempotency check: Searching for blobs with prefix: '{prefix}'")
        cutoff_time = pendulum.now('UTC').subtract(minutes=time_window_minutes)
        container_client = blob_service_client.get_container_client(container_name)
        found_recent = False
        async for blob in container_client.list_blobs(name_starts_with=prefix):
            if blob.creation_time:
                blob_time = pendulum.instance(blob.creation_time).in_tz('UTC')
                if blob_time > cutoff_time:
                    found_recent = True
                    logging.warning(f"IDEMPOTENCY: Found recent blob '{blob.name}' created at {blob_time.isoformat()}. Skipping duplicate.")
                    break
        if not found_recent:
             logging.info(f"Idempotency check: No recent analysis found for item '{item_id}'.")
        return found_recent
    except HttpResponseError as ex:
        if ex.status_code == 404:
             logging.info(f"Idempotency check: Container '{container_name}' does not exist yet.")
             return False
        else:
             logging.error(f"Idempotency check: HTTP error listing blobs: {ex}", exc_info=True)
             return False
    except Exception as e:
        logging.error(f"Idempotency check: Error checking blobs for item '{item_id}': {e}", exc_info=True)
        return False


async def extract_markdown_with_doc_intelligence(file_content_bytes: bytes) -> str:
    """ Extracts document content as markdown using Azure Document Intelligence. """
    if not doc_intelligence_client:
        error_msg = "Document Intelligence client not initialized"
        logging.error(error_msg)
        raise RuntimeError(error_msg)
    if not file_content_bytes:
         error_msg = "Input file content is empty."
         logging.error(error_msg)
         raise ValueError(error_msg)
    try:
        logging.info(f"DI Extract: Preparing request with {len(file_content_bytes)} bytes.")
        if len(file_content_bytes) < 50:
            logging.info(f"DI Extract: File content start: {file_content_bytes[:50]}")
        request = AnalyzeDocumentRequest(bytes_source=file_content_bytes)
        logging.info(f"DI Extract: Starting analysis with model: {DOC_INTELLIGENCE_MODEL}")
        poller = await doc_intelligence_client.begin_analyze_document(
            DOC_INTELLIGENCE_MODEL,
            request,
            output_content_format=DocumentContentFormat.MARKDOWN
        )
        logging.info("DI Extract: Waiting for analysis to complete...")
        result = await poller.result()
        logging.info("DI Extract: Analysis poller completed.")
        if result and hasattr(result, "content") and result.content:
            markdown_content = result.content
            content_length = len(markdown_content)
            logging.info(f"DI Extract: Success. Extracted {content_length} characters.")
            logging.debug(f"Markdown content sample: {markdown_content[:200]}...")
            return markdown_content
        else:
            error_msg = "DI Extract: Operation completed but returned no content."
            logging.error(error_msg)
            return f"[Error: {error_msg}]"
    except HttpResponseError as api_err:
        error_details = f"Status Code: {api_err.status_code}, Reason: {api_err.reason}, Message: {api_err.message}"
        error_msg = f"DI Extract: API error: {error_details}"
        logging.error(error_msg, exc_info=True)
        raise RuntimeError(error_msg) from api_err
    except Exception as e:
        error_msg = f"DI Extract: Unexpected error: {str(e)}"
        logging.error(error_msg, exc_info=True)
        raise RuntimeError(error_msg) from e


async def acquire_blob_lease(blob_service_client: BlobServiceClient, container_name: str, blob_name: str, lease_duration: int) -> Optional[str]:
    """
    Attempts to acquire a lease on a blob, creating the blob if it doesn't exist.
    Returns the lease ID if successful, None otherwise.
    """
    if not blob_service_client:
        logging.error(f"Lease Lock: Blob service client not available for blob {blob_name}.")
        return None
    
    try:
        lock_container_client = blob_service_client.get_container_client(container_name)
        # Ensure container exists (optional, depends if created elsewhere)
        try:
            await lock_container_client.create_container()
            logging.info(f"Lease Lock: Container '{container_name}' created.")
        except HttpResponseError as ex:
            if ex.status_code == 409: # Conflict - container already exists
                logging.debug(f"Lease Lock: Container '{container_name}' already exists.")
            else:
                raise # Re-raise other errors

        lock_blob_client = lock_container_client.get_blob_client(blob_name)
        logging.info(f"Lease Lock Debug: Type of lock_blob_client: {type(lock_blob_client)}")

        # 1. Try to create the blob if it doesn't exist (atomic operation)
        try:
            # Use if_none_match='*' to ensure it only uploads if blob doesn't exist
            await lock_blob_client.upload_blob(b"", overwrite=False, if_none_match='*')
            logging.info(f"Lease Lock: Created lock blob '{blob_name}'.")
        except HttpResponseError as ex:
            if ex.status_code == 409 or ex.status_code == 412: # Conflict or Precondition Failed - blob already exists
                logging.debug(f"Lease Lock: Lock blob '{blob_name}' already exists.")
            else:
                 logging.error(f"Lease Lock: Error checking/creating lock blob '{blob_name}': {ex}", exc_info=True)
                 return None # Cannot proceed if blob check/creation fails unexpectedly

        # 2. Attempt to acquire the lease using BlobLeaseClient
        from azure.storage.blob.aio import BlobLeaseClient
        lease_client = BlobLeaseClient(client=lock_blob_client)
        logging.info(f"Lease Lock Debug: Created lease client of type {type(lease_client)}")
        
        try:
            # Acquire the lease using the lease client
            lease = await lease_client.acquire(lease_duration=lease_duration)
            logging.info(f"Lease Lock: Successfully acquired lease '{lease}' on '{blob_name}'.")
            return lease # Return the lease ID
        except AttributeError as attr_err:
            # Handle AttributeError in case the acquire method is not found
            logging.error(f"Lease Lock: AttributeError when acquiring lease: {attr_err}. This might be a SDK version issue.")
            # Fallback attempt to use blob client directly
            try:
                lease = await lock_blob_client.acquire_lease(lease_duration=lease_duration)
                logging.info(f"Lease Lock: Successfully acquired lease '{lease.id}' using fallback method.")
                return lease.id # Return the lease ID
            except AttributeError as fallback_err:
                logging.error(f"Lease Lock: Fallback method also failed with AttributeError: {fallback_err}")
                return None
            except Exception as fallback_ex:
                logging.error(f"Lease Lock: Fallback attempt failed: {fallback_ex}")
                return None

    except HttpResponseError as ex:
        if ex.status_code == 409: # Conflict - Lease already present
            logging.warning(f"Lease Lock: Failed to acquire lease on '{blob_name}'. Already locked by another instance.")
            return None
        else:
            logging.error(f"Lease Lock: HTTP error acquiring lease on '{blob_name}': {ex}", exc_info=True)
            return None
    except Exception as e:
        logging.error(f"Lease Lock: Unexpected error acquiring lease on '{blob_name}': {e}", exc_info=True)
        return None


async def release_blob_lease(blob_service_client: BlobServiceClient, container_name: str, blob_name: str, lease_id: str) -> None:
    """Releases a lease on a blob."""
    if not blob_service_client:
        logging.error(f"Lease Lock: Blob service client not available for releasing lease on {blob_name}.")
        return
    if not lease_id:
        logging.error(f"Lease Lock: No lease ID provided for releasing lease on {blob_name}.")
        return
    
    try:
        # Get the blob client first
        lock_blob_client = blob_service_client.get_blob_client(container_name, blob_name)
        logging.info(f"Lease Lock Debug: Type of lock_blob_client: {type(lock_blob_client)}")
        
        # Create a BlobLeaseClient from the blob client
        from azure.storage.blob.aio import BlobLeaseClient
        lease_client = BlobLeaseClient(client=lock_blob_client, lease_id=lease_id)
        logging.info(f"Lease Lock Debug: Created lease client of type {type(lease_client)}")
        
        try:
            # Break the lease using the lease client
            await lease_client.break_lease()
            logging.info(f"Lease Lock: Successfully released lease '{lease_id}' on '{blob_name}'.")
        except AttributeError as attr_err:
            # Handle AttributeError in case the lease break method is not found
            logging.error(f"Lease Lock: AttributeError when breaking lease: {attr_err}. This might be a SDK version issue.")
            # Fallback attempt to use blob client directly
            try:
                await lock_blob_client.break_lease(lease_id=lease_id)
                logging.info(f"Lease Lock: Successfully broke lease using fallback method.")
            except AttributeError as fallback_err:
                logging.error(f"Lease Lock: Fallback method also failed with AttributeError: {fallback_err}")
            except Exception as fallback_ex:
                logging.error(f"Lease Lock: Fallback attempt failed: {fallback_ex}")
        
        # Optional: Delete the lock blob after releasing the lease if desired
        # try:
        #     await lock_blob_client.delete_blob(lease_id=lease_id) # Must provide lease ID if just released
        #     logging.info(f"Lease Lock: Deleted lock blob '{blob_name}'.")
        # except Exception as del_err:
        #     logging.warning(f"Lease Lock: Failed to delete lock blob '{blob_name}' after release: {del_err}")
    except HttpResponseError as ex:
        # Don't worry too much if release fails (e.g., blob deleted, lease expired), but log it.
        logging.warning(f"Lease Lock: HTTP error releasing lease '{lease_id}' on '{blob_name}': {ex}", exc_info=True)
    except Exception as e:
        logging.error(f"Lease Lock: Unexpected error releasing lease '{lease_id}' on '{blob_name}': {e}", exc_info=True)


async def get_sharepoint_metadata(drive_id: str, item_id: str, access_token: str) -> Dict[str, Any]:
    """
    Fetches SharePoint metadata for a DriveItem via Microsoft Graph API.
    
    Args:
        drive_id: The ID of the drive containing the item
        item_id: The ID of the drive item
        access_token: Microsoft Graph API access token
        
    Returns:
        Dict containing the SharePoint metadata fields if available, empty dict otherwise
    """
    metadata_url = f"{GRAPH_API_ENDPOINT}/drives/{drive_id}/items/{item_id}?$expand=listItem($expand=fields)"
    sharepoint_fields = {}
    
    try:
        logging.info(f"Fetching SharePoint metadata for item {item_id} in drive {drive_id}")
        async with httpx.AsyncClient() as client:
            response = await client.get(
                metadata_url,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=30
            )
            
            if response.status_code == 200:
                item_data = response.json()
                # Extract ListItem fields if available
                if "listItem" in item_data and "fields" in item_data["listItem"]:
                    sharepoint_fields = item_data["listItem"]["fields"]
                    logging.info(f"Retrieved SharePoint metadata: {len(sharepoint_fields)} fields")
                else:
                    logging.info("No ListItem fields found in the item data")
                    logging.debug(f"Item data keys: {list(item_data.keys())}")
            else:
                logging.warning(f"Failed to retrieve metadata: {response.status_code}")
                if response.text:
                    logging.warning(f"Error response: {response.text[:500]}")
    except Exception as e:
        logging.error(f"Error fetching SharePoint metadata: {str(e)}", exc_info=True)
    
    # Add detailed logging about what's being returned
    if sharepoint_fields:
        logging.info(f"Metadata Fetch: Returning {len(sharepoint_fields)} SharePoint fields. Keys: {list(sharepoint_fields.keys())}")
    else:
        # This branch runs if fields weren't found in response OR if an exception occurred
        logging.info("Metadata Fetch: Returning empty SharePoint metadata dictionary (no fields found or error occurred).")
    
    return sharepoint_fields


# --- Helper Function: User ID Retrieval ---
async def get_user_id_from_email(email: str, access_token: str) -> Optional[str]:
    """
    Retrieves the Azure AD user ID for a given email address via Microsoft Graph API.
    
    Args:
        email: The email address of the user
        access_token: Microsoft Graph API access token
        
    Returns:
        str: User ID if exactly one user is found, None otherwise
    """
    if not email or not access_token:
        logging.warning(f"User ID Lookup: Missing required parameters - Email: {'present' if email else 'missing'}, Token: {'present' if access_token else 'missing'}")
        return None
    
    try:
        # URL encode the email properly for the filter query
        encoded_email = urllib.parse.quote(email)
        user_url = f"{GRAPH_API_ENDPOINT}/users?$filter=mail eq '{encoded_email}'&$select=id"
        
        logging.info(f"User ID Lookup: Querying Graph API for user with email: {email}")
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                user_url,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=30
            )
            
            if response.status_code == 200:
                user_data = response.json()
                users = user_data.get('value', [])
                
                if len(users) == 1:
                    user_id = users[0].get('id')
                    logging.info(f"User ID Lookup: Successfully retrieved ID for email {email}: {user_id}")
                    return user_id
                elif len(users) == 0:
                    logging.warning(f"User ID Lookup: No user found with email {email}")
                    return None
                else:
                    logging.warning(f"User ID Lookup: Multiple users ({len(users)}) found with email {email}")
                    return None
            else:
                logging.error(f"User ID Lookup: Failed with status code {response.status_code}")
                if response.text:
                    logging.error(f"User ID Lookup: Error response: {response.text[:500]}")
                return None
                
    except Exception as e:
        logging.error(f"User ID Lookup: Error retrieving user ID for email {email}: {str(e)}", exc_info=True)
        return None


# --- Helper Function: Planner Tasks Retrieval ---
async def get_employee_planner_tasks(plan_id: Optional[str], user_id: str, access_token: str) -> List[Dict[str, Any]]:
    """
    Retrieves Planner tasks assigned to a user from Microsoft Graph API.
    
    Args:
        plan_id: Optional Plan ID to filter tasks to a specific plan
        user_id: The Azure AD user ID
        access_token: Microsoft Graph API access token
        
    Returns:
        List[Dict[str, Any]]: List of task dictionaries containing details
    """
    if not user_id or not access_token:
        logging.warning(f"Planner Tasks: Missing required parameters - User ID: {'present' if user_id else 'missing'}, Token: {'present' if access_token else 'missing'}")
        return []
    
    # Early return if plan_id is not provided
    if not plan_id:
        logging.warning("Planner Tasks: Plan ID is required. Skipping task retrieval.")
        return []
    
    try:
        # Construct API URL for the specific plan and expand details and assignments
        tasks_url = f"{GRAPH_API_ENDPOINT}/planner/plans/{plan_id}/tasks?$expand=details,assignments"
        logging.info(f"Planner Tasks: Querying Graph API for tasks in plan: {plan_id}")
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                tasks_url,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=30
            )
            
            if response.status_code == 200:
                tasks_data = response.json()
                tasks = tasks_data.get('value', [])
                
                # Process and filter tasks
                formatted_tasks = []
                
                for task in tasks:
                    # Get assignments dictionary, default to empty
                    assignments = task.get('assignments', {})
                    
                    # Only include tasks where user_id is a direct key in the assignments dictionary
                    if user_id in assignments:
                        task_details = task.get('details', {})
                        
                        formatted_task = {
                            "id": task.get('id'),
                            "title": task.get('title'),
                            "percentComplete": task.get('percentComplete'),
                            "createdDateTime": task.get('createdDateTime'),
                            "dueDateTime": task.get('dueDateTime'),
                            "completedDateTime": task.get('completedDateTime'),
                            "description": task_details.get('description', ''),
                            "priority": task.get('priority'),
                            "planTitle": "",  # Will be populated below if possible
                            "bucketName": ""  # Will be populated below if possible
                        }
                        
                        # Add task to the formatted list
                        formatted_tasks.append(formatted_task)
                
                logging.info(f"Planner Tasks: Retrieved {len(formatted_tasks)} tasks for user {user_id} in plan {plan_id}")
                
                # Try to enrich with plan and bucket names if tasks exist
                if formatted_tasks:
                    try:
                        # Get a sample task to fetch plan and bucket details
                        sample_task = tasks[0]
                        sample_plan_id = sample_task.get('planId')
                        bucket_id = sample_task.get('bucketId')
                        
                        if sample_plan_id:
                            # Get plan details
                            plan_url = f"{GRAPH_API_ENDPOINT}/planner/plans/{sample_plan_id}"
                            plan_response = await client.get(
                                plan_url,
                                headers={"Authorization": f"Bearer {access_token}"},
                                timeout=30
                            )
                            
                            if plan_response.status_code == 200:
                                plan_data = plan_response.json()
                                plan_title = plan_data.get('title', '')
                                
                                # Update all tasks with the plan title
                                for task in formatted_tasks:
                                    task["planTitle"] = plan_title
                                
                                logging.info(f"Planner Tasks: Enriched tasks with plan title: {plan_title}")
                        
                        if bucket_id:
                            # Get bucket details
                            bucket_url = f"{GRAPH_API_ENDPOINT}/planner/buckets/{bucket_id}"
                            bucket_response = await client.get(
                                bucket_url,
                                headers={"Authorization": f"Bearer {access_token}"},
                                timeout=30
                            )
                            
                            if bucket_response.status_code == 200:
                                bucket_data = bucket_response.json()
                                bucket_name = bucket_data.get('name', '')
                                
                                # Update tasks in this bucket
                                for task in formatted_tasks:
                                    if sample_task.get('bucketId') == bucket_id:
                                        task["bucketName"] = bucket_name
                                
                                logging.info(f"Planner Tasks: Enriched tasks with bucket name: {bucket_name}")
                    
                    except Exception as enrich_err:
                        logging.warning(f"Planner Tasks: Error enriching tasks with plan/bucket details: {str(enrich_err)}")
                        # Continue without enrichment - we still have the basic task data
                
                return formatted_tasks
            else:
                logging.error(f"Planner Tasks: Failed with status code {response.status_code}")
                if response.text:
                    logging.error(f"Planner Tasks: Error response: {response.text[:500]}")
                return []
                
    except Exception as e:
        logging.error(f"Planner Tasks: Error retrieving tasks for user {user_id}: {str(e)}", exc_info=True)
        return []


async def generate_dynamic_prompt(
    content_sample: str, 
    file_type: str,
    sharepoint_custom_metadata: Optional[Dict[str, Any]] = None,
    employee_planner_tasks: Optional[List[Dict[str, Any]]] = None
) -> str:
    """
    Generate a dynamic prompt for Claude based on document content.
    
    Args:
        content_sample: The document content in markdown format
        file_type: The file extension (type) of the document
        sharepoint_custom_metadata: Optional SharePoint custom metadata for context enhancement
        employee_planner_tasks: Optional list of employee's planner tasks for context
        
    Returns:
        str: Generated prompt for Claude or error indicator
    """
    if not gemini_model:
        error_msg = "GEMINI_MODEL_NOT_INITIALIZED"
        logging.error(f"Gemini: {error_msg}")
        return error_msg
    
    try:
        # Prepare a sample of the markdown content
        content_sample_length = 2000
        content_sample = content_sample[:content_sample_length] + "..." if len(content_sample) > content_sample_length else content_sample
        
        # Generate metadata section if available
        metadata_section = ""
        if sharepoint_custom_metadata and len(sharepoint_custom_metadata) > 0:
            metadata_section = f"""
SHAREPOINT CUSTOM METADATA:
{json.dumps(sharepoint_custom_metadata, indent=2)}

Use this metadata (which may include author, dates, categories, or custom fields) to enhance your understanding of the document context.
"""

        # Generate planner tasks section if available
        planner_tasks_section = ""
        if employee_planner_tasks and len(employee_planner_tasks) > 0:
            planner_tasks_section = f"""
EMPLOYEE PLANNER TASKS:
{json.dumps(employee_planner_tasks, indent=2)}

These are the current planner tasks assigned to the document author/owner. Use this information to understand the employee's current projects, priorities, and deadlines when analyzing the document.
"""
        
        # Create the meta-prompt for Gemini (removed reference to Web Search Context)
        meta_prompt = f"""
            Prompt Designer Instructions:

            Your task is to generate a highly specific, context-aware prompt that will guide a final AI model to analyze a single working document (Word, Excel, PowerPoint, or PDF). This final prompt must instruct the AI to produce an output that is structured, objective, and sharply aligned with the document type, its content, and the full set of provided metadata (e.g., deadline, project context, review status, feedback history, document objective).

            You are not creating a one-size-fits-all prompt. You are designing a precise and tailored instruction set that forces the final AI to analyze the document in depth and in context. Your output must ensure the final AI adapts its language, scoring criteria, and recommendations to suit the specific nature and function of the document.

            Requirements for the Final Prompt You Will Generate:
            Output Format:

            Instruct the final AI to output only a structured JSON object.

            The JSON must start with {{ and end with }} — absolutely no text before or after.

            No markdown, no formatting, no headings. Just raw JSON.

            Top-Level JSON Keys (MANDATORY STRUCTURE):

            "descriptive_summary": 5–7 objective sentences describing the document's purpose, content, and key points. Must remain neutral and not evaluative.

            "technical_summary": 3–5 sentences critically evaluating the quality and effectiveness of the document, based on its type and intended function.

            "technical_scores": Exactly 5 metrics scored on a 1–10 scale, with brief, targeted justifications. These metrics must be document-type-specific (see below).

            "recommendations": 2–3 clear, actionable suggestions to improve the document. These should reflect both its type and the project/review context found in the metadata.

            Scoring Metric Guidelines (adapt based on document type):

            Excel/Data-Heavy (.xlsx):
            "Data Accuracy/Integrity", "Formula Complexity/Efficiency", "Clarity of Presentation (Charts/Tables)", "Structural Organization", "Actionability of Data"

            Word/Analytical Reports (.docx):
            "Logical Structure/Flow", "Clarity of Argument", "Evidence Quality/Support", "Depth of Analysis", "Writing Style/Professionalism"

            PowerPoint/Presentations (.pptx):
            "Visual Design/Appeal", "Clarity of Key Messages", "Slide Structure/Flow", "Audience Appropriateness", "Information Density"

            PDF or Mixed-Type Documents:
            Instruct the AI to choose 4–5 metrics most relevant to the content and context — e.g., formatting quality, clarity, usability, analytical depth, etc.

            Alignment with Metadata Context:

            Instruct the final AI to incorporate metadata into all evaluative components — especially the "technical_summary" and "recommendations".

            If a document is part of a draft cycle, under review, or linked to a specific deadline or objective, this must directly influence the tone and content of the evaluation.

            Scoring should reflect expectations based on the document's stage, role, and use-case in the project.

            Enforcement of Strict Format Compliance:

            Reinforce that any deviation from the defined JSON structure, or inclusion of additional text, invalidates the output.

            Your Output:
            Generate a targeted and context-aware prompt that includes all the above constraints, tailored to the specific document type and the full metadata. It should push the final AI to deliver a precise, useful, and context-grounded evaluation, not a generic assessment.
        """
        
        logging.info(f"Gemini: Generating dynamic prompt for {file_type} document with custom metadata: {len(sharepoint_custom_metadata) if sharepoint_custom_metadata else 0} fields and {len(employee_planner_tasks) if employee_planner_tasks else 0} planner tasks")
        
        response = await gemini_model.generate_content_async(meta_prompt)
        
        if not response or not hasattr(response, 'text'):
            logging.error("Gemini: Failed to get valid response")
            return "GEMINI_GENERATION_FAILED: Invalid response structure"
        
        dynamic_prompt = response.text.strip()
        
        logging.info(f"Gemini: Successfully generated dynamic prompt ({len(dynamic_prompt)} characters)")
        logging.debug(f"Gemini: Dynamic prompt sample: {dynamic_prompt[:200]}...")
        
        return dynamic_prompt
        
    except Exception as e:
        error_msg = f"GEMINI_GENERATION_FAILED: {str(e)}"
        logging.error(f"Gemini Error: {error_msg}", exc_info=True)
        return error_msg


# --- Gemini Analysis Function ---
async def analyze_with_gemini_dynamic(
    dynamic_prompt: str,
    content: str, 
    file_name: str,
    sharepoint_custom_metadata: Optional[Dict[str, Any]] = None,
    employee_planner_tasks: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """ 
    Analyzes document content using Gemini with dynamic prompt. 
    
    Args:
        dynamic_prompt: Generated dynamic prompt from first Gemini call
        content: The document content in markdown format
        file_name: Name of the file being analyzed
        sharepoint_custom_metadata: Optional SharePoint custom metadata for context enhancement
        employee_planner_tasks: Optional list of employee's planner tasks for context
    
    Returns:
        Dict containing analysis results or error information
    """
    if not gemini_analysis_model:
        error_msg = "Gemini analysis model not initialized"
        logging.error(error_msg)
        return {
            "status": "initialization_error",
            "error": error_msg
        }

    try:
        content_to_analyze = content
        if len(content) > MAX_CHARS_FOR_ANALYSIS:
            logging.warning(f"Gemini Analysis: Truncating content from {len(content)} to {MAX_CHARS_FOR_ANALYSIS} characters.")
            content_to_analyze = content[:MAX_CHARS_FOR_ANALYSIS]

        logging.info(f"Gemini Analysis: Sending {len(content_to_analyze)} characters for analysis with "
                     f"{len(sharepoint_custom_metadata) if sharepoint_custom_metadata else 0} custom metadata fields and "
                     f"{len(employee_planner_tasks) if employee_planner_tasks else 0} planner tasks")

        # Generate metadata section if available
        metadata_section = ""
        if sharepoint_custom_metadata and len(sharepoint_custom_metadata) > 0:
            metadata_section = f"""
SHAREPOINT CUSTOM METADATA:
{json.dumps(sharepoint_custom_metadata, indent=2)}
"""

        # Generate planner tasks section if available
        planner_tasks_section = ""
        if employee_planner_tasks and len(employee_planner_tasks) > 0:
            planner_tasks_section = f"""
EMPLOYEE PLANNER TASKS:
{json.dumps(employee_planner_tasks, indent=2)}

Consider the author's current tasks and priorities when analyzing this document.
"""
        
        # Construct the complete analysis prompt
        final_analysis_prompt = f"""
{metadata_section}
{planner_tasks_section}

DOCUMENT CONTENT:
{content_to_analyze}

{dynamic_prompt}
"""

        # Configure Gemini to return JSON
        generation_config = GenerationConfig(
            response_mime_type="application/json"
        )
        
        logging.info("Gemini Analysis: Sending analysis request to Gemini")
        
        # Send request to Gemini
        response = await gemini_analysis_model.generate_content_async(
            final_analysis_prompt, 
            generation_config=generation_config
        )
        
        # Check if response was successful
        if not response or not hasattr(response, 'candidates') or not response.candidates:
            logging.error("Gemini Analysis: Empty or invalid response received")
            return {
                "status": "gemini_response_error",
                "error": "Empty or invalid response from Gemini",
                "raw_response": str(response) if response else "None"
            }
            
        # Check finish reason
        finish_reason = response.candidates[0].finish_reason
        if finish_reason != "STOP":
            logging.warning(f"Gemini Analysis: Response didn't complete normally. Finish reason: {finish_reason}")
            
        # Extract response text
        if not hasattr(response, 'text'):
            logging.error("Gemini Analysis: Response missing text attribute")
            return {
                "status": "gemini_format_error",
                "error": "Response missing text attribute",
                "raw_response": str(response)
            }
            
        analysis_text = response.text.strip()
        logging.debug(f"Gemini raw response text: {analysis_text[:500]}...")
        
        try:
            # Attempt to parse as JSON
            json_match = json.loads(analysis_text)
            logging.info("Gemini Analysis: Successfully parsed JSON response.")
            
            # Validate required keys
            required_keys = ["summary", "technical_scores", "content_analysis", "recommendations"]
            if all(key in json_match for key in required_keys):
                # Remove metadata if present - we don't want it
                if "metadata" in json_match:
                    logging.warning("Gemini Analysis: Removing 'metadata' key found in response despite instructions not to include it")
                    del json_match["metadata"]
                return json_match
            else:
                missing_keys = [key for key in required_keys if key not in json_match]
                logging.error(f"Gemini Analysis: Parsed JSON missing required keys: {missing_keys}. Found: {list(json_match.keys())}")
                return {
                    "status": "gemini_format_error",
                    "error": f"Parsed JSON missing required keys: {missing_keys}",
                    "analysis_text": analysis_text, # Keep original text for debugging
                    "parsed_keys": list(json_match.keys())
                }
                
        except json.JSONDecodeError as json_err:
            logging.error(f"Gemini Analysis: Failed to parse response as JSON: {json_err}")
            return {
                "status": "gemini_json_error",
                "error": f"Failed to parse Gemini response as JSON: {str(json_err)}",
                "analysis_text": analysis_text,
                "raw_response": analysis_text
            }
            
    except google_api_exceptions.ResourceExhausted as rle:
        error_msg = f"Gemini Analysis: Rate limit error: {str(rle)}"
        logging.error(error_msg)
        return {
            "status": "gemini_rate_limit_error",
            "error": error_msg
        }
    except google_api_exceptions.GoogleAPIError as api_err:
        error_msg = f"Gemini Analysis: API error: {str(api_err)}"
        logging.error(error_msg)
        return {
            "status": "gemini_api_error",
            "error": error_msg
        }
    except Exception as e:
        error_msg = f"Gemini Analysis: Unexpected error: {str(e)}"
        logging.error(error_msg, exc_info=True)
        return {
            "status": "gemini_unexpected_error",
            "error": error_msg
        }


async def get_group_id_for_site(site_id: str, access_token: str) -> Optional[str]:
    """
    Find the Microsoft 365 Group ID associated with a SharePoint site.
    
    Args:
        site_id: The SharePoint site ID
        access_token: Microsoft Graph API access token
        
    Returns:
        Optional[str]: The Group ID if found, otherwise None
    """
    if not site_id or not access_token:
        logging.warning("Group ID: Missing required parameters - Site ID: " + 
                        f"{'present' if site_id else 'missing'}, Token: {'present' if access_token else 'missing'}")
        return None
    
    try:
        # Ensure site_id is properly encoded for URL
        encoded_site_id = urllib.parse.quote(site_id)
        graph_url = f"{GRAPH_API_ENDPOINT}/sites/{encoded_site_id}?$select=id&$expand=group($select=id)"
        
        logging.info(f"Group ID: Querying Graph API for group associated with site: {site_id}")
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                graph_url,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=30
            )
            
            if response.status_code == 200:
                site_data = response.json()
                group = site_data.get('group')
                
                if group and 'id' in group:
                    group_id = group['id']
                    logging.info(f"Group ID: Found group ID '{group_id}' for site '{site_id}'")
                    return group_id
                else:
                    logging.warning(f"Group ID: No group associated with site '{site_id}'")
                    return None
            else:
                logging.error(f"Group ID: Failed with status code {response.status_code}")
                if response.text:
                    logging.error(f"Group ID: Error response: {response.text[:500]}")
                return None
                
    except Exception as e:
        logging.error(f"Group ID: Error retrieving group ID for site '{site_id}': {str(e)}", exc_info=True)
        return None


async def get_plan_id_from_group(group_id: str, access_token: str) -> Optional[str]:
    """
    Find a Planner Plan ID associated with a Microsoft 365 Group.
    
    Args:
        group_id: The Microsoft 365 Group ID
        access_token: Microsoft Graph API access token
        
    Returns:
        Optional[str]: The Plan ID if found, otherwise None
    """
    if not group_id or not access_token:
        logging.warning("Plan ID: Missing required parameters - Group ID: " + 
                        f"{'present' if group_id else 'missing'}, Token: {'present' if access_token else 'missing'}")
        return None
    
    try:
        graph_url = f"{GRAPH_API_ENDPOINT}/groups/{group_id}/planner/plans?$select=id,title"
        
        logging.info(f"Plan ID: Querying Graph API for plans in group: {group_id}")
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                graph_url,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=30
            )
            
            if response.status_code == 200:
                plan_data = response.json()
                plans = plan_data.get('value', [])
                
                if not plans:
                    logging.warning(f"Plan ID: No plans found for group '{group_id}'")
                    return None
                    
                if len(plans) == 1:
                    plan_id = plans[0].get('id')
                    plan_title = plans[0].get('title', 'Unknown')
                    logging.info(f"Plan ID: Found single plan '{plan_title}' with ID '{plan_id}' for group '{group_id}'")
                    return plan_id
                    
                # If multiple plans, log them and select the first one
                # This is a simplified selection strategy
                logging.info(f"Plan ID: Found {len(plans)} plans for group '{group_id}':")
                for idx, plan in enumerate(plans):
                    plan_id = plan.get('id')
                    plan_title = plan.get('title', 'Unknown')
                    logging.info(f"  {idx+1}. Plan '{plan_title}' with ID '{plan_id}'")
                
                # Default to first plan
                selected_plan = plans[0]
                plan_id = selected_plan.get('id')
                plan_title = selected_plan.get('title', 'Unknown')
                logging.info(f"Plan ID: Selected plan '{plan_title}' with ID '{plan_id}' (first in list)")
                return plan_id
            else:
                logging.error(f"Plan ID: Failed with status code {response.status_code}")
                if response.text:
                    logging.error(f"Plan ID: Error response: {response.text[:500]}")
                return None
                
    except Exception as e:
        logging.error(f"Plan ID: Error retrieving plans for group '{group_id}': {str(e)}", exc_info=True)
        return None


# --- Main Function ---
async def main(msg: func.QueueMessage) -> None:
    """Azure Function entry point for processing queue messages."""
    function_start_time = pendulum.now('UTC')
    message_body = None
    data = None
    tenant_id = "unknown"
    item_id = "unknown"
    file_name = "unknown"
    msg_id = msg.id if hasattr(msg, 'id') else "unknown_msg_id"
    item_metadata = {}
    lease_id = None # <--- Initialize lease_id
    lock_blob_name = None # <--- Initialize lock_blob_name
    sharepoint_fields = {} # Initialize SharePoint metadata
    sharepoint_custom_metadata = {} # Initialize custom SharePoint metadata
    sharepoint_internal_metadata = {} # Initialize internal SharePoint metadata
    employee_user_id = None # Initialize employee user ID
    employee_planner_tasks = [] # Initialize employee planner tasks
    site_id = None # Initialize site ID
    group_id = None # Initialize group ID
    plan_id = None # Initialize plan ID

    logging.info(f"{msg_id}: ----- Function execution started -----")

    try:
        logging.info(f"{msg_id}: Processing queue message at {function_start_time.isoformat()}")
        message_body = msg.get_body().decode('utf-8')
        logging.debug(f"{msg_id}: Message body (sample): {message_body[:500] if message_body else 'None'}")

        try:
            data = json.loads(message_body)
            logging.info(f"{msg_id}: Successfully parsed queue message JSON.")
        except json.JSONDecodeError as json_error:
            logging.error(f"{msg_id}: Failed to parse queue message JSON: {json_error}")
            return

        tenant_id = data.get('tenantId', 'unknown')
        drive_id = data.get('driveId')
        item_id = data.get('itemId', 'unknown')
        file_name = data.get('fileName', 'unknown')
        
        # Extract site ID from the incoming message
        site_id = data.get('siteId')
        if site_id:
            logging.info(f"{msg_id}: Found SharePoint site ID in message: '{site_id}'")
        else:
            logging.info(f"{msg_id}: No SharePoint site ID found in message")
            
        logging.info(f"{msg_id}: Extracted document info - Tenant: '{tenant_id}', Drive: '{drive_id}', Item: '{item_id}', File: '{file_name}'")

        if not all([tenant_id != "unknown", drive_id, item_id != "unknown", file_name != "unknown"]):
            logging.error(f"{msg_id}: Missing required fields (tenantId, driveId, itemId, fileName). Skipping processing.")
            return

        item_metadata = {
            "tenant_id": tenant_id, "drive_id": drive_id, "item_id": item_id,
            "file_name": file_name, "message_id": msg_id,
            "processed_at_utc": function_start_time.to_iso8601_string()
        }
        
        # Add site_id to metadata if available
        if site_id:
            item_metadata["site_id"] = site_id
            
        logging.info(f"{msg_id}: Metadata extraction complete.")

        logging.info(f"{msg_id}: Checking file extension.")
        file_extension = os.path.splitext(file_name.lower())[1] if '.' in file_name else ''
        if file_extension not in SUPPORTED_FILE_EXTENSIONS:
            logging.warning(f"{msg_id}: Unsupported file type: {file_extension}. Skipping processing.")
            await store_result({
                "status": "skipped_unsupported_type",
                "reason": f"Unsupported file type: {file_extension}",
                "supported_types": SUPPORTED_FILE_EXTENSIONS,
                "file_name": file_name, "metadata": item_metadata,
                "timestamp_utc": pendulum.now('UTC').to_iso8601_string()
            }, item_id, tenant_id)
            return

        logging.info(f"{msg_id}: Starting idempotency check.")
        if blob_service_client:
            try:
                already_processed = await check_recent_analysis_exists(
                    blob_service_client, RESULTS_CONTAINER_NAME, tenant_id, item_id, IDEMPOTENCY_WINDOW_MINUTES)
                if already_processed:
                    return
            except Exception as idempotency_err:
                logging.error(f"{msg_id}: Error during idempotency check: {idempotency_err}", exc_info=True)
                logging.warning(f"{msg_id}: Continuing processing despite idempotency check failure.")
        else:
            logging.warning(f"{msg_id}: Blob storage client unavailable - skipping idempotency check")

        logging.info(f"{msg_id}: Checking required client initializations.")
        if not doc_intelligence_client or not gemini_model or not gemini_analysis_model:
            error_parts = []
            if not doc_intelligence_client: error_parts.append("Document Intelligence client missing.")
            if not gemini_model: error_parts.append("Gemini prompt model missing.")
            if not gemini_analysis_model: error_parts.append("Gemini analysis model missing.")
            error_msg = f"{msg_id}: Prerequisite client(s) not initialized. " + " ".join(error_parts)
            logging.error(error_msg)
            await store_result({
                "status": "initialization_error", "error": error_msg,
                "file_name": file_name, "metadata": item_metadata,
                "timestamp_utc": pendulum.now('UTC').to_iso8601_string()
            }, item_id, tenant_id)
            return

        logging.info(f"{msg_id}: Attempting to get Graph token.")
        access_token = await get_graph_token()
        if not access_token:
            error_msg = f"{msg_id}: Failed to obtain Graph token."
            await store_result({
                "status": "authentication_error", "error": error_msg,
                "file_name": file_name, "metadata": item_metadata,
                "timestamp_utc": pendulum.now('UTC').to_iso8601_string()
            }, item_id, tenant_id)
            return

        # --- Look up Group ID and Plan ID from Site ID (if available) ---
        if site_id and access_token:
            try:
                logging.info(f"{msg_id}: Looking up Group ID for SharePoint site ID: {site_id}")
                group_id = await get_group_id_for_site(site_id, access_token)
                
                if group_id:
                    logging.info(f"{msg_id}: Found Group ID: {group_id}")
                    
                    # Now look up the Plan ID from the Group ID
                    logging.info(f"{msg_id}: Looking up Plan ID for Group ID: {group_id}")
                    plan_id = await get_plan_id_from_group(group_id, access_token)
                    
                    if plan_id:
                        logging.info(f"{msg_id}: Found Plan ID: {plan_id}")
                    else:
                        logging.info(f"{msg_id}: No Plan ID found for Group ID: {group_id}")
                else:
                    logging.info(f"{msg_id}: No Group ID found for SharePoint site ID: {site_id}")
            except Exception as site_lookup_err:
                logging.warning(f"{msg_id}: Error looking up Group/Plan ID from Site ID: {site_lookup_err}")
                # Continue processing even if lookup fails

        # --- Fetch SharePoint Metadata (New Step) ---
        try:
            sharepoint_fields = await get_sharepoint_metadata(drive_id, item_id, access_token)
            
            # Define the set of internal SharePoint field names to exclude from custom metadata
            internal_field_names = {
                "@odata.etag", "id", "ContentType", "Created", "AuthorLookupId", 
                "Modified", "EditorLookupId", "_CheckinComment", "LinkFilenameNoMenu", 
                "LinkFilename", "DocIcon", "FileSizeDisplay", "ItemChildCount", 
                "FolderChildCount", "_ComplianceFlags", "_ComplianceTag", 
                "_ComplianceTagWrittenTime", "_ComplianceTagUserId", "_CommentCount", 
                "_LikeCount", "_DisplayName", "Edit", "_UIVersionString", 
                "ParentVersionStringLookupId", "ParentLeafNameLookupId", "FileRef",
                "FileLeafRef", "Last_x0020_Modified", "Created_x0020_Date",
                "File_x0020_Type", "HTML_x0020_File_x0020_Type"
            }
            
            # Separate custom fields from internal fields
            if sharepoint_fields:
                for key, value in sharepoint_fields.items():
                    if key in internal_field_names:
                        sharepoint_internal_metadata[key] = value
                    else:
                        sharepoint_custom_metadata[key] = value
                
                # Create a sorted version of custom metadata for consistency
                sharepoint_custom_metadata = dict(sorted(sharepoint_custom_metadata.items()))
                
                logging.info(f"{msg_id}: Separated metadata - Custom: {len(sharepoint_custom_metadata)} fields, Internal: {len(sharepoint_internal_metadata)} fields")
                
                # Log the custom fields for verification
                if sharepoint_custom_metadata:
                    logging.info(f"{msg_id}: Custom metadata fields: {list(sharepoint_custom_metadata.keys())}")
            else:
                logging.info(f"{msg_id}: No SharePoint metadata available for this item (received empty dictionary)")
            
            # Add additional verification logging
            logging.info(f"{msg_id}: Type of sharepoint_fields: {type(sharepoint_fields)}")
            logging.info(f"{msg_id}: Value received for sharepoint_fields: {sharepoint_fields}")
        except Exception as metadata_err:
            logging.warning(f"{msg_id}: Error retrieving SharePoint metadata (continuing without it): {metadata_err}")
            # We continue processing even if metadata retrieval fails

        # --- Fetch Employee Planner Tasks (Updated Step) ---
        try:
            author_email = None
            
            # Check directly for the specific Autore_x002f_i field
            if 'Autore_x002f_i' in sharepoint_custom_metadata:
                author_field = sharepoint_custom_metadata['Autore_x002f_i']
                logging.info(f"{msg_id}: Found 'Autore_x002f_i' field with value type: {type(author_field)}")
                
                # Check if the field has the expected structure (list of dictionaries)
                if isinstance(author_field, list) and len(author_field) > 0 and isinstance(author_field[0], dict):
                    # Extract email from the first entry
                    author_email = author_field[0].get('Email')
                    
                    if author_email:
                        logging.info(f"{msg_id}: Successfully extracted author email '{author_email}' from 'Autore_x002f_i[0].Email'")
                    else:
                        logging.warning(f"{msg_id}: 'Email' key not found in 'Autore_x002f_i[0]' dictionary: {author_field[0]}")
                else:
                    logging.warning(f"{msg_id}: 'Autore_x002f_i' field has unexpected structure: {author_field}")
            else:
                logging.warning(f"{msg_id}: 'Autore_x002f_i' field not found in SharePoint metadata. Available fields: {list(sharepoint_custom_metadata.keys())}")
                logging.debug(f"{msg_id}: Full SharePoint custom metadata for debugging: {json.dumps(sharepoint_custom_metadata)}")
            
            if author_email:
                # Get the user ID for the author
                employee_user_id = await get_user_id_from_email(author_email, access_token)
                
                if employee_user_id:
                    logging.info(f"{msg_id}: Retrieved user ID for author: {employee_user_id}")
                    
                    # Get the planner tasks for the author
                    if employee_user_id and plan_id:
                        # If we have both plan_id and employee_user_id, fetch tasks
                        logging.info(f"{msg_id}: Attempting to retrieve planner tasks for user {employee_user_id} in plan {plan_id}")
                        employee_planner_tasks = await get_employee_planner_tasks(
                            plan_id=plan_id,
                            user_id=employee_user_id, 
                            access_token=access_token
                        )
                        
                        if employee_planner_tasks:
                            logging.info(f"{msg_id}: Retrieved {len(employee_planner_tasks)} planner tasks for author")
                        else:
                            logging.info(f"{msg_id}: No planner tasks found for author in plan {plan_id}")
                    elif not plan_id:
                        logging.info(f"{msg_id}: Skipping Planner task fetch because no Plan ID was derived from the Group.")
                else:
                    logging.info(f"{msg_id}: Could not retrieve user ID for author email: {author_email}")
            else:
                logging.info(f"{msg_id}: No author email found in SharePoint metadata")
                
        except Exception as planner_err:
            logging.warning(f"{msg_id}: Error retrieving employee planner tasks (continuing without them): {planner_err}")
            # We continue processing even if planner task retrieval fails
            employee_planner_tasks = []

        # --- Lease Lock Acquisition (NEW STEP) ---
        logging.info(f"{msg_id}: Attempting to acquire processing lock.")
        # Ensure safe_item_id calculation happens here if not already present before this block
        safe_item_id = "".join(c for c in item_id if c.isalnum() or c in ('-', '_')).rstrip()[:100]
        lock_blob_name = f"{tenant_id}/{safe_item_id}.lock" # Define lock blob name

        # Ensure blob_service_client is available before calling acquire_blob_lease
        if not blob_service_client:
            logging.error(f"{msg_id}: Blob service client not initialized. Cannot acquire lock.")
            # Decide handling: maybe store an error or just return
            # Storing a system error might be appropriate
            await store_result({"status": "system_error", "error": "Blob client not available for locking", 
                "file_name": file_name, "metadata": item_metadata, 
                "timestamp_utc": pendulum.now('UTC').to_iso8601_string()
            }, item_id, tenant_id)
            return

        lease_id = await acquire_blob_lease(blob_service_client, LOCK_CONTAINER_NAME, lock_blob_name, DEFAULT_LEASE_DURATION)

        if not lease_id:
            logging.warning(f"{msg_id}: Failed to acquire lock for item '{item_id}'. Another instance is likely processing. Exiting.")
            # Do NOT store an error result here, as this is expected behavior for duplicates.
            return # Exit gracefully

        logging.info(f"{msg_id}: Successfully acquired lock lease ID: {lease_id}")

        try:
            # --- File Content Fetching (Moved after successful lock acquisition) ---
            file_content_bytes = None
            try:
                file_content_bytes = await fetch_file_content(drive_id, item_id, access_token)
                logging.info(f"{msg_id}: Downloaded {file_name}. Size: {len(file_content_bytes) if file_content_bytes else 0} bytes.")
                if not file_content_bytes:
                    # ... (store skipped_empty_file result, BUT ensure lease is released in finally) ...
                    # Consider setting a flag or specific error status to handle in finally
                    final_status = "skipped_empty_file" # Example status
                    logging.warning(f"{msg_id}: File content is empty. Storing result and releasing lock.")
                    await store_result({ 
                        "status": final_status,
                        "reason": "Downloaded file content was empty.",
                        "file_name": file_name, 
                        "metadata": item_metadata,
                        "timestamp_utc": pendulum.now('UTC').to_iso8601_string() 
                    }, item_id, tenant_id)
                    # No 'return' here, let finally handle release
                else:
                    # Proceed with processing only if file content is valid
                    # --- Main Processing Logic (Inside Lock)---
                    markdown_content = None
                    dynamic_prompt = None
                    analysis_result = None
                    final_status = "processing_failed" # Default status

                    try:
                        logging.info(f"{msg_id}: Entering main processing block (lock acquired).")

                        # Step 1: Extract markdown content
                        markdown_content = await extract_markdown_with_doc_intelligence(file_content_bytes)
                        if markdown_content is None or markdown_content.startswith("[Error:"):
                            # Handle extraction failure, set status
                            final_status = "doc_intelligence_error"
                            raise RuntimeError(f"Extraction failed: {markdown_content}")


                        # Step 2: Generate dynamic prompt with Gemini
                        dynamic_prompt = await generate_dynamic_prompt(
                            content_sample=markdown_content,
                            file_type=file_extension,
                            sharepoint_custom_metadata=sharepoint_custom_metadata,
                            employee_planner_tasks=employee_planner_tasks
                        )

                        # Step 3: Analyze with Gemini using dynamic prompt
                        # Make sure dynamic_prompt was successfully generated before proceeding
                        if dynamic_prompt and not dynamic_prompt.startswith("GEMINI_GENERATION_FAILED"):
                            analysis_result = await analyze_with_gemini_dynamic(
                                dynamic_prompt=dynamic_prompt,
                                content=markdown_content,
                                file_name=file_name,
                                sharepoint_custom_metadata=sharepoint_custom_metadata,
                                employee_planner_tasks=employee_planner_tasks
                            )
                            if analysis_result is None or (isinstance(analysis_result, dict) and analysis_result.get("error")):
                                final_status = "gemini_analysis_error"
                                raise RuntimeError(f"Gemini analysis failed: {analysis_result.get('error', 'Unknown Gemini Error')}")
                        else:
                            # Handle case where dynamic prompt generation failed
                            logging.error(f"{msg_id}: Skipping analysis because dynamic prompt generation failed: {dynamic_prompt}")
                            final_status = "gemini_prompt_error"
                            raise RuntimeError(f"Dynamic prompt generation failed: {dynamic_prompt}")


                        # Success Case
                        final_status = "success"
                        logging.info(f"{msg_id}: Processing pipeline completed successfully for {file_name}.")

                        # Step 4: Store Successful Result (before releasing lock)
                        final_result_data = {
                            "status": final_status,
                            "file_name": file_name,
                            "file_size_bytes": len(file_content_bytes) if file_content_bytes else 0,
                            "file_extension": file_extension,
                            "intermediate_markdown_content": markdown_content,
                            "intermediate_markdown_length": len(markdown_content),
                            "intermediate_markdown_sample": markdown_content[:1000] + "..." if len(markdown_content) > 1000 else markdown_content,
                            "dynamic_prompt": dynamic_prompt[:1000] + "..." if len(dynamic_prompt) > 1000 else dynamic_prompt,
                            "analysis_output": analysis_result,
                            "sharepoint_custom_metadata": sharepoint_custom_metadata,
                            "sharepoint_internal_metadata": sharepoint_internal_metadata,
                            "employee_planner_tasks": employee_planner_tasks,
                            "sharepoint_site_id": site_id,  # Include SharePoint site ID if available
                            "m365_group_id": group_id,      # Include Microsoft 365 Group ID if available
                            "planner_plan_id": plan_id,     # Include Planner Plan ID if available
                            "metadata": item_metadata,
                            "processing_duration_seconds": (pendulum.now('UTC') - function_start_time).total_seconds(),
                            "timestamp_utc": pendulum.now('UTC').to_iso8601_string()
                        }
                        logging.info(f"{msg_id}: Attempting to store final successful result.")
                        storage_success = await store_result(final_result_data, item_id, tenant_id)
                        if not storage_success:
                            logging.error(f"{msg_id}: Critical - Failed to store successful results for {file_name}")
                            final_status = "storage_error" # Update status if storage fails
                        processing_time = (pendulum.now('UTC') - function_start_time).total_seconds()
                        logging.info(f"{msg_id}: Completed analysis for {file_name} in {processing_time:.2f} sec.")


                    except Exception as processing_error:
                        # --- Error Handling within Lock ---
                        # Status might already be set (e.g., doc_intelligence_error, gemini_analysis_error)
                        if final_status == "processing_failed": # If not set by specific step failure
                            final_status = "processing_failed" # Keep or refine based on error type

                        logging.error(f"{msg_id}: Error during processing pipeline for {file_name} (lock held): {processing_error}", exc_info=True)
                        # Store Error Result (before releasing lock)
                        error_result_data = {
                            "status": final_status,
                            "error": str(processing_error),
                            "file_name": file_name,
                            "metadata": item_metadata,
                            "sharepoint_custom_metadata": sharepoint_custom_metadata,
                            "sharepoint_internal_metadata": sharepoint_internal_metadata,
                            "employee_planner_tasks": employee_planner_tasks,
                            "sharepoint_site_id": site_id,  # Include SharePoint site ID if available
                            "m365_group_id": group_id,      # Include Microsoft 365 Group ID if available
                            "planner_plan_id": plan_id,     # Include Planner Plan ID if available
                            "intermediate_markdown_sample": (markdown_content[:1000] + "..." if isinstance(markdown_content, str) and len(markdown_content) > 1000 else markdown_content) if markdown_content else None,
                            "dynamic_prompt": dynamic_prompt[:1000] + "..." if dynamic_prompt and len(dynamic_prompt) > 1000 else dynamic_prompt,
                            "raw_gemini_response": analysis_result.get("raw_response", None) if isinstance(analysis_result, dict) and "raw_response" in analysis_result else None,
                            "timestamp_utc": pendulum.now('UTC').to_iso8601_string()
                        }
                        await store_result(error_result_data, item_id, tenant_id)
                        # Do not return here, let finally release the lock
            
            except Exception as fetch_err:
                error_msg = f"Failed to fetch file content: {str(fetch_err)}"
                logging.error(f"{msg_id}: {error_msg}", exc_info=True)
                await store_result({
                    "status": "fetch_error", "error": error_msg,
                    "file_name": file_name, "metadata": item_metadata,
                    "timestamp_utc": pendulum.now('UTC').to_iso8601_string()
                }, item_id, tenant_id)
                # No return here, let finally handle the lease release

        # --- Lock Release (Crucial!) ---
        finally:
            if lease_id and lock_blob_name and blob_service_client: # Check blob_service_client again
                 logging.info(f"{msg_id}: Releasing lock lease '{lease_id}' for blob '{lock_blob_name}'.")
                 try:
                     await release_blob_lease(blob_service_client, LOCK_CONTAINER_NAME, lock_blob_name, lease_id)
                     logging.debug(f"{msg_id}: Successfully released lock lease '{lease_id}'")
                 except Exception as release_err:
                     logging.error(f"{msg_id}: Error releasing lock lease: {release_err}")
                 lease_id = None # Clear lease ID after attempting release
            elif lease_id:
                 logging.error(f"{msg_id}: Cannot release lease '{lease_id}' because blob service client is not available.")

    except Exception as e: # Catch broader exceptions outside the main processing block but within main try
        logging.error(f"{msg_id}: CAUGHT UNEXPECTED ERROR IN TOP LEVEL HANDLER: {type(e).__name__} - {e}", exc_info=True)
        # Attempt to store system error only if essential info is known
        if blob_service_client and tenant_id != "unknown" and item_id != "unknown":
            error_result = {
                "status": "system_error", "error": f"Unhandled Toplevel Error: {str(e)}",
                "file_name": file_name, # Use file_name if available
                "metadata": item_metadata if item_metadata else {"message_id": msg_id, "tenant_id": tenant_id, "item_id": item_id},
                "timestamp_utc": pendulum.now('UTC').to_iso8601_string()
            }
            try:
                 # Check if we hold a lease - release it before storing the error if possible
                 # The lease should ideally be released by the inner finally block already.
                 # But as a safeguard, check again.
                 if lease_id and lock_blob_name:
                      logging.warning(f"{msg_id}: Releasing lock due to top-level error (outer handler).")
                      try:
                          await release_blob_lease(blob_service_client, LOCK_CONTAINER_NAME, lock_blob_name, lease_id)
                          logging.debug(f"{msg_id}: Successfully released lock lease '{lease_id}' in error handler")
                      except Exception as release_err:
                          logging.error(f"{msg_id}: Error releasing lock lease in error handler: {release_err}")
                      lease_id = None # Prevent release in the final finally
                 # Now store the error
                 await store_result(error_result, item_id if item_id != "unknown" else "unknown_item", tenant_id if tenant_id != "unknown" else "unknown_tenant")
            except Exception as store_err:
                 logging.error(f"{msg_id}: Failed to store system error result after toplevel error: {store_err}")
        else:
             logging.error(f"{msg_id}: Cannot store system error due to missing info or Blob Client after toplevel error.")

    finally:
         # Final cleanup & telemetry
         if lease_id and lock_blob_name and blob_service_client:
              logging.warning(f"{msg_id}: Releasing lock in final top-level finally block (indicates potential prior release failure or error before inner finally).")
              try:
                  await release_blob_lease(blob_service_client, LOCK_CONTAINER_NAME, lock_blob_name, lease_id)
                  logging.debug(f"{msg_id}: Successfully released lock lease '{lease_id}' in final cleanup")
              except Exception as final_release_err:
                  logging.error(f"{msg_id}: Error releasing lock lease in final cleanup: {final_release_err}")
              lease_id = None

         end_time = pendulum.now('UTC')
         total_duration = (end_time - function_start_time).total_seconds()
         logging.info(f"{msg_id}: Function execution finished. Duration: {total_duration:.2f} seconds")

# --- End of ProcessQueuedDocument/__init__.py ---
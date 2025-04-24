# OneDriveChangeWebhook/__init__.py

import logging
import json
import os
import urllib.parse
from typing import Dict, Any, Optional, List, Set
import asyncio
import sys

import azure.functions as func
from azure.identity.aio import DefaultAzureCredential # Import the credential class
from azure.storage.blob.aio import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError, ClientAuthenticationError
# We still use GraphServiceClient for authentication token fetching
from msgraph import GraphServiceClient
from msgraph.generated.models.o_data_errors.o_data_error import ODataError
import httpx # Use httpx for all Graph HTTP requests
import aiohttp # Import for explicit cleanup

# --- Configuration ---
GRAPH_API_BASE_URL = "https://graph.microsoft.com/v1.0" # Base URL
GRAPH_SCOPES = ["https://graph.microsoft.com/.default"]
STORAGE_CONNECTION_STRING = os.environ.get("AzureWebJobsStorage")
DELTA_TOKEN_CONTAINER = "deltatokens"

# --- Helper Functions ---

async def get_blob_service_client() -> Optional[BlobServiceClient]:
    """Initialize and return an async blob service client."""
    if not STORAGE_CONNECTION_STRING:
        logging.error("AzureWebJobsStorage connection string is not configured.")
        return None
    try:
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        try:
            # Use try-except for container creation
            async with blob_service_client.get_container_client(DELTA_TOKEN_CONTAINER) as container_client:
                try:
                     await container_client.create_container()
                     logging.info(f"Container '{DELTA_TOKEN_CONTAINER}' created successfully.")
                except ResourceExistsError:
                     logging.info(f"Container '{DELTA_TOKEN_CONTAINER}' already exists.")
        except Exception as container_error:
            logging.warning(f"Could not ensure container '{DELTA_TOKEN_CONTAINER}' exists: {container_error}", exc_info=False)

        return blob_service_client
    except Exception as client_error:
        logging.error(f"Error initializing async blob service client: {client_error}", exc_info=True)
        return None

# Modified to accept credential object
async def get_graph_token(credential: DefaultAzureCredential) -> Optional[str]:
     """Gets a Graph API token using the provided credential object."""
     try:
         logging.info("Attempting to get token using provided credential...")
         token = await credential.get_token(*GRAPH_SCOPES)
         if token and token.token:
              logging.info("Graph token obtained successfully.")
              return token.token
         logging.error("Failed to obtain Graph token (token was empty).")
         return None
     except ClientAuthenticationError as auth_error:
          logging.error(f"Authentication error getting Graph token: {auth_error}", exc_info=True)
          return None
     except Exception as e:
         logging.error(f"Failed to get Graph token: {e}", exc_info=True)
         return None

def extract_identifiers_from_resource_path(resource_path: str) -> Dict[str, str]:
    """
    Parse resource path to extract user, drive or site ID.
    Returns a dictionary with the extracted identifiers and path type.
    
    The function has been enhanced to better handle SharePoint site IDs which 
    often come in formats like "sites/root" or "sites/{hostname},{spsite-guid},{spweb-guid}".
    """
    result = {"path_type": None, "id": None}
    
    if not resource_path:
        return result
        
    # Clean up the path by removing leading/trailing slashes
    clean_path = resource_path.strip('/')
    parts = clean_path.split('/')
    
    if not parts:
        return result
    
    # Determine path type and extract the relevant ID
    if clean_path.startswith("me/"):
        result["path_type"] = "me"
    elif clean_path.startswith("users/"):
        result["path_type"] = "users"
        if len(parts) > 1:
            result["id"] = parts[1]
    elif clean_path.startswith("drives/"):
        result["path_type"] = "drives"
        if len(parts) > 1:
            result["id"] = parts[1]
    elif clean_path.startswith("sites/"):
        result["path_type"] = "sites"
        # SharePoint site paths can be complex, handle multiple formats:
        # - sites/root: Root site
        # - sites/{hostname},{siteid}: Standard site format
        # - sites/{hostname},{siteid},{webid}: Site with specific web
        if len(parts) > 1:
            # Extract the site identifier which may contain commas
            # For SharePoint, we want to preserve the full site identifier, including commas
            # which are used to separate hostname, site guid, and web guid
            site_identifier = parts[1]
            
            # Handle SharePoint site collections that might contain multiple path segments
            if len(parts) > 2 and "," not in site_identifier:
                # This might be a nested site collection path
                # Grab the remaining parts to form a complete site path
                site_identifier = "/".join(parts[1:])
            
            result["id"] = site_identifier
            logging.info(f"Extracted SharePoint site identifier: '{site_identifier}' from path: '{resource_path}'")
    
    return result

def construct_delta_query_url(resource_path: str) -> Optional[str]:
    """
    Construct the correct Graph delta query URL based on the resource path.
    Returns None if the URL cannot be constructed.
    
    Enhanced to better handle SharePoint site paths, including complex site IDs
    with hostname and GUID components.
    """
    identifiers = extract_identifiers_from_resource_path(resource_path)
    path_type = identifiers.get("path_type")
    entity_id = identifiers.get("id")
    
    if not path_type:
        logging.error(f"Could not determine path type from resource path: {resource_path}")
        return None
        
    if path_type == "me":
        logging.error(f"Cannot use /me/ path with application permissions: {resource_path}")
        return None
        
    if not entity_id and path_type != "me":
        logging.error(f"Could not extract ID from resource path: {resource_path}")
        return None
    
    # Construct the appropriate delta URL based on path type
    if path_type == "users":
        logging.info(f"Constructing OneDrive for Business (user) delta URL for user: {entity_id}")
        return f"{GRAPH_API_BASE_URL}/users/{entity_id}/drive/root/delta"
    elif path_type == "drives":
        logging.info(f"Constructing direct drive delta URL for drive: {entity_id}")
        return f"{GRAPH_API_BASE_URL}/drives/{entity_id}/root/delta"
    elif path_type == "sites":
        logging.info(f"Constructing SharePoint site delta URL for site: {entity_id}")
        # For SharePoint sites, we need to ensure we're getting changes from the site drive
        return f"{GRAPH_API_BASE_URL}/sites/{entity_id}/drive/root/delta"
    
    return None

# Modified to accept credential object
async def process_delta_changes(
    credential: DefaultAzureCredential, # Accept credential
    blob_service_client: BlobServiceClient,
    resource_path: str,
    subscription_id: str,
    tenant_id: str
) -> List[Dict]:
    """
    Calls the Graph Delta API using raw HTTP requests to get changes.
    Uses Azure Blob Storage for persistent delta token storage. Async implementation.
    Handles application vs delegated permission scope issues for delta calls.
    Deduplicates items to ensure each file is only processed once.
    
    Enhanced to better handle SharePoint site notifications and improve logging
    for distinguishing between OneDrive and SharePoint content sources.
    
    Returns:
        List[Dict]: List of deduplicated items that need to be queued for processing
    """
    potential_items_to_queue = []  # Temporary collection to store all items before deduplication
    blob_name = f"{subscription_id}.token"
    next_page_url = None
    delta_link_to_save = None
    initial_sync = False
    http_client = None  # Initialize outside try block to use in finally
    
    # Determine content source type (SharePoint or OneDrive)
    source_type = "Unknown"
    if resource_path:
        if resource_path.startswith("sites/"):
            source_type = "SharePoint"
        elif resource_path.startswith("drives/"):
            source_type = "OneDrive"
        elif resource_path.startswith("users/"):
            source_type = "OneDrive for Business"
    
    logging.info(f"Starting delta changes processing for {source_type} source. Resource path: {resource_path}")

    try:
        container_client = blob_service_client.get_container_client(DELTA_TOKEN_CONTAINER)
        blob_client = container_client.get_blob_client(blob=blob_name)
    except Exception as e:
        logging.error(f"Failed to get blob container/client: {e}", exc_info=True)
        return []

    # --- 1. Retrieve persisted delta token/link ---
    try:
        logging.info(f"Attempting to download delta token blob: {blob_name}")
        download_stream = await blob_client.download_blob()
        blob_content = await download_stream.readall()
        next_page_url = blob_content.decode("utf-8").strip()
        if next_page_url:
             logging.info(f"Found persisted delta/next link for subscription {subscription_id}.")
        else:
             logging.info(f"Persisted token file empty for {subscription_id}. Starting fresh.")
             next_page_url = None
             initial_sync = True
    except ResourceNotFoundError:
        logging.info(f"No delta token found for {subscription_id}. Starting fresh.")
        next_page_url = None
        initial_sync = True
    except Exception as e:
        # Only reset token when absolutely necessary
        logging.error(f"Error retrieving delta token: {e}", exc_info=True)
        logging.warning("Attempting to continue with existing token if possible.")
        if not next_page_url:  # Only initialize a fresh sync if we didn't get a valid token
            next_page_url = None
            initial_sync = True

    # --- 2. Loop through Delta API pages ---
    try:
        # Create HTTP client outside the loop to ensure proper lifecycle management
        http_client = httpx.AsyncClient(timeout=60.0)
        current_url = next_page_url
        total_items_found = 0  # Track total items found across all pages before deduplication

        while True:
            delta_data = None
            current_page_items = []
            # Get token using the passed credential object
            access_token = await get_graph_token(credential) # Pass credential
            if not access_token:
                 logging.error("Failed to get Graph token. Cannot process delta.")
                 break

            headers = {"Authorization": f"Bearer {access_token}"}
            request_url = ""

            try:
                if current_url:
                    request_url = current_url
                    logging.info(f"Following delta/next link: {request_url}")
                elif initial_sync:
                    logging.info(f"Constructing initial delta query URL manually for {source_type} content")
                    
                    # Use the dedicated function to construct the URL
                    request_url = construct_delta_query_url(resource_path)
                    
                    if not request_url:
                         logging.warning(f"Could not construct initial delta URL for {source_type}. Skipping delta query.")
                         break
                         
                    logging.info(f"Constructed initial request URL: {request_url}")
                    initial_sync = False
                else:
                     logging.error("Processing loop in invalid state (no current_url and not initial_sync).")
                     break

                # Add prefer header to ensure we only get changes
                headers["Prefer"] = "deltashowremovedasdeleted"
                
                # Make the HTTP GET request
                logging.info(f"Making GET request to: {request_url}")
                response = await http_client.get(request_url, headers=headers)
                logging.info(f"Received response status: {response.status_code} from {request_url}")
                response.raise_for_status()
                delta_data = response.json()

                # --- Process items ---
                current_page_items = delta_data.get('value', [])
                logging.info(f"Processing {len(current_page_items)} items from {source_type} response.")
                total_items_found += len(current_page_items)
                
                for item in current_page_items:
                    if not isinstance(item, dict): 
                        continue
                        
                    item_id = item.get('id')
                    item_name = item.get('name')
                    
                    # Skip if it doesn't have an ID
                    if not item_id:
                        logging.warning("Item missing ID. Skipping.")
                        continue
                        
                    # Check if item is deleted
                    if item.get('deleted'):
                        logging.info(f"{source_type} item deleted: ID='{item_id}'")
                        continue
                        
                    # Skip folders, we only want files
                    if item.get('folder'):
                        logging.info(f"{source_type} folder change: Name='{item_name}', ID='{item_id}'")
                        continue
                        
                    # Only process if it's a file and has actual content changes
                    if item.get('file'):
                        # Extract the drive ID - important for accessing the file later
                        # For SharePoint files, driveId is usually in parentReference
                        parent_ref = item.get('parentReference', {})
                        item_drive_id = parent_ref.get('driveId')
                        
                        # Try to get driveId from various locations depending on the source
                        if not item_drive_id and isinstance(item.get('remoteItem'), dict):
                             # This is a shared or referenced item
                             remote_parent_ref = item['remoteItem'].get('parentReference', {})
                             item_drive_id = remote_parent_ref.get('driveId')
                             # Log additional details for SharePoint items
                             if source_type == "SharePoint" and remote_parent_ref:
                                 site_id = remote_parent_ref.get('siteId')
                                 if site_id:
                                     logging.info(f"SharePoint remote item found. SiteId: {site_id}")
                        
                        # Fallback to driveId at the item level (common in some Graph responses)
                        if not item_drive_id: 
                            item_drive_id = item.get('driveId')
                            
                        # For SharePoint files specifically, try additional paths to find driveId
                        if not item_drive_id and source_type == "SharePoint":
                            # Try from SharePoint-specific fields
                            sharepoint_ids = item.get('sharepointIds', {})
                            if sharepoint_ids:
                                logging.info(f"Found SharePoint IDs for item: {sharepoint_ids}")
                                # SharePoint items might have list ID information we can log
                                list_id = sharepoint_ids.get('listId')
                                list_item_id = sharepoint_ids.get('listItemId')
                                if list_id:
                                    logging.info(f"SharePoint item belongs to list. ListId: {list_id}, ListItemId: {list_item_id}")
                            
                            # For SharePoint, we might have site details in the nested path
                            if parent_ref and 'siteId' in parent_ref:
                                site_id = parent_ref.get('siteId')
                                logging.info(f"Found SharePoint site ID in parent reference: {site_id}")
                                
                                # If we have a site ID but no drive ID, try to log a warning
                                # We might need to enhance the code in the future to fetch the drive ID from the site
                                if site_id and not item_drive_id:
                                    logging.warning(f"Found SharePoint site ID but no driveId for item: {item_id}")
                        
                        if not item_drive_id:
                             logging.warning(f"{source_type} item missing driveId. Skipping. Item ID: {item_id}, Name: {item_name}")
                             continue
                             
                        # Check if file was actually modified (has content changes)
                        last_modified = item.get('lastModifiedDateTime')
                        created_date = item.get('createdDateTime')
                        
                        logging.info(f"Found {source_type} file change: Name='{item_name}', ID='{item_id}', DriveID='{item_drive_id}', LastModified='{last_modified}'")
                        
                        # Add source_type to processing data for downstream processing awareness
                        processing_data = {
                            "tenantId": tenant_id, 
                            "driveId": item_drive_id, 
                            "itemId": item_id, 
                            "fileName": item_name,
                            "lastModified": last_modified,
                            "created": created_date,
                            "sourceType": source_type  # Adding the source type for context
                        }
                        potential_items_to_queue.append(processing_data)

                # --- Check for next/delta link ---
                next_link = delta_data.get('@odata.nextLink')
                delta_link = delta_data.get('@odata.deltaLink')

                if next_link:
                    logging.info(f"Following @odata.nextLink for {source_type} changes...")
                    current_url = next_link
                    delta_link_to_save = None
                elif delta_link:
                    logging.info(f"Found final @odata.deltaLink for {source_type} subscription.")
                    delta_link_to_save = delta_link
                    current_url = None
                    break
                else:
                    logging.warning(f"No nextLink or deltaLink for {source_type}. Processing complete.")
                    delta_link_to_save = None
                    current_url = None
                    if not next_page_url: # Check if we started fresh
                         try:
                              await blob_client.delete_blob(delete_snapshots="include")
                              logging.info(f"Deleted token blob for {subscription_id} (initial sync yielded no delta link).")
                         except ResourceNotFoundError: pass
                         except Exception as del_err: logging.error(f"Error deleting token blob: {del_err}")
                    break

            except httpx.HTTPStatusError as http_err:
                 logging.error(f"HTTP error processing {source_type} delta URL {http_err.request.url!r}: {http_err.response.status_code}", exc_info=False)
                 error_payload = None
                 try:
                      error_payload = http_err.response.json()
                      logging.error(f"Graph error details: {json.dumps(error_payload)}")
                      graph_error_code = error_payload.get("error", {}).get("code")
                      
                      # Only reset the token if Graph explicitly says it's invalid
                      if graph_error_code == 'SyncStateNotFound':
                           logging.warning(f"Delta token invalid/expired (SyncStateNotFound) for {source_type}. Resetting.")
                           current_url = None
                           initial_sync = True
                           delta_link_to_save = None
                           try:
                                await blob_client.delete_blob(delete_snapshots="include")
                                logging.info(f"Deleted invalid delta token blob for {subscription_id}.")
                           except ResourceNotFoundError: pass
                           except Exception as del_err: logging.error(f"Error deleting invalid token: {del_err}")
                           continue
                      else:
                           # For other errors, don't reset automatically
                           logging.error(f"Graph API error for {source_type}: {graph_error_code}. Not resetting token.")
                           break
                 except json.JSONDecodeError: logging.error(f"Could not parse error response body: {http_err.response.text}")
                 except Exception as parse_err: logging.error(f"Error processing error response: {parse_err}")
                 break
            except Exception as e:
                 logging.error(f"Unexpected error in {source_type} delta processing loop: {e}", exc_info=True)
                 break

        # --- Deduplicate items ---
        # Use a dictionary with itemId as key to efficiently track unique items
        unique_items = {}
        skipped_items = 0
        duplicate_item_ids = set()
        
        for item in potential_items_to_queue:
            item_id = item.get("itemId")
            
            # Skip any items with missing itemId
            if not item_id:
                logging.warning(f"Skipping {item.get('sourceType', 'unknown')} item with missing itemId")
                skipped_items += 1
                continue
                
            if item_id in unique_items:
                # Track duplicates for logging
                duplicate_item_ids.add(item_id)
                source = item.get('sourceType', 'unknown')
                logging.info(f"Detected duplicate {source} item: ID='{item_id}', Name='{item.get('fileName', 'unknown')}'")
                
                # If we have a duplicate, keep the more recent version based on lastModified
                if item.get("lastModified") and unique_items[item_id].get("lastModified"):
                    if item["lastModified"] > unique_items[item_id]["lastModified"]:
                        logging.info(f"  Replacing with more recent version of: ID='{item_id}'")
                        unique_items[item_id] = item
            else:
                unique_items[item_id] = item
        
        # Convert dictionary values back to a list
        all_processed_items = list(unique_items.values())
        
        # Log detailed deduplication results
        logging.info(f"Deduplication summary for {source_type} subscription {subscription_id}:")
        logging.info(f"  - Total items found: {total_items_found}")
        logging.info(f"  - Items with missing ID skipped: {skipped_items}")
        logging.info(f"  - Duplicate items removed: {len(duplicate_item_ids)} (unique duplicate IDs: {len(duplicate_item_ids)})")
        logging.info(f"  - Unique items after deduplication: {len(all_processed_items)}")
        
        if len(all_processed_items) > 0:
            logging.info(f"{source_type} items being queued for processing from subscription {subscription_id}:")
            for idx, item in enumerate(all_processed_items):
                source = item.get('sourceType', source_type)
                logging.info(f"  {idx+1}. {source} Item: ID='{item.get('itemId', 'unknown')}', Name='{item.get('fileName', 'unknown')}'")
        
        # --- Persist the final delta link ---
        if delta_link_to_save:
            try:
                await blob_client.upload_blob(delta_link_to_save.encode('utf-8'), overwrite=True)
                logging.info(f"Stored final @odata.deltaLink for {source_type} subscription {subscription_id}.")
            except Exception as e:
                logging.error(f"Error storing final deltaLink for {source_type}: {e}", exc_info=True)
        elif not next_page_url and not all_processed_items and not current_url:
             logging.info(f"Initial {source_type} delta query returned no changes/link for {subscription_id}.")

        logging.info(f"Finished {source_type} delta processing for {subscription_id}. Found {len(all_processed_items)} unique items.")
        return all_processed_items
    
    except Exception as e:
        logging.error(f"Unexpected error in process_delta_changes for {source_type}: {e}", exc_info=True)
        return []
    finally:
        # Ensure httpx client is properly closed in all execution paths
        if http_client:
            try:
                await http_client.aclose()
                logging.info(f"HTTP client for {source_type} processing closed successfully")
            except Exception as close_err:
                logging.error(f"Error closing HTTP client for {source_type}: {close_err}", exc_info=True)


async def main(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    """
    Azure Function entry point for handling Graph Change Notifications via HTTP. (Async)
    
    Enhanced to better identify and handle SharePoint site notifications while
    preserving existing OneDrive functionality.
    """
    logging.info(f'Webhook function triggered. Method: {req.method}')
    start_time = asyncio.get_event_loop().time()
    blob_service_client = None
    credential = None  # Initialize credential variable
    
    try:
        # --- 1. Handle Subscription Validation ---
        validation_token = req.params.get('validationToken')
        if validation_token:
            decoded_token = urllib.parse.unquote(validation_token)
            logging.info(f"Handling Graph validation request. Token: {decoded_token[:20]}...")
            return func.HttpResponse(body=decoded_token, mimetype="text/plain", status_code=200)

        # Initialize credential once per function execution
        credential = DefaultAzureCredential()

        # --- 2. Handle Change Notification ---
        if req.method == 'POST':
            blob_service_client = await get_blob_service_client()
            if not blob_service_client:
                return func.HttpResponse("Internal Server Error: Failed to initialize storage.", status_code=500)

            try:
                notifications = req.get_json()
                logging.info(f"Received notification payload: {json.dumps(notifications, indent=1)}")
            except ValueError as json_error:
                logging.error(f"Invalid JSON received: {json_error}", exc_info=True)
                return func.HttpResponse(status_code=202) # ACK bad json

            if 'value' not in notifications or not isinstance(notifications['value'], list):
                logging.warning("Notification format invalid ('value' missing/not list).")
                return func.HttpResponse(status_code=202) # ACK

            # --- 3. Process Notifications ---
            items_for_queue = []
            
            # Group notifications by subscription and resource to avoid redundant delta calls
            subscription_groups = {}
            sharepoint_count = 0
            onedrive_count = 0
            other_count = 0
            
            for notification in notifications['value']:
                subscription_id = notification.get('subscriptionId')
                resource_path = notification.get('resource')
                tenant_id = notification.get('tenantId')

                if not all([subscription_id, resource_path, tenant_id]):
                    logging.warning(f"Notification missing required fields. Skipping: {notification}")
                    continue
                
                # Identify notification source type for logging
                source_type = "Unknown"
                if resource_path.startswith("sites/"):
                    source_type = "SharePoint"
                    sharepoint_count += 1
                elif resource_path.startswith("drives/"):
                    source_type = "OneDrive"
                    onedrive_count += 1
                elif resource_path.startswith("users/"):
                    source_type = "OneDrive for Business"
                    onedrive_count += 1
                else:
                    other_count += 1
                
                logging.info(f"Received {source_type} notification: Resource={resource_path}, SubscriptionId={subscription_id}")
                
                key = f"{subscription_id}:{resource_path}"
                if key not in subscription_groups:
                    subscription_groups[key] = {
                        "subscription_id": subscription_id,
                        "resource_path": resource_path,
                        "tenant_id": tenant_id,
                        "source_type": source_type
                    }
            
            # Log summary of notification types received
            logging.info(f"Notification summary: {sharepoint_count} SharePoint, {onedrive_count} OneDrive, {other_count} Other")
            
            # Process each subscription/resource group only once
            for group_key, group_data in subscription_groups.items():
                subscription_id = group_data["subscription_id"]
                resource_path = group_data["resource_path"]
                tenant_id = group_data["tenant_id"]
                source_type = group_data["source_type"]
                
                logging.info(f"Processing {source_type} notification group for resource: {resource_path} (Sub ID: {subscription_id}) Tenant: {tenant_id}")
                
                try:
                    # Pass the single credential object
                    notification_items = await process_delta_changes(
                        credential, blob_service_client, resource_path, subscription_id, tenant_id
                    )
                    items_for_queue.extend(notification_items)
                except Exception as delta_error:
                    logging.error(f"Error processing {source_type} delta changes for sub {subscription_id}: {delta_error}", exc_info=True)

            # --- 4. Queue Items for Downstream Processing ---
            if items_for_queue:
                try:
                    # Count by source type
                    source_type_counts = {}
                    for item in items_for_queue:
                        source = item.get("sourceType", "Unknown")
                        source_type_counts[source] = source_type_counts.get(source, 0) + 1
                    
                    # Log queuing summary with source type breakdown
                    logging.info(f"Final queue summary across all subscriptions:")
                    logging.info(f"  - Total subscription/resource groups processed: {len(subscription_groups)}")
                    logging.info(f"  - Total unique items to queue: {len(items_for_queue)}")
                    for source, count in source_type_counts.items():
                        logging.info(f"  - {source} items: {count}")
                    
                    # Create a final set of unique messages to send
                    final_item_dict = {}
                    for item in items_for_queue:
                        item_id = item.get("itemId")
                        if not item_id:
                            logging.warning("Skipping queue item with missing itemId")
                            continue
                            
                        if item_id not in final_item_dict:
                            final_item_dict[item_id] = item
                        else:
                            # If we have duplicate items from different sources (SharePoint vs OneDrive)
                            # log this as it might indicate a configuration issue
                            existing_source = final_item_dict[item_id].get("sourceType", "Unknown")
                            new_source = item.get("sourceType", "Unknown")
                            if existing_source != new_source:
                                logging.warning(f"Found cross-source duplicate for item '{item_id}': {existing_source} vs {new_source}")
                            else:
                                logging.warning(f"Found cross-subscription duplicate for item '{item_id}' - keeping first instance")
                    
                    # Convert the final dictionary to JSON strings
                    messages_to_send = [json.dumps(item_data) for item_data in final_item_dict.values()]
                    
                    # Add specific details about what's being queued
                    logging.info(f"Queueing {len(messages_to_send)} items for processing...")
                    for idx, item_json in enumerate(messages_to_send):
                        try:
                            item = json.loads(item_json)
                            source = item.get("sourceType", "Unknown")
                            logging.info(f"  {idx+1}. Queuing {source} item: ID='{item.get('itemId', 'unknown')}', Name='{item.get('fileName', 'unknown')}', Tenant='{item.get('tenantId', 'unknown')}'")
                        except Exception:
                            logging.warning(f"  {idx+1}. Could not parse queued item JSON for logging")
                            
                    # Send to queue
                    msg.set(messages_to_send)
                    logging.info(f"Successfully queued {len(messages_to_send)} items for processing.")
                except Exception as queue_error:
                    logging.error(f"Error queueing messages: {queue_error}", exc_info=True)
                    return func.HttpResponse("Internal Server Error: Failed to queue items.", status_code=500)

            # --- 5. Acknowledge Receipt to Graph ---
            logging.info("Finished processing notification batch. Responding 202 Accepted.")
            return func.HttpResponse(status_code=202)
        elif req.method == 'GET':
            logging.info("Received GET request. This endpoint only accepts POST requests for notifications or validation.")
            return func.HttpResponse("Method Not Allowed. This endpoint only accepts POST requests.", status_code=405)
        else: # Method not POST or GET
            logging.warning(f"Received unexpected HTTP method: {req.method}.")
            return func.HttpResponse("Method Not Allowed.", status_code=405)

    except Exception as e:
        logging.error(f"Unexpected error in main handler: {e}", exc_info=True)
        return func.HttpResponse("Internal Server Error.", status_code=500)
    finally:
        # Close resources in finally block
        end_time = asyncio.get_event_loop().time()
        logging.info(f"Function execution completed in {end_time - start_time:.4f} seconds")
        
        # Clean up blob service client
        if blob_service_client:
            try:
                await blob_service_client.close()
                logging.info("Blob service client closed.")
            except Exception as close_err:
                logging.error(f"Error closing blob service client: {close_err}", exc_info=True)
                
        # Handle DefaultAzureCredential cleanup
        if credential:
            try:
                # Close the DefaultAzureCredential's internal aiohttp session
                if hasattr(credential, "_client"):
                    client = getattr(credential, "_client")
                    if hasattr(client, "session") and client.session:
                        session = client.session
                        if isinstance(session, aiohttp.ClientSession) and not session.closed:
                            await session.close()
                            logging.info("Closed aiohttp ClientSession from DefaultAzureCredential")
                
                # Close sessions from any chain credentials
                if hasattr(credential, "_successful_credential"):
                    success_cred = getattr(credential, "_successful_credential")
                    if hasattr(success_cred, "_client"):
                        success_client = getattr(success_cred, "_client")
                        if hasattr(success_client, "session") and success_client.session:
                            if isinstance(success_client.session, aiohttp.ClientSession) and not success_client.session.closed:
                                await success_client.session.close()
                                logging.info("Closed aiohttp ClientSession from successful credential in chain")
                
                # For the credentials in the chain
                if hasattr(credential, "_credentials"):
                    for cred in getattr(credential, "_credentials"):
                        if hasattr(cred, "_client"):
                            cred_client = getattr(cred, "_client") 
                            if hasattr(cred_client, "session") and cred_client.session:
                                if isinstance(cred_client.session, aiohttp.ClientSession) and not cred_client.session.closed:
                                    await cred_client.session.close()
                                    logging.info(f"Closed aiohttp ClientSession from credential in chain: {type(cred).__name__}")
                
                # Check for any other unclosed sessions from the loop
                if sys.version_info >= (3, 7):
                    for task in asyncio.all_tasks():
                        if not task.done() and "aiohttp" in str(task):
                            logging.warning(f"Found potentially unclosed aiohttp task: {task}")
                
                logging.info("Completed DefaultAzureCredential cleanup")
            except Exception as cred_err:
                logging.error(f"Error during credential cleanup: {cred_err}", exc_info=True)


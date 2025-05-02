import os
from dotenv import load_dotenv
from azure.storage.blob import (  
    generate_blob_sas,  
    BlobSasPermissions,  
    BlobServiceClient,  
    ContainerClient,  
    BlobClient  
)  
from datetime import datetime, timedelta  
from io import BytesIO

# Load environment variables from .env file
load_dotenv()
  
def download_blob(storage_account_key, container_name, storage_account_name, folder_name, file_name):  
    """Download a file from Azure Data Lake Storage given the container name, folder name, and file name."""  
    try:  
        # Create a BlobServiceClient using the connection string  
        blob_service_client = BlobServiceClient(  
            account_url=f"https://{storage_account_name}.blob.core.windows.net",  
            credential=storage_account_key  
        )  
          
        # Create a BlobClient for the specified blob  
        blob_path = f"{folder_name}/{file_name}" if folder_name else file_name  
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)  
          
        # Download the blob content  
        blob_data = blob_client.download_blob()  
          
        # Read the data into a stream (BytesIO)  
        stream = BytesIO()  
        stream.write(blob_data.readall())  
        stream.seek(0)  # Reset the stream position to the beginning  
          
        return stream  # Returning the stream for further processing  
    except Exception as e:  
        print("An error occurred while downloading the blob:")  
        print(e)  # Print the full error message  
        return None  
  
# Example usage
if __name__ == "__main__":
    # Load sensitive information from environment variables
    folder_name = os.getenv("FOLDER_NAME", "bronce")  # Default to "bronce" if not set
    file_name = os.getenv("FILE_NAME", "CPS_DATOS.json")  # Default to "CPS_DATOS.json" if not set
    container_name = os.getenv("CONTAINER_NAME", "contracts")  # Default to "contracts" if not set
    storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
    storage_account_key = os.getenv("STORAGE_ACCOUNT_KEY")

    # Download the blob stream
    file_stream = download_blob(storage_account_key, container_name, storage_account_name, folder_name, file_name)

    if file_stream:
        # Process the file stream (e.g., read content)
        content = file_stream.read()
        print(content)  # Print the content or process it further
  
def generate_sas_url(storage_account_name, storage_account_key, container_name, blob_name, expiry_minutes=60):  
    """  
    Generate a SAS URL for a file in Azure Data Lake.  
      
    :param storage_account_name: The name of the Azure storage account.  
    :param storage_account_key: The key for the Azure storage account.  
    :param container_name: The name of the container.  
    :param blob_name: The name of the blob (file) including the folder path.  
    :param expiry_minutes: The expiration time for the SAS token in minutes.  
    :return: The SAS URL for the specified blob.  
    """  
    try:  
        # Generate the SAS token  
        sas_token = generate_blob_sas(  
            account_name=storage_account_name,  
            container_name=container_name,  
            blob_name=blob_name,  
            permission=BlobSasPermissions(read=True),  
            expiry=datetime.utcnow() + timedelta(minutes=expiry_minutes),  
            account_key=storage_account_key  # Use the storage account key here  
        )  
          
        # Construct the full SAS URL  
        sas_url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"  
        return sas_url  
    except Exception as e:  
        print("An error occurred while generating SAS URL:")  
        print(e)  # Print the full error message  
        return None  
  

def upload_file_stream(storage_account_name, storage_account_key, container_name, folder_name, file_name, stream):  
    """  
    Upload a file stream to Azure Blob Storage.  
      
    :param storage_account_name: The name of the Azure storage account.  
    :param storage_account_key: The key for the Azure storage account.  
    :param container_name: The name of the container.  
    :param folder_name: The name of the folder within the container.  
    :param file_name: The name of the file to be saved.  
    :param stream: The stream of data to upload.  
    :return: The URL of the uploaded blob or None if an error occurs.  
    """  
    try:  
        # Create a BlobServiceClient  
        blob_service_client = BlobServiceClient(  
            account_url=f"https://{storage_account_name}.blob.core.windows.net",  
            credential=storage_account_key  
        )  
          
        # Get the container client  
        container_client = blob_service_client.get_container_client(container_name)  
          
        # Create the blob client  
        blob_client = container_client.get_blob_client(f"{folder_name}/{file_name}")  
          
        # Upload the file stream  
        blob_client.upload_blob(stream, overwrite=True)  
          
        # Return the URL of the uploaded blob  
        return blob_client.url  
    except Exception as e:  
        print("An error occurred while uploading the file stream:")  
        print(e)  # Print the full error message  
        return None
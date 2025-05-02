import json  
from io import BytesIO  
import io
from datalake_services import download_blob,   upload_file_stream  # Import the download_blob function  
from contracts_agent import process_pages_with_ai_specs, process_contract_final_phase   # Import the processing function  
import asyncio   
import ast   
import os

from dotenv import load_dotenv
def load_prompts(file_path):  
    with open(file_path, 'r', encoding='utf-8') as f:  
        file_content = f.read()  
      
    # Remove any leading or trailing whitespace/newlines  
    file_content = file_content.strip()  
      
    # Evaluate the dictionary using ast.literal_eval  
    prompts = ast.literal_eval(file_content)  
      
    return prompts  
  
# Load prompts at the start of your program  
PROMPTS = load_prompts('prompts.txt')  
load_dotenv()
# Define a mapping for nombre_specs to PROMPTS  
prompt_mapping = {  
    "multas": PROMPTS["process_page_with_ai_multas"],  
     "TipoContrato": PROMPTS["TipoContrato"],  
   # "TipoServicio": PROMPTS["Tipo_Servicio"],  
    "ParteContraparte": PROMPTS["ParteContraparte"],  
  #  "Fecha": PROMPTS["Fecha"],  
  #  "Monto": PROMPTS["Monto"],   
  #  "Penalidades": PROMPTS["Penalidades"],  
  #  "RenovacionAutomatica": PROMPTS["Renovacion_Automatica"],  
  #  "TerminoAnticipado": PROMPTS["Termino_Anticipado"],  
  #  "Exclusividad": PROMPTS["Exclusividad"],  
}  
async def process_contract_ai(lake_key, container_name, folder_name, file_name, nombre_specs, x=1):  
    """  
    Download a JSON file from Azure Data Lake Storage, read its content, and extract metadata from combined pages.  
    
    :param lake_key: The key for accessing the data lake.  
    :param container_name: The name of the container.  
    :param folder_name: The name of the folder (if applicable).  
    :param file_name: The name of the file to download.  
    :param x: The number of pages to combine and process at a time.  
    :return: A dictionary containing metadata from the processed pages or None if an error occurred.  
    """  
    storageaccount = "walmartchiledatalake"  
    # Download the blob  
    stream = download_blob(lake_key, container_name, storageaccount, folder_name, file_name)  
  
    if stream:  
        try:  
            # Read the JSON content from the stream  
            json_content = json.load(stream)  
  
            # Initialize a dictionary to hold metadata for each combined batch of pages  
            metadata_results = {}  
  
            # Process pages in chunks of size x  
            page_items = list(json_content.items())  
            for i in range(0, len(page_items), x):  
                # Get the chunk of pages to combine  
                page_chunk = page_items[i:i + x]  
  
                # Combine the lines from the selected pages into a single string  
                combined_page_string = ' '.join([' '.join(lines) for _, lines in page_chunk])  
                
  
                # Create a string representation of the page range  
                page_range = f"{i + 1}-{min(i + x, len(page_items))}"  # e.g., "1-10"  
  
                 # The page number can be a representation of the combined pages, e.g., "combined-1"  
                page_number = f"combined_pages_{page_range}"  # Pass the range of pages as part of the identifier 
                 
              
                prompt_template = prompt_mapping.get(nombre_specs )  # Fallback to default if not found  
                                
                # Concatenating strings using + operator  
                page_prompt = (  
                    prompt_template + " " +  
                    "This is the content of the pages from a Walmart contract that you need to process. " +  
                    "The content is as follows: " +  
                    combined_page_string  
                )  
                                # Create the page content string  
              
  
                metadata= await process_pages_with_ai_specs(page_number, page_prompt)   
  
                # Store the result in the dictionary  
                metadata_results[f"pages_{page_range}"] = metadata  # Use the page range in the key  
             # Prepare to save the metadata_results as a JSON file  
            total_pages = len(page_items)  
            output_file_name = f"walmart-contract-{nombre_specs}-analytics.json"  
  
            # Convert metadata_results to JSON string  
            metadata_json = json.dumps(metadata_results)  
  
            # Upload the JSON string to Azure Data Lake  
            upload_stream = io.BytesIO(metadata_json.encode('utf-8'))  
            upload_file_stream(storageaccount, lake_key, container_name, folder_name, output_file_name, upload_stream)  
            return metadata_results  
  
        except json.JSONDecodeError as json_err:  
            print("Error decoding JSON:", json_err)  
            return None  
        except Exception as e:  
            print("An error occurred while processing the document lines:", e)  
            return None  
    else:  
        print("Failed to download the blob.")  
        return None  
    

async def process_contract_ai_summary(lake_key, container_name, folder_name, file_name, x=1):  
    """  
    
    """  
    storageaccount = "walmartchiledatalake"  
    # Download the blob  
    stream = download_blob(lake_key, container_name, storageaccount, folder_name, file_name)  
  
    if stream:  
        try:  
          
            json_content = json.load(stream)  
            json_string = json.dumps(json_content, indent=4)  # indent for pretty printing (optional)  
  
            # You can manipulate or log the json_string here if needed  
  
            return json_string  # Return the string representation of JSON content  
  
        except json.JSONDecodeError as json_err:  
            print("Error decoding JSON:", json_err)  
            return None  
        except Exception as e:  
            print("An error occurred while processing the document lines:", e)  
            return None  
    else:  
        print("Failed to download the blob.")  
        return None  
  
# Example usage  
if __name__ == "__main__":  
    storage_account_connection_string = os.getenv("STORAGE_ACCOUNT_CONNECTION_STRING")
    container_name = os.getenv("CONTAINER_NAME", "contracts")  # Default to "contracts" if not set
    folder_name = os.getenv("FOLDER_NAME", "bronce")  # Default to "bronce" if not set
    file_name_output = os.getenv("FILE_NAME_OUTPUT", "walmart-contract-140.json")  # Default value
    file_name = os.getenv("FILE_NAME", "CPS_DATOS.json")  # Default value
    lake_key = os.getenv("LAKE_KEY")

    # Example usage of the function
    # Set the number of pages to combine and process at once  
    number_of_pages_to_process = 5  # Change this value as needed  
    # results = asyncio.run(process_contract_ai_summary(lake_key, container_name, folder_name, file_name, number_of_pages_to_process))
    # Run the async function within asyncio.run()  
    metadata_results = asyncio.run(process_contract_ai(lake_key, container_name, folder_name,
                                                       file_name, "ParteContraparte",number_of_pages_to_process))  
   # asyncio.run(process_contract_final_phase())
    if metadata_results:  
        print("Metadata Results:")  
        print(metadata_results)  
    else:  
        print("No metadata was extracted.")  
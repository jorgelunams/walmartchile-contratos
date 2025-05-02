import asyncio  
import logging  
import os  
import io  
import json  
from dotenv import load_dotenv  
from semantic_kernel import Kernel  
from semantic_kernel.connectors.ai.open_ai import AzureChatCompletion  
from semantic_kernel.functions import KernelArguments  
from semantic_kernel.contents import ChatHistory  
from datalake_services import download_blob, upload_file_stream  
import ast    
# Load prompts from the text file   


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
  
async def process_page_with_ai(page_number, prompt):  
    load_dotenv()  
    logging.basicConfig(level=logging.WARNING)  
  
    service_id = "reasoning"  
    endpoint = os.getenv("AZURE_ENDPOINT")  
    api_key = os.getenv("AZURE_API_KEY")  
    model_name = os.getenv("AZURE_MODEL_NAME")  
  
    chat_service = AzureChatCompletion(service_id=service_id, endpoint=endpoint, api_key=api_key, deployment_name=model_name)  
    kernel = Kernel()  
    kernel.add_service(chat_service)  
  
    req_settings = kernel.get_prompt_execution_settings_from_service_id(service_id=service_id)  
    req_settings.max_tokens = 15000  
    req_settings.temperature = 0.7  
    req_settings.top_p = 0.8   
    arguments = KernelArguments(input=prompt)  
    chat_function = kernel.add_function(  
        prompt=prompt,  
        function_name="chat",  
        plugin_name="chat",  
        prompt_execution_settings=req_settings,  
    )  
  
    chat_history = ChatHistory()  
    chat_history.add_user_message("Hola, ¿Procesa este contrato?")  
    chat_history.add_assistant_message("Expert in Contract Processing.")  
  
    try:  
        accumulated_messages = ""  
        answer = kernel.invoke_stream(  
            chat_function,  
            arguments=arguments,  
        )  
  
        async for message in answer:  
            accumulated_messages += str(message[0])  
        return accumulated_messages  
    except Exception as e:  
        print(f"Ocurrió un error al procesar la página {page_number}: {e}")  
        return None  
  
async def process_pages_with_ai_specs(page_number,  formatted_prompt):  
    load_dotenv()  
    logging.basicConfig(level=logging.WARNING)  
  
    service_id = "reasoning"  
    endpoint = os.getenv("AZURE_ENDPOINT")  
    api_key = os.getenv("AZURE_API_KEY")  
    model_name = os.getenv("AZURE_MODEL_NAME")  
  
    chat_service = AzureChatCompletion(service_id=service_id, endpoint=endpoint, api_key=api_key, deployment_name=model_name)  
    kernel = Kernel()  
    kernel.add_service(chat_service)  
  
    req_settings = kernel.get_prompt_execution_settings_from_service_id(service_id=service_id)  
    req_settings.max_tokens = 15000  
    req_settings.temperature = 0.7  
    req_settings.top_p = 0.8  
    
   
    
    arguments = KernelArguments(input=formatted_prompt)  
    chat_function = kernel.add_function(  
        prompt=formatted_prompt,  
        function_name="chat",  
        plugin_name="chat",  
        prompt_execution_settings=req_settings,  
    )  
  
    chat_history = ChatHistory()  
    chat_history.add_user_message("Hola, ¿Procesa este contrato?")  
    chat_history.add_assistant_message("Expert in Contract Processing.")  
  
    try:  
        accumulated_messages = ""  
        answer = kernel.invoke_stream(  
            chat_function,  
            arguments=arguments,  
        )  
  
        async for message in answer:  
            accumulated_messages += str(message[0])  
        return accumulated_messages  
    except Exception as e:  
        print(f"Ocurrió un error al procesar la página {page_number}: {e}")  
        return None  
  
async def process_contract_final_phase():  
    load_dotenv()  
    logging.basicConfig(level=logging.WARNING)  
  
    storageaccount = "walmartchiledatalake"  
    container_name = "contracts"  
    folder_name = "bronce"  
    file_name = "walmart-contractV2-140.json"  
    file_name_output = "walmart-contracts-final.json"  
    lake_key = "your_lake_key_here"  # Update with your actual lake key  
  
    # Download the blob  
    stream = download_blob(lake_key, container_name, storageaccount, folder_name, file_name)  
      
    service_id = "reasoning"  
    endpoint = os.getenv("AZURE_ENDPOINT")  
    api_key = os.getenv("AZURE_API_KEY")  
    model_name = os.getenv("AZURE_MODEL_NAME")  
  
    chat_service = AzureChatCompletion(service_id=service_id, endpoint=endpoint, api_key=api_key, deployment_name=model_name)  
    kernel = Kernel()  
    kernel.add_service(chat_service)  
  
    req_settings = kernel.get_prompt_execution_settings_from_service_id(service_id=service_id)  
    req_settings.max_tokens = 15000  
    json_content = json.load(stream)  
    contract_text = json.dumps(json_content, indent=4)  # Pretty print JSON  
  
    prompt = PROMPTS["process_contract_final_phase"].format(contract_text=contract_text)  
  
    arguments = KernelArguments(input=prompt)  
    chat_function = kernel.add_function(  
        prompt=prompt,  
        function_name="chat",  
        plugin_name="chat",  
        prompt_execution_settings=req_settings,  
    )  
  
    chat_history = ChatHistory()  
    chat_history.add_user_message("Hola, ¿Procesa este contrato?")  
    chat_history.add_assistant_message("Expert in Contract Processing.")  
  
    try:  
        accumulated_messages = ""  
        answer = kernel.invoke_stream(  
            chat_function,  
            arguments=arguments,  
        )  
  
        async for message in answer:  
            accumulated_messages += str(message[0])  
  
        # Upload the JSON string to Azure Data Lake  
        upload_stream = io.BytesIO(accumulated_messages.encode('utf-8'))  
        upload_file_stream(storageaccount, lake_key, container_name, folder_name, file_name_output, upload_stream)  
  
        return accumulated_messages  
    except Exception as e:  
        print(f"Ocurrió un error al procesar la página: {e}")  
        return None  
  
async def main() -> None:  
    try:  
        page_number = 1  
        page_content = (  
            "Este contrato se celebra a partir de la fecha firmada a continuación. "  
            "La Parte A se compromete a proporcionar servicios según lo descrito en este documento. "  
            "La Parte B compensará a la Parte A según lo especificado en la Sección 3. "  
            "Este acuerdo está sujeto a las leyes del Estado Ejemplo. "  
            "Cualquier disputa que surja de este contrato se resolverá mediante arbitraje."  
        )  
  
        result = await process_page_with_ai(page_number, page_content)  
        if result is not None:  
            try:  
                json_result = json.dumps(json.loads(result), ensure_ascii=False, indent=2)  
            except json.JSONDecodeError:  
                json_result = result  
            print("Respuesta de la IA:", json_result)  
        else:  
            print("No se obtuvo respuesta de la IA.")  
  
        # Call process_contract_final_phase to process the contract  
        final_phase_result = await process_contract_final_phase()  
        if final_phase_result is not None:  
            print("Resultado de la fase final del contrato:", final_phase_result)  
        else:  
            print("No se obtuvo respuesta de la fase final del contrato.")  
  
    except Exception as e:  
        print(f"Ocurrió un error en la función principal: {e}")  
  
if __name__ == "__main__":  
    asyncio.run(main())  
import json
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient

KEYVAULT_URI = 'https://kv-datalink-dp-pilot.vault.azure.net'
KEYVAULT_COG_SVCS_SECRET = "CogSvcsSecret"

def get_kv_secret(secret_name):
    # connect to Azure Key vault and returns the specified secret value
    az_credential = AzureCliCredential()
    kv_client = SecretClient(vault_url=KEYVAULT_URI, credential=az_credential)
    fetched_secret = kv_client.get_secret(secret_name)
    return fetched_secret.value

# sample document
# formUrl = "https://raw.githubusercontent.com/Azure-Samples/cognitive-services-REST-api-samples/master/curl/form-recognizer/invoice_sample.jpg"

def format_bounding_region(bounding_regions):
    if not bounding_regions:
        return "N/A"
    return ", ".join("Page #{}: {}".format(region.page_number, format_polygon(region.polygon)) for region in bounding_regions)

def format_polygon(polygon):
    if not polygon:
        return "N/A"
    return ", ".join(["[{}, {}]".format(p.x, p.y) for p in polygon])

def analyze_document(bytes_doc):
    cog_svcs_details = json.loads(get_kv_secret(KEYVAULT_COG_SVCS_SECRET))

    document_analysis_client = DocumentAnalysisClient(
        endpoint=cog_svcs_details["endpoint"], credential=AzureKeyCredential(cog_svcs_details["key"])
    )
    
    # poller = document_analysis_client.begin_analyze_document_from_url("prebuilt-invoice", formUrl)
    poller=document_analysis_client.begin_analyze_document(model_id="prebuilt-invoice",document=bytes_doc)
    result = poller.result()

    # print(result)
    content=""
    if result.content:
        content = result.content

    for style in result.styles:
        if style.is_handwritten:
            print("Document contains handwritten content: ")
            print(",".join([result.content[span.offset:span.offset + span.length] for span in style.spans]))
    
    print("----Key-value pairs found in document----")

    KeyValueInfo = []
    for kv_pair in result.key_value_pairs:
        kv_details = {}
        if kv_pair.key:
            kv_details["key"] = kv_pair.key.content
            # kv_details["key_bounding_region"] = format_bounding_region(kv_pair.key.bounding_regions)
        
        if kv_pair.value:
            kv_details["value"] = kv_pair.value.content
            # kv_details["value_bounding_region"] = format_bounding_region(kv_pair.value.bounding_regions)

        KeyValueInfo.append(kv_details)
    
    doc_info = {"content":content,"kv_pairs":KeyValueInfo}
    print(doc_info)

    return KeyValueInfo

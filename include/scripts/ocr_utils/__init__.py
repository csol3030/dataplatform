import ocr_utils.azure_ocr as azure_ocr, ocr_utils.open_src_ocr as open_src_ocr


def get_ocr_output(ocr_mode, document):
    """
    ocr_mode = cloud, open_source

    document = document(pdf) in bytes

    return content and key value pairs based on the selected ocr mode

    ### Sample Function Call
    get_ocr_output(ocr_mode="cloud", document=bytes_data)

    get_ocr_output(ocr_mode="open_source", document=bytes_data)
    """
    ocr_output = {}
    if ocr_mode == "cloud" and document is not None:
        ocr_output = azure_ocr.analyze_document(bytes_doc=document)
    elif ocr_mode == "open_source" and document is not None:
        ocr_output = open_src_ocr.analyze_document(bytes_doc=document)

    return ocr_output

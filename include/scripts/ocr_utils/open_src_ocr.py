import os
import json
import cv2
import numpy as np
from pdf2image import convert_from_bytes
from PIL import Image
import pytesseract

# pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
# pytesseract.get_tesseract_version()

original_text = ""
processed_text = ""


def convert_pdf_image(pdf_byte_data):
    pages = convert_from_bytes(pdf_byte_data)

    src_text = []
    i = 1
    for page in pages:
        img = np.array(page)
        img = cv2.resize(img, None, fx=0.5, fy=0.5)
        text = pytesseract.image_to_string(image=img, config="--oem 3 --psm 6")
        src_text.append(text)
        i = i + 1

    src_text = "\n".join(src_text)

    global original_text
    original_text = src_text

    src_text = src_text.split("\n")
    # print(src_text)
    global processed_text
    processed_text = [row.replace("\n", "") for row in src_text if row != "\n"]
    return original_text
    # (processed_text)


def extract_generic_info(key_name):
    # processed_text = ""
    val = [row for row in processed_text if key_name in row]

    if type(val) == list:
        val = " ".join(val)

    try:
        if " " in key_name:
            new_key_name = key_name.replace(" ", "_")
            val = val.replace(key_name, new_key_name)
            key_name = new_key_name

        val = val.split(" ")
        start_index = val.index(key_name)
        # print(val, start_index)
        # val = val[start_index:]
        tmp_val = val[start_index + 1 :]
        end_index = -1

        for index, item in enumerate(tmp_val):
            if ":" in item:
                end_index = index
                break
        # print(start_index, end_index, tmp_val)
        if end_index == 0 or end_index == -1:
            val = tmp_val
        else:
            val = tmp_val[:end_index]

    except Exception as err:
        print("Error occured", err)
    finally:
        if len(val) > 0:
            val = " ".join(val)
        else:
            val = ""
    return val


def analyze_document(bytes_doc, key_list=[]):

    org_text = convert_pdf_image(bytes_doc)

    if len(key_list) == 0:

        key_list = [
            "Patient:", "DOB:", "Sex:", "Age:", "MR#:", "FIN:",
            "Visit Date:","Visit Time:","Visit Type:","Service Date / Time:","Document Name:",
            "Provider:","Ethnicity:","Referring Provider:","Indication:","Study:","Exam Date:",
            "Accession number:","Reported by:","Interpreted By:"
        ]

    print("==============version===============",pytesseract.get_tesseract_version())

    KeyValueInfo = []
    for key in key_list:
        KeyValueInfo.append({
            "key":key,
            "value":extract_generic_info(key_name=key)
        })

    doc_info = {"content":org_text,"kv_pairs":KeyValueInfo}
    print(doc_info)

    return doc_info

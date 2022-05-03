from urllib.parse import quote_from_bytes
from tqdm import tqdm
from datetime import datetime, timedelta, date
import requests
import pandas as pd
from google.cloud import bigquery
import json
import os
import warnings

warnings.filterwarnings("ignore")

credentials_path = 'C:/Proyectos/Python/brandlive-dwh-test73-fe94209c88b6.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

client = bigquery.Client()

def getIDs(min_of_list, max_of_list):
    df_ids = pd.DataFrame()
    j = min_of_list

    while j <= max_of_list:

        #En el pagesize se especifican la cantidad de items por hoja
        url = "https://nikeclprod.myvtex.com/api/catalog_system/pvt/sku/stockkeepingunitids?page="+str(j)+"&pagesize=10"

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-VTEX-API-AppKey": "vtexappkey-nikeclprod-VCCTAF",
            "X-VTEX-API-AppToken": "RRMUBIYXMUDIJKKHMCNLVGQYEHYJORFFZUXCPEXMVUTRIZAQTDXLJTTLBOQTKPIVOXIPNIRHFAQBNZPFHIPLTTWFAMABNDCGMXCKQLDKLPGJDSYCYLMBZWCVBDOBHPUB"
        }

        response = requests.request("GET", url, headers=headers)
        json_response = response.json()

        if len(json_response) > 0:
            df_new_ids = pd.DataFrame(json_response)
            df_ids = pd.concat([df_ids, df_new_ids])
            j = j + 1

        else:
            print("NA")
            j = j + 1000

    return df_ids

def getCatalog(skuID):
    url = "https://nikeclprod.myvtex.com/api/catalog_system/pvt/sku/stockkeepingunitbyid/"+str(skuID)+"?sc=1"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-VTEX-API-AppKey": "vtexappkey-nikeclprod-VCCTAF",
        "X-VTEX-API-AppToken": "RRMUBIYXMUDIJKKHMCNLVGQYEHYJORFFZUXCPEXMVUTRIZAQTDXLJTTLBOQTKPIVOXIPNIRHFAQBNZPFHIPLTTWFAMABNDCGMXCKQLDKLPGJDSYCYLMBZWCVBDOBHPUB"
    }

    response = requests.get(url, headers=headers)
    json_response = response.json()

    aux = pd.DataFrame([json_response['Id']], columns=['Sku_Ids'])

    aux['Product_Id'] = json_response['ProductId']
    aux['Name_Complete'] = json_response['NameComplete']
    aux['Complement_Name'] = json_response['ComplementName']
    aux['Product_Name'] = json_response['ProductName']
    aux['Product_Description'] = json_response['ProductDescription']
    aux['Product_Ref_Id'] = json_response['ProductRefId']
    aux['Tax_Code'] = json_response['TaxCode']
    aux['Sku_Name'] = json_response['SkuName']
    aux['Is_Active'] = json_response['IsActive']
    aux['Is_Transported'] = json_response['IsTransported']
    aux['Is_Inventoried'] = json_response['IsInventoried']
    aux['Is_GiftCardRecharge'] = json_response['IsGiftCardRecharge']
    aux['Image_Url'] = json_response['ImageUrl']
    aux['Detail_Url'] = json_response['DetailUrl']
    aux['CSC_Identification'] = json_response['CSCIdentification']
    aux['Brand_Id'] = json_response['BrandId']
    aux['Brand_Name'] = json_response['BrandName']
    aux['cubicweight'] = json_response['Dimension']['cubicweight']
    aux['height'] = json_response['Dimension']['height']
    aux['length'] = json_response['Dimension']['length']
    aux['weight'] = json_response['Dimension']['weight']
    aux['width'] = json_response['Dimension']['width']
    aux['real_CubicWeight'] = json_response['RealDimension']['realCubicWeight']
    aux['real_Height'] = json_response['RealDimension']['realHeight']
    aux['real_Weight'] = json_response['RealDimension']['realWeight']
    aux['real_Length'] = json_response['RealDimension']['realLength']
    aux['real_Width'] = json_response['RealDimension']['realWidth']
    aux['realCubicWeight'] = json_response['RealDimension']['realCubicWeight']
    aux['Manufacturer_Code'] = json_response['ManufacturerCode']
    aux['Is_Kit'] = json_response['IsKit']
    aux['Seller_Id'] = json_response['SkuSellers'][0]['SellerId']
    aux['Sales_Channels'] = json_response['SalesChannels']
    aux['Product_Clusters_Ids'] = json_response['ProductClustersIds']
    aux['Product_Category_Ids'] = json_response['ProductCategoryIds']
    aux['Product_GlobalCategory_Id'] = json_response['ProductGlobalCategoryId']
    #aux['Product_Categories'] = json_response['ProductCategories']
    aux['Commercial_Condition_Id'] = json_response['CommercialConditionId']
    aux['Reward_Value'] = json_response['RewardValue']
    #aux['Alternate_Ids'] = json_response['AlternateIds']['RefId']
    aux['Estimated_Date_Arrival'] = json_response['EstimatedDateArrival']
    aux['Measurement_Unit'] = json_response['MeasurementUnit']
    aux['Unit_Multiplier'] = json_response['UnitMultiplier']
    aux['Information_Source'] = json_response['InformationSource']
    aux['Modal_Type'] = json_response['ModalType']
    aux['Key_Words'] = json_response['KeyWords']
    aux['Release_Date'] = json_response['ReleaseDate']
    aux['Product_Is_Visible'] = json_response['ProductIsVisible']
    aux['Show_If_Not_Available'] = json_response['ShowIfNotAvailable']
    aux['Fecha_Registro'] = datetime.today()
    aux['Proceso_Registro'] = "VM-VTEX-script-catalog-Nike"

    return aux

def getDF(min_of_list, max_of_list):
    aux = pd.DataFrame()
    df_ids = getIDs(min_of_list, max_of_list)
    df_catalog = pd.DataFrame()
    i=0

    try:
        for i in range(len(df_ids)):
            id = df_ids[0].iloc[i]
            aux = getCatalog(id)
            df_catalog = pd.concat([df_catalog, aux])
            i = i + 1

    except:
        print("NA")
        i = i + 1

    return df_catalog

def loadDocument(df, tabla_id):
    write_disposition = "WRITE_TRUNCATE"
    # Job que carga el dataframe en BQ
    job_config = bigquery.LoadJobConfig(
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition=write_disposition,
    )

    job = client.load_table_from_dataframe(
        df, tabla_id, job_config=job_config
    )  # Make an API request.
    job.result()

def start(a):
    tabla_id = 'brandlive-dwh-test73.Test.Master-Product-Catalog-Nike'
    #Parametro de pagesize en linea 21
    min_of_list = 1
    max_of_list = 2
    df = getDF(min_of_list, max_of_list)
    registros = pd.DataFrame()
    registros = pd.concat([registros, df])

    loadDocument(registros, tabla_id)
    return "ok"

print(datetime.today())
print(getDF(1,2))
print(datetime.today())
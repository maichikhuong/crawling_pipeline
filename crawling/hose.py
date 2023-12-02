import numpy as np  # linear algebra
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
import requests
import time
from bs4 import BeautifulSoup
import json

from utils.config import *
from typing import Annotated


def login_and_get_data_raw(start_date: str = None, end_date: str = None, page: int = 1) -> Annotated[dict[str, any], 'data hose']:

    first_date = start_date[:2] + '.' + start_date[2:4] + '.' + start_date[-4:]
    last_date = end_date[:2] + '.' + end_date[2:4] + '.' + end_date[-4:]

    url = f'https://www.hsx.vn/Modules/CMS/Web/ArticleInCategory/dca0933e-a578-4eaf-8b29-beb4575052c5?'\
        f'exclude=00000000-0000-0000-0000-000000000000&lim=True&pageFieldName1=FromDate&'\
        f'pageFieldValue1={first_date}&pageFieldOperator1=eq&pageFieldName2=ToDate'\
        f'&pageFieldValue2={last_date}&pageFieldOperator2=eq&pageFieldName3=TokenCode&pageFieldValue3=&pageFieldOperator3=eq&pageFieldName4=CategoryId&pageFieldValue4=dca0933e-a578-4eaf-8b29-beb4575052c5&pageFieldOperator4=eq&pageCriteriaLength=4&_search=false&nd=1696140007149&rows=30&'\
        f'page={page}&sidx=id&sord=desc'

    response = requests.request("GET", url)
    data_raw = response.json()

    return data_raw


def processing_and_get_data(data) -> Annotated[pd.DataFrame, 'Data of page']:

    list_data = []
    list_link_id = []

    for i in range(0, len(data['rows'])):
        list_link_id.append(data['rows'][i]['cell'][0])
        ngaytao = data['rows'][i]['cell'][1]
        tieudebaiviet = data['rows'][i]['cell'][2]
        tieudebaiviet = tieudebaiviet.split('>')[1]
        if ":" in tieudebaiviet:
            MaCK = tieudebaiviet.split(':')[0]
            tieudebaiviet = tieudebaiviet.split(':')[1].strip()
            tieudebaiviet = tieudebaiviet.split('</')[0]
            list_data.append([ngaytao, MaCK, tieudebaiviet])
            # print([ngaytao, MaCK, tieudebaiviet])
        else:
            MaCK = ''
            tieudebaiviet = tieudebaiviet.split('</')[0]
            list_data.append([ngaytao, MaCK, tieudebaiviet])
            # print([ngaytao, MaCK, tieudebaiviet])

    pdf_append = pd.DataFrame(data=list_data, columns=[
                              'Ngày tạo', 'Mã CK', 'Tiêu đề bài viết'])

    list_link = []
    for i in list_link_id:
        link_url = f'https://www.hsx.vn/Modules/Cms/Web/LoadArticle?id={i}&objectType=1'
        link_url = requests.request("GET", link_url)
        soup = BeautifulSoup(link_url.text, 'html.parser')
        try:
            files = soup.find_all(href=True)
            file = ['https://www.hsx.vn' + file.get('href') for file in files]
            # print(file)
            list_link.append(file)
        except:
            # print('error with get file')
            # print('Not link')
            list_link.append('')

    pdf_append['Link'] = list_link
    

    return pdf_append


def hose(start_date: str = None, end_date: str = None, page: int = 1) -> Annotated[list[tuple[any]], 'Data Hose']:

    # first_date = start_date[:2] + '-' + start_date[2:4] + '-' + start_date[-4:]
    # print(f'First Date: {first_date}')
    # last_date = end_date[:2] + '-' + end_date[2:4] + '-' + end_date[-4:]
    # print(f'Last Date: {last_date}')

    pdf = pd.DataFrame(
        columns=['Ngày tạo', 'Mã CK', 'Tiêu đề bài viết', 'Link'])

    while True:
        data = login_and_get_data_raw(start_date, end_date, page)
        pdf_append = processing_and_get_data(data)


        if pdf_append.empty:
            break

        else:
            pdf = pd.concat([pdf, pdf_append], ignore_index=True)
            # print(pdf)
            # print(f'Shape of page {page}: {pdf.shape}')
            page += 1

    # pdf['Ngày tạo'] = pdf['Ngày tạo'].str.split(' ')[0]
    # # print(f'Number row of HOSE Table before filter: {len(pdf)}')
    pdf = pdf[pdf['Tiêu đề bài viết'].str.contains('|'.join(list_tieu_de_key_HOSE))]
    # # print(f'Number row of HOSE Table after filter: {len(pdf)}')

    data = [tuple(pdf.iloc[i]) for i in range(0, len(pdf))]
    
    return data

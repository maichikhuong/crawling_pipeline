from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait

from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import math

from prefect import flow, get_run_logger, task
from prefect.deployments import Deployment
from prefect.filesystems import LocalFileSystem
from config import *

options = webdriver.EdgeOptions()
driver = webdriver.Edge(options=options)


def login_and_search_hnx(first_date: str = None, last_date: str = None):
    
    logger = get_run_logger()

    first_date = first_date[:2] + '/' + first_date[2:4] + '/' + first_date[-4:]
    last_date = last_date[:2] + '/' + last_date[2:4] + '/' + last_date[-4:]

    try:
        driver.get('https://www.hnx.vn/thong-tin-cong-bo-ny-tcph.html')
        time.sleep(1)
        start_date = driver.find_element(By.ID, 'txtTuNgay')
        start_date.send_keys(first_date)
        logger.info(f'Start_date: {first_date}')

        End_date = driver.find_element(By.ID, 'txtDenNgay')
        End_date.send_keys(last_date)
        logger.info(f'End_date: {last_date}')

        time.sleep(2)
        driver.find_element(By.ID, 'btn_searchL').click()
        time.sleep(2)

       
    except TypeError as e:
        print(f'{e}')
        


def get_data_hnx():

    data = driver.find_elements(By.ID, '_tableDatas')

    data_list = []
    for x in data:
        td = x.find_elements(By.TAG_NAME, 'td')
        for y in td:
            if y.text != '':
                data_list.append(y.text)
    

    data_information = []        
    for i in range(0, len(data_list)):
        if i % 5 == 0:
            data_information.append(data_list[i: i+5])


    data_link = driver.find_element(By.ID, '_tableDatas')
    list_link = []
    for i in range(1, len(data_information) + 1):
        data_link.find_element(By.XPATH, f'/html/body/div[1]/div[2]/div[3]/div[5]/div[2]/div[2]/div[1]/table/tbody/tr[{i}]/td[5]/a').click()
        
        time.sleep(2)

        topup = driver.find_element(By.CLASS_NAME, 'divContentArticlesDetail')
        try:
            link = topup.find_element(By.TAG_NAME, 'a').get_attribute('href')
            print(link)
            list_link.append(link)
            

        except:
            try:
                noidung = topup.find_element(By.CLASS_NAME, 'Box-Noidung')
                print(noidung.text)
                list_link.append(noidung.text)
            except:
                print('Not Link')
                list_link.append('Not Link')
            
        finally:
            pass
        
        
        time.sleep(2)
        driver.find_element(By.CLASS_NAME, 'clsBtnClosePopup').click()
        time.sleep(2)

    

    df = pd.DataFrame(data=data_information, columns=['STT','Ngày đăng tin','Mã CK','Tên TCPH','Tiêu đề tin'])
    df['Link đính kèm'] = list_link

    return data_information, list_link, df


@task(name = 'get_all_information_hnx')
def get_all_information_hnx(df):

    df = df.copy()
    logger = get_run_logger()

    paging = driver.find_elements(By.CLASS_NAME, 'paging')
    for i in paging:
        if 'ghi' in i.text:
            limit = i.text
    
    limit = int(limit.split(' ')[2])
    limit = math.ceil(limit/10)
    number = 2

    while number <= limit:
        next_page = driver.find_elements(By.CLASS_NAME, 'paging')
        for x in next_page:
            y = x.find_elements(By.TAG_NAME, 'span')
            for z in y:
                if z.text == '>':
                    z.click()

        
        time.sleep(2)

        data_infomation, list_link, df_append = get_data_hnx()
        logger.info(f'Row page {number}: {len(data_infomation)}')
        logger.info(f'Link page {number}: {len([x for x in list_link if x != "Not Link"])}')

        df = pd.concat([df, df_append], ignore_index=True)

        number += 1

    return df


@task(name = 'login_and_search_upcom')
def login_and_search_upcom(first_date: str = None, last_date: str = None):
    
    logger = get_run_logger()

    first_date = first_date[:2] + '/' + first_date[2:4] + '/' + first_date[-4:]
    last_date = last_date[:2] + '/' + last_date[2:4] + '/' + last_date[-4:]

    try:
        driver.get('https://www.hnx.vn/thong-tin-cong-bo-up-tcph.html')
        time.sleep(1)
        start_date = driver.find_element(By.ID, 'txtTuNgay')
        start_date.send_keys(first_date)
        logger.info(f'Start_date: {first_date}')

        End_date = driver.find_element(By.ID, 'txtDenNgay')
        End_date.send_keys(last_date)
        logger.info(f'End_date: {last_date}')

        time.sleep(2)
        driver.find_element(By.XPATH, '//*[@id="btn_searchU"]').click()
        time.sleep(2)

       
    except TypeError as e:
        print(f'{e}')
        


def get_data_upcom():

    data = driver.find_elements(By.ID, '_tableDatas')

    data_list = []
    for x in data:
        td = x.find_elements(By.TAG_NAME, 'td')
        for y in td:
            if y.text != '':
                data_list.append(y.text)
    

    data_information = []        
    for i in range(0, len(data_list)):
        if i % 5 == 0:
            data_information.append(data_list[i: i+5])


    data_link = driver.find_element(By.ID, '_tableDatas')
    list_link = []
    for i in range(1, len(data_information) + 1):
        data_link.find_element(By.XPATH, f'/html/body/div[1]/div[2]/div[3]/div[5]/div[2]/div[2]/div[1]/table/tbody/tr[{i}]/td[5]/a').click()
        
        time.sleep(2)

        topup = driver.find_element(By.CLASS_NAME, 'divContentArticlesDetail')
        try:
            link = topup.find_element(By.TAG_NAME, 'a').get_attribute('href')
            print(link)
            list_link.append(link)
            

        except:
            try:
                noidung = topup.find_element(By.CLASS_NAME, 'Box-Noidung')
                print(noidung.text)
                list_link.append(noidung.text)
            except:
                print('Not Link')
                list_link.append('Not Link')
            
        finally:
            pass
        
        
        time.sleep(2)
        driver.find_element(By.CLASS_NAME, 'clsBtnClosePopup').click()
        time.sleep(2)

    

    df = pd.DataFrame(data=data_information, columns=['STT','Ngày đăng tin','Mã CK','Tên TCPH','Tiêu đề tin'])
    df['Link đính kèm'] = list_link

    return data_information, list_link, df


@task(name = 'get_all_information_upcom')
def get_all_information_upcom(df):

    df = df.copy()
    logger = get_run_logger()

    paging = driver.find_elements(By.CLASS_NAME, 'paging')
    for i in paging:
        if 'ghi' in i.text:
            limit = i.text
    
    limit = int(limit.split(' ')[2])
    limit = math.ceil(limit/10)
    number = 2

    while number <= limit:
        next_page = driver.find_elements(By.CLASS_NAME, 'paging')
        for x in next_page:
            y = x.find_elements(By.TAG_NAME, 'span')
            for z in y:
                if z.text == '>':
                    z.click()

        
        time.sleep(2)

        data_infomation, list_link, df_append = get_data_upcom()
        logger.info(f'Row page {number}: {len(data_infomation)}')
        logger.info(f'Link page {number}: {len([x for x in list_link if x != "Not Link"])}')

        df = pd.concat([df, df_append], ignore_index=True)

        number += 1

    return df



@task(name = 'crawling_hnx_upcom', retries=5, retry_delay_seconds=5, log_prints=True)
def hnx_upcom(first_date: str, last_date: str):

    logger = get_run_logger()

    login_and_search_hnx(first_date, last_date)
    data_infomation, list_link, df = get_data_hnx()
    # logger.info(f'Row original: {len(data_infomation)}')
    # logger.info(f'Link original: {len([x for x in list_link if x != "Not Link"])}')
    # logger.info(f'Data original: {len(df)}')
    df = get_all_information_hnx(df)
    df['Ngày đăng tin'] =  df['Ngày đăng tin'].str.split(' ')[0]
    logger.info(f'Number row of HNX Table before filter: {len(df)}')
    df = df[df['Tiêu đề tin'].str.contains('|'.join(list_tieu_de_key_HNX_UPCOM))]
    logger.info(df)
    logger.info(f'Number row of HNX Table after filter: {len(df)}')
    df.to_excel(f'Thông tin NNB,NLQ sàn HNX {first_date}.xlsx',index=False)


    login_and_search_upcom(first_date, last_date)
    data_infomation, list_link, df = get_data_upcom()
    # logger.info(f'Row original: {len(data_infomation)}')
    # logger.info(f'Link original: {len([x for x in list_link if x != "Not Link"])}')
    # logger.info(f'Data original: {len(df)}')
    df = get_all_information_upcom(df)
    df['Ngày đăng tin'] =  df['Ngày đăng tin'].str.split(' ')[0]
    logger.info(f'Number row of UPCOM Table before filter: {len(df)}')
    df = df[df['Tiêu đề tin'].str.contains('|'.join(list_tieu_de_key_HNX_UPCOM))]
    logger.info(df)
    logger.info(f'Number row of UPCOM Table after filter: {len(df)}')
    df.to_excel(f'Thông tin NNB,NLQ sàn UPCOM {first_date}.xlsx',index=False)

    driver.quit()









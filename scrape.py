from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth
from datetime import datetime
from confluent_kafka import SerializingProducer
from unidecode import unidecode
import undetected_chromedriver as uc
import mysql.connector
import time
import math
import json
import pandas as pd

chrome_options = Options()
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("--incognito")
chrome_options.add_argument("disable-blink-features=AutomationControlled")  # Hide Selenium
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
chrome_options.add_experimental_option("useAutomationExtension", False)
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {msg}')
    else:
        print(f'Message delivered to {msg.topic()}')

def insert_to_mysql(conn, cur, data) -> None:
    cur.executemany(
        """
        INSERT INTO jobs.jobs(position, company, address, source, query_day, min_salary, max_salary, experience) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [tuple(row) for row in data.itertuples(index=False, name=None)]
    )
    conn.commit()
    return

def insert_to_kafka(producer, data) -> None:
    for index, row in data.iterrows():
        row['position'] = unidecode(row['position'])
        row['company']  = unidecode(row['company'])
        row['address']  = unidecode(row['address'])
        producer.produce(
            'jobs-topic',
            key=str(row['position']).encode('utf-8'),  # Use row value as key
            value=row.to_json(),  # Convert single row to JSON
            on_delivery=delivery_report
        )
    producer.flush()

def get_job_from_top_cv(conn, cur, producer) -> None:
    # get total pages
    driver = uc.Chrome(headless=False, use_subprocess=False)
    driver.get('https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257')

    pagination = WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.ID, 'job-listing-paginate-text'))
    )
    total_page = int(pagination.text.split()[2])

    for i in range(1, total_page+1):
        # get job elements, use presence_of_element_located to await til the element appears
        job_lists_container = WebDriverWait(driver, 20).until(
            ec.presence_of_element_located((By.CLASS_NAME, 'job-list-search-result'))
        )
        job_lists = job_lists_container.find_elements(By.CLASS_NAME, 'job-item-search-result')

        # query each job in each page
        data = []
        for job in job_lists:
            try:
                position = job.find_element(By.CSS_SELECTOR, "h3.title a span").text.strip()
            except NoSuchElementException:
                position = "Not Available"

            try:
                company = job.find_element(By.CSS_SELECTOR, "a.company span.company-name").text.strip()
            except NoSuchElementException:
                company = "Not Available"

            try:
                salary = job.find_element(By.CSS_SELECTOR, "label.title-salary").text.strip()
            except NoSuchElementException:
                salary = "Not Available"

            try:
                address = job.find_element(By.CSS_SELECTOR, "label.address span").text.strip()
            except NoSuchElementException:
                address = "Not Available"

            try:
                exp = job.find_element(By.CSS_SELECTOR, "label.exp span").text.strip()
            except NoSuchElementException:
                exp = "Not Available"

            data.append([position, company, salary, address, exp])

        # next page
        driver.find_element(By.CSS_SELECTOR, 'ul.pagination').find_elements(By.CSS_SELECTOR, 'li')[2].click()
        time.sleep(2)

        data = pd.DataFrame(data)
        data.columns = ['position', 'company', 'salary', 'address', 'exp']

        # Ensure salary column is a string and stripped of leading/trailing spaces
        data['salary'] = data['salary'].astype(str).str.strip()
        data['exp']    = data['exp'].astype(str).str.strip()

        # Initialize columns
        data['source']     = 'topcv'
        data['query_day']  = time.strftime("%Y-%m-%d")
        data['min_salary'] = 0.0
        data['max_salary'] = 0.0
        data['final_exp']  = 0

        # CLEAN EXPERIENCE
        # Case 1: 'ko yeu cau' -> 0
        # Case 2: ' X năm'
        condition = data['exp'].str.contains('năm')
        data.loc[condition, 'final_exp'] = (
            data.loc[condition, 'exp']
            .str.split().str[-2]
            .astype(int)
        )

        # delete column exp
        data = data.drop('exp', axis=1)

        # CLEAN SALARY
        # Case 1: 'Thoả thuận' → min = max = 0
        # Case 2: 'Tới X USD' or 'Tới X triệu'
        condition = data['salary'].str.startswith('Tới')
        is_usd = data['salary'].str.contains('USD', na=False)
        data.loc[condition, 'max_salary'] = (
            data.loc[condition, 'salary']
            .str.split().str[1].str
            .replace(',', '').astype(float)
        )
        data.loc[condition & is_usd, 'max_salary'] *= 25000 / 1000000  # Convert USD to VND
        data.loc[condition & is_usd & (data['max_salary'] > 100), 'max_salary'] //= 12

        # Case 3: 'X - Y USD' or 'X - Y triệu'
        condition = data['salary'].str.contains('-')
        data.loc[condition, 'min_salary'] = (
            data.loc[condition, 'salary']
            .str.replace(',', '')
            .str.split(' - ')
            .str[0].astype(float)
        )
        data.loc[condition, 'max_salary'] = (
            data.loc[condition, 'salary']
            .str.replace(',', '')
            .str.split(' - ').str[1]
            .str.split().str[0]
            .astype(float)
        )
        data.loc[condition & is_usd, ['min_salary', 'max_salary']] *= 25000 / 1000000

        # delete column salary
        data = data.drop('salary', axis=1)

        insert_to_mysql(conn, cur, data)
        insert_to_kafka(producer, data)

    driver.quit()
    return

def get_job_from_career_link(conn, cur, producer) -> None:
    # get total pages
    driver = uc.Chrome(headless=False, use_subprocess=False)
    driver.get('https://www.careerlink.vn/vieclam/tim-kiem-viec-lam?category_ids=130%2C19&page=1')
    try:
        pagination = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'ul.pagination'))
        )
    except (TimeoutException, NoSuchElementException):
        pagination = None

    if pagination:
        li_list = pagination.find_elements(By.CSS_SELECTOR, 'li')
        total_page = int(li_list[-2].text)
    else:
        total_page = 1

    for i in range(1, total_page):
        # get job elements, use presence_of_element_located to await til the element appears
        job_lists_container = WebDriverWait(driver, 50).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'ul.list-group.mt-4'))
        )
        job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.media-body.overflow-hidden")

        # query each job in each page
        data = []
        for job in job_lists:
            try:
                position = job.find_element(By.CSS_SELECTOR, "h5.job-name").text.strip()
            except NoSuchElementException:
                position = "Not Available"

            try:
                company = job.find_element(By.CSS_SELECTOR, "a.text-dark").text.strip()
            except NoSuchElementException:
                company = "Not Available"

            try:
                salary = job.find_element(By.CSS_SELECTOR, "span.job-salary").text.strip()
            except NoSuchElementException:
                salary = "Not Available"

            try:
                address = job.find_element(By.CSS_SELECTOR, "div.job-location div a").text.strip()
            except NoSuchElementException:
                address = "Not Available"

            data.append([position, company, salary, address])

        # next page
        driver.find_element(By.CSS_SELECTOR, 'ul.pagination').find_elements(By.CSS_SELECTOR, 'li')[-1].click()
        time.sleep(2)

        data = pd.DataFrame(data)
        data.columns = ['position', 'company', 'salary', 'address']

        # Ensure salary column is a string and stripped of leading/trailing spaces
        data['salary'] = data['salary'].astype(str).str.strip()

        # Initialize columns
        data['source']      = 'careerlink'
        data['query_day']   = time.strftime("%Y-%m-%d")
        data['min_salary']  = 0.0
        data['max_salary']  = 0.0
        data['final_exp']   = 0

        # CLEAN SALARY
        # Case 1: 'Thương lượng' or 'Cạnh tranh'
        # Case 2: 'X triệu' or 'Trên X triệu'
        condition = data['salary'].str.contains('triệu', na=False) & ~data['salary'].str.contains('-', na=False)
        is_usd = data['salary'].str.contains('USD', na=False)
        data.loc[condition, 'max_salary'] = (
            data.loc[condition, 'salary']
            .str.split()
            .apply(lambda x: float(x[0]) if len(x) == 2 else float(x[1]))
        ) # check if X triệu or Trên X triệu by its length
        data.loc[condition & is_usd, 'max_salary'] *= 25000 / 1000000  # Convert USD to VND
        data.loc[condition & is_usd & (data['max_salary'] > 100), 'max_salary'] //= 12

        # Case 3: 'X triệu - Y triệu' or 'X USD - Y USD'
        condition = data['salary'].str.contains('-')
        data.loc[condition, 'min_salary'] = (
            data.loc[condition, 'salary']
            .str.replace(',', '', regex=True)
            .str.split(' - ').str[0]
            .str.split().str[0]
            .astype(float)
        )
        data.loc[condition, 'max_salary'] = (
            data.loc[condition, 'salary']
            .str.replace(',', '', regex=True)
            .str.split(' - ').str[1]
            .str.split().str[0]
            .astype(float)
        )
        data.loc[condition & is_usd, ['min_salary', 'max_salary']] *= 25000 / 1000000

        # delete column salary
        data = data.drop('salary', axis=1)

        insert_to_mysql(conn, cur, data)
        insert_to_kafka(producer, data)

    driver.quit()
    return

def get_job_from_career_viet(conn, cur, producer) -> None:
    # get total pages
    driver = uc.Chrome(headless=False, use_subprocess=False)
    driver.get('https://careerviet.vn/viec-lam/cntt-phan-cung-mang-cntt-phan-mem-c63,1-vi.html')

    try:
        pagination = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'div.job-found-amout h1'))
        )
    except (TimeoutException, NoSuchElementException):
        pagination = None

    if pagination.text:
        total_page = math.ceil(int(pagination.text.split()[0])/50)
    else:
        total_page = 1

    # get jobs each page
    for i in range(1, total_page+1):
        # get job elements, use presence_of_element_located to await til the element appears
        job_lists_container = WebDriverWait(driver, 50).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'div.jobs-side-list'))
        )
        time.sleep(2)
        job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.figcaption")

        # query each job in each page
        data = []
        for job in job_lists:
            try:
                position = job.find_element(By.CSS_SELECTOR, "h2 a").text.strip()
            except NoSuchElementException:
                position = "Not Available"

            try:
                company = job.find_element(By.CSS_SELECTOR, "a.company-name").text.strip()
            except NoSuchElementException:
                company = "Not Available"

            try:
                salary = job.find_element(By.CSS_SELECTOR, "div.salary p").text.strip()
            except NoSuchElementException:
                salary = "Not Available"

            try:
                address = job.find_element(By.CSS_SELECTOR, "div.location ul").text.strip()
            except NoSuchElementException:
                address = "Not Available"

            data.append([position, company, salary, address])

        # next page
        driver.find_element(By.CSS_SELECTOR, 'div.pagination ul').find_elements(By.CSS_SELECTOR, 'li')[-1].click()
        time.sleep(2)

        data = pd.DataFrame(data)
        data.columns = ['position', 'company', 'salary', 'address']

        # Ensure salary column is a string and stripped of leading/trailing spaces
        data['salary'] = data['salary'].astype(str).str.strip()

        # Initialize columns
        data['source']      = 'careerviet'
        data['query_day']   = time.strftime("%Y-%m-%d")
        data['min_salary']  = 0.0
        data['max_salary']  = 0.0
        data['final_exp']   = 0

        # CLEAN SALARY
        # Case 1: 'Cạnh tranh'
        # Case 2: 'Lương: Trên X Tr VND'
        condition = data['salary'].str.contains('Trên', na=False) & ~data['salary'].str.contains('-', na=False)
        is_usd = data['salary'].str.contains('USD', na=False)
        data.loc[condition, 'max_salary'] = (
            data.loc[condition, 'salary']
            .str.split(':').str[1]
            .str.split().str[1]
            .astype(float)
        )
        data.loc[condition & is_usd, 'max_salary'] *= 25000 / 1000000  # Convert USD to VND
        data.loc[condition & is_usd & (data['max_salary'] > 100), 'max_salary'] //= 12

        # Case 3: 'Lương: Lên đến X Tr VND'
        condition = data['salary'].str.contains('Lên đến', na=False)
        data.loc[condition, 'max_salary'] = (
            data.loc[condition, 'salary']
            .str.split(':').str[1]
            .str.split().str[2]
            .astype(float)
        )
        data.loc[condition & is_usd, 'max_salary'] *= 25000 / 1000000  # Convert USD to VND
        data.loc[condition & is_usd & (data['max_salary'] > 100), 'max_salary'] //= 12

        # Case 3: 'Lương: X Tr - Y Tr VND'
        condition = data['salary'].str.contains('-') & data['salary'].str.contains('Tr')
        data.loc[condition, 'min_salary'] = (
            data.loc[condition, 'salary']
            .str.split(':').str[1]
            .str.replace(',', '')
            .str.split(' - ').str[0]
            .str.split().str[0]
            .astype(float)
        )
        data.loc[condition, 'max_salary'] = (
            data.loc[condition, 'salary']
            .str.split(':').str[1]
            .str.replace(',', '')
            .str.split(' - ').str[1]
            .str.split().str[0]
            .astype(float)
        )

        # CASE 4: 'Lương: X - Y USD'
        condition = data['salary'].str.contains('-') & data['salary'].str.contains('USD')
        data.loc[condition, 'min_salary'] = (
            data.loc[condition, 'salary']
            .str.split(':').str[1]
            .str.replace(',', '')
            .str.split(' - ').str[0]
            .astype(float)
        )
        data.loc[condition, 'max_salary'] = (
            data.loc[condition, 'salary']
            .str.split(':').str[1]
            .str.replace(',', '')
            .str.split(' - ').str[1]
            .str.split().str[0]
            .astype(float)
        )
        data.loc[condition, ['min_salary', 'max_salary']] *= 25000 / 1000000

        # delete column salary
        data = data.drop('salary', axis=1)

        insert_to_mysql(conn, cur, data)
        insert_to_kafka(producer, data)

    driver.quit()
    return

def get_job_from_it_viec(conn, cur, producer) -> None:
    # get total pages
    driver = uc.Chrome(headless=False, use_subprocess=False)
    driver.get('https://itviec.com/it-jobs?&page=1')

    try:
        pagination = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'nav.ipagination.imt-10'))
        )
    except (TimeoutException, NoSuchElementException):
        pagination = None

    if pagination:
        li_list = pagination.find_elements(By.CSS_SELECTOR, 'div.page')
        total_page = int(li_list[-2].text)
    else:
        total_page = 1

    # get jobs each page
    for i in range(1, total_page):
        # get job elements, use presence_of_element_located to await til the element appears
        job_lists_container = WebDriverWait(driver, 50).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'div.col-xl-5.card-jobs-list.ips-0.ipe-0.ipe-xl-6'))
        )
        job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.job-card")

        # query each job in each page
        data = []
        for job in job_lists:
            try:
                position = job.find_element(By.CSS_SELECTOR, "h3.imt-3").text.strip()
            except NoSuchElementException:
                position = "Not Available"

            try:
                company = job.find_element(By.CSS_SELECTOR, "span.ims-2.small-text.text-hover-underline a").text.strip()
            except NoSuchElementException:
                company = "Not Available"

            try:
                salary = job.find_element(By.CSS_SELECTOR, "div.d-flex.align-items-center.salary.text-rich-grey a").text.strip()
            except NoSuchElementException:
                salary = "Not Available"

            try:
                address = job.find_elements(By.CSS_SELECTOR, "span.ips-2.small-text.text-rich-grey")[1].text.strip()
            except NoSuchElementException:
                address = "Not Available"

            data.append([position, company, salary, address])

        # next page
        driver.execute_script("window.scrollBy(0, document.body.scrollHeight)")
        time.sleep(2)
        driver.find_element(By.CSS_SELECTOR, 'div.pagination-search-jobs nav.ipagination').find_element(By.CSS_SELECTOR, 'div.page.next').click()
        time.sleep(2)

        data = pd.DataFrame(data)
        data.columns = ['position', 'company', 'salary', 'address']

        # Initialize columns
        data['source']      = 'itviec'
        data['query_day']   = time.strftime("%Y-%m-%d")
        data['min_salary']  = 0.0
        data['max_salary']  = 0.0
        data['final_exp']   = 0

        # delete column salary
        data = data.drop('salary', axis=1)

        insert_to_mysql(conn, cur, data)
        insert_to_kafka(producer, data)

    driver.quit()
    return

def get_job_from_vietnam_works(conn, cur, producer) -> None:
    # get total pages
    driver = uc.Chrome(headless=False, use_subprocess=False)
    driver.get('https://www.vietnamworks.com/viec-lam?g=5&page=1')

    try:
        pagination = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'div.wrapper-job-criteria'))
        )
    except (TimeoutException, NoSuchElementException):
        pagination = None

    if pagination:
        total_page = math.ceil(779/50)
    else:
        total_page = 1

    print(total_page)

    # get jobs each page
    for i in range(1, total_page):
        driver.execute_script("window.scrollBy(0, document.body.scrollHeight)")
        time.sleep(2)

        # get job elements, use presence_of_element_located to await til the element appears
        job_lists_container = WebDriverWait(driver, 20).until(
            ec.presence_of_element_located((By.CLASS_NAME, 'block-job-list'))
        )
        job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.sc-eEbqID.jZzXhN")

        # query each job in each page
        data = []
        for job in job_lists:
            try:
                position = job.find_element(By.CSS_SELECTOR, "h2 a").text.strip()
            except NoSuchElementException:
                position = "Not Available"

            try:
                company = job.find_element(By.CSS_SELECTOR, "div.sc-cdaca-d.dVvIA a").text.strip()
            except NoSuchElementException:
                company = "Not Available"

            try:
                salary = job.find_element(By.CSS_SELECTOR, "span.sc-fgSWkL.gKHoAZ").text.strip()
            except NoSuchElementException:
                salary = "Not Available"

            try:
                address = job.find_element(By.CSS_SELECTOR, "span.sc-kzkBiZ.hAkUGp").text.strip()
            except NoSuchElementException:
                address = "Not Available"

            data.append([position, company, salary, address])


        data = pd.DataFrame(data)
        data.columns = ['position', 'company', 'salary', 'address']

        # Ensure salary column is a string and stripped of leading/trailing spaces
        data['salary'] = data['salary'].astype(str).str.strip()

        # Initialize columns
        data['source']      = 'vietnamworks'
        data['query_day']   = time.strftime("%Y-%m-%d")
        data['min_salary']  = 0.0
        data['max_salary']  = 0.0
        data['final_exp']   = 0

        # CLEAN SALARY
        # Case 1: 'Thương lượng'
        # Case 2: 'Tới $ X /tháng' or Tới $ X /năm' or 'Từ $ X /tháng'
        condition = data['salary'].str.startswith('Tới')
        is_usd = data['salary'].str.contains('USD', na=False)
        data.loc[condition, 'max_salary'] = data.loc[condition, 'salary'].str.split().str[1].str.replace(',',
                                                                                                         '').astype(
            float)
        data.loc[condition & is_usd, 'max_salary'] *= 25000 / 1000000  # Convert USD to VND
        data.loc[condition & is_usd & (data['max_salary'] > 100), 'max_salary'] //= 12

        # Case 3:  'Xtr-Ytr ₫/tháng'
        condition = data['salary'].str.contains('-')
        data.loc[condition, 'min_salary'] = data.loc[condition, 'salary'].str.replace(',', '').str.split(' - ').str[
            0].astype(float)
        data.loc[condition, 'max_salary'] = \
        data.loc[condition, 'salary'].str.replace(',', '').str.split(' - ').str[1].str.split().str[0].astype(float)
        data.loc[condition & is_usd, ['min_salary', 'max_salary']] *= 25000 / 1000000

        # CASE 4: '$ X-Y /tháng'
        # delete column salary
        data = data.drop('salary', axis=1)

        insert_to_mysql(conn, cur, data)
        insert_to_kafka(producer, data)

    driver.quit()
    return

if __name__ == '__main__':
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root"
        )
        cur = conn.cursor()

        get_job_from_top_cv(conn, cur, producer)
        # get_job_from_career_link(conn, cur, producer)
        # get_job_from_career_viet(conn, cur, producer)
        # get_job_from_it_viec(conn, cur, producer)
        # get_job_from_vietnam_works(conn, cur, 1)

    except Exception as e:
        print(e)
from helium import *
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import time

links = []
data = []

start = time.time()
def lin():
    url = 'https://www.amarstock.com/latest-share-price'
    browser = start_firefox(url, headless=True)

    s = BeautifulSoup(browser.page_source, 'lxml')
    heads = s.find_all('td', class_='ob-left')

    for head in heads:
        try:
            link = head.find('a', class_='scrip green')['href']
            links.append(link)
        except:
            link = 'None'

    kill_browser()


lin()
print(links)


def links_scrape():
    for l in links[21:40]:
        if l is None:
            return
        else:
            url = l
            browser = start_firefox(url, headless=True)
            s = BeautifulSoup(browser.page_source, 'lxml')

            Open = s.find('span', {'data-key': 'OpenPrice'})
            if Open is not None:
                Open = s.find('span', {'data-key': 'OpenPrice'}).text
            if Open is None:
                Open = '0'
            Close = s.find('span', {'data-key': 'YCp'})
            if Close is not None:
                Close = s.find('span', {'data-key': 'YCp'}).text
            if Close is None:
                Close = '0'
            Q1EPS = s.find('span', {'data-key': 'Q1Eps'})
            if Q1EPS is not None:
                Q1EPS = s.find('span', {'data-key': 'Q1Eps'}).text
            if Q1EPS is None:
                Q1EPS = '0'
            Q2EPS = s.find('span', {'data-key': 'Q2Eps'})
            if Q2EPS is not None:
                Q2EPS = s.find('span', {'data-key': 'Q2Eps'}).text
            if Q2EPS is None:
                Q2EPS = '0'
            Q3EPS = s.find('span', {'data-key': 'Q3Eps'})
            if Q3EPS is not None:
                Q3EPS = s.find('span', {'data-key': 'Q3Eps'}).text
            if Q3EPS is None:
                Q3EPS = '0'
            Q4EPS = s.find('span', {'data-key': 'Q4Eps'})
            if Q4EPS is not None:
                Q4EPS = s.find('span', {'data-key': 'Q4Eps'}).text
            if Q4EPS is None:
                Q4EPS = '0'
            address = s.find('div', {'data-key': 'Address'}).text.strip()
            email = s.find('div', {'data-key': 'Email'}).text
            name = s.find('h1', class_='h2 title').text.strip()
            dat = {'Name': name, 'Open': Open, 'Close': Close,
                   'Q1 EPS': Q1EPS, 'Q2 EPS': Q2EPS, 'Q3 EPS': Q3EPS, 'Q4 EPS': Q4EPS,
                   'Email': email, 'Address': address}
            data.append(dat)
            print(dat)
            kill_browser()
            time.sleep(2)


links_scrape()

df = pd.DataFrame(data)

print(df)
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="123456",
                               db="stock"))

df.to_sql('stock_data', con=engine, if_exists='append', chunksize=1000)
end = time.time()
print(end- start)
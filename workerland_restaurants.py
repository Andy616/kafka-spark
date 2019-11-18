import requests
from bs4 import BeautifulSoup
import pandas as pd
import time, json
from fake_useragent import UserAgent
import multiprocessing as mp
from multiprocessing import Queue
import pymongo as mg
import sys, os

def to_mongo(dbname,collname,df,ip):
    myclient = mg.MongoClient(f'mongodb://{ip}:27017')
    db = myclient[f'{dbname}']
    coll = db[f'{collname}']
    coll.insert_many(json.loads(df.T.to_json(force_ascii=False)).values())

def manager(page, page_jq):
    page_jq.put(page)


def crawl_text(crawler_id, page_jq, q, data_q, newest_page, page_range):
    while True:
        page = page_jq.get()
        ua = UserAgent()
        url = f'https://www.walkerland.com.tw/poi/view/{page}'
        res = requests.get(url, headers={'User-Agent': ua.random})
        soup = BeautifulSoup(res.text, 'html.parser')
        try:
            shop_name = soup.select('h1[itemprop="name"]')[0].text[:-1]
        except:
            shop_name = ''
        try:
            phone = soup.select('span[itemprop="telephone"]')[0].text
        except:
            phone = ''
        try:
            address = soup.select('span[itemprop="address"]')[0].text
        except:
            address = ''
        try:
            category = soup.select('span[itemprop="additionalType"]')[0].text
        except:
            category = ''
        try:
            article_url = 'https://www.walkerland.com.tw' + soup.select('h5')[0].select('a')[1]['href']
            q.put(article_url)
        except:
            pass

        data_q.put([url, shop_name, phone, address, category])
        print(f'crawler{crawler_id} : progress {page_range-newest_page+page}/{page_range}.')
        page_jq.task_done()


def main():
    page_jq = mp.JoinableQueue()
    data_q = Queue()
    q = Queue()
    url_data = []

    ua = UserAgent()
    res = requests.get('https://www.walkerland.com.tw/poi/zone/taipei/food', headers={'User-Agent': ua.random})
    soup = BeautifulSoup(res.text, 'html.parser')
    newest_page = int(soup.select('h5')[0].a['href'].split('/')[-1])
    page_list = [i for i in range(newest_page-page_range+1,newest_page+1)]

    for page in page_list:
        manager(page, page_jq)

    for i in range(1, 5):
        name = f'pre_crawler_{i}'
        name = mp.Process(target=crawl_text, args=(i, page_jq, q, data_q, newest_page, page_range))
        name.daemon = True
        name.start()

    page_jq.join()

    # for i in range(q.qsize()):
    #     tmp = q.get()
    #     url_data.append(tmp)
    #
    # with open(f'./walkerland.txt', 'w', encoding='utf8') as f:
    #     f.write(str(url_data))

    df = pd.DataFrame(columns=['_id', '店名', '電話', '地址', '分類'])

    for i in range(data_q.qsize()):
        tmp = data_q.get()
        df.loc[i] = tmp

    to_mongo(dbname, collname, df, ip)

    # df.to_json(save_path, orient='index', force_ascii=False)
    finish = time.clock()
    working_time = finish-start
    print(f'Used {round(working_time / 3600, 3)} hrs')


if __name__ == '__main__':
    start = time.clock()
    page_range = 50
    dbname = 'restaurants'
    collname = 'walkerland'

    if not os.isatty(sys.stdin.fileno()):
        ip = sys.stdin.readlines()[0].split("\n")[0]
        print(ip)
    else:
        print("Skip, so it doesn't hang")
    # Remember to add ".json" at the end of the file name 'cause the output is json file >ω<
    # save_path = 'C:\\Users\\Big data\\Desktop\\walkerland_restaurants.json'

    main()

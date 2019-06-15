#!/usr/bin/env python
# coding=utf-8

import requests
from lxml import etree
from bs4 import BeautifulSoup
import pymysql
import time
from fake_useragent import UserAgent
from pandas import read_csv
import numpy as np
import re

addr_postid = read_csv('./Australian_Post_Codes_Lat_Lon.csv')




def create_database(db_name, connection, cursor):
    query = "CREATE DATABASE {} DEFAULT CHARACTER SET utf8".format(db_name)
    cursor.execute(query)
    # use the db_name as the default database
    connection.commit()  # accept the changes




if __name__ == '__main__':
    print('Link mysql server...')
    db = pymysql.connect(host="localhost", user="root", password="assfASSF1224", port=3306)  # connect your mysql
    cursor = db.cursor()
    # cursor.execute("DROP DATABASE IF EXISTS houserelated")
    # create_database("houserelated", db, cursor)

    print('Linked!')
    cursor.execute("USE houserelated")

    # cursor.execute("DROP TABLE IF EXISTS rentprice")

    sql = """CREATE TABLE rentprice (
            PropID CHAR(20) NOT NULL,
            Price FLOAT,
            Bedrooms INT,
            Bathrooms INT,
            Carparks INT,
            ZIPCODE CHAR(20) )"""

    cursor.execute(sql)
    db.commit()
    #------------table created---------------#

    ua = UserAgent()
    hdrs = {'User-Agent':str(ua.chrome)}

    state = 'nsw'
    for num in range(472,555):
        postZIPCODE = str(addr_postid['postcode'][num])
        mid1 = str(addr_postid['suburb'][num])
        suburb = mid1.replace(' ','+')

        list_num = '1'
        list_over = '2'
        count = 1
        while list_num != list_over:
            url = "https://a_web_address/rent/in-{},+{}+{}/list-{}".format(suburb, state, postZIPCODE, list_num)
            req = requests.get(url, headers=hdrs)
            soup = BeautifulSoup(req.content.decode('gbk', 'ignore'), 'lxml')

            #----------obtain the page number----#
            if list_num == '1':
                pagenum = soup.find_all('p')
                pagenum = str(pagenum[1].contents[0])

                try:
                    ind1 = pagenum.index('of')
                    ind2 = pagenum.index('total')
                    pagenum = int(np.floor(int(pagenum[ind1+3:ind2-1])//20))+1
                    list_over = str(pagenum)
                except:
                    list_num = '2'
                    break
            # file = s.xpath('//p[@class="priceText"')

            # data = requests.get(url).text

            data = req.text
            s = etree.HTML(data)
            price = s.xpath('//*[@class="priceText"]')
            propID = s.xpath('//*//article')
            # propZIPCODE  # this is defined when perform the searching
            trs = soup.find_all('dl')

            time.sleep(1) # sleep some time

            for (pri, pro, tr) in zip(price, propID, trs):  # search all rows
                mid1 = pri.text
                mid1 = mid1.replace(',', '')
                mid1 = re.findall('\d+', mid1)
                if mid1 == []:
                    prices = 0
                else:
                    # mid1 = re.findall('\d+', mid1)
                    prices = float(mid1[0])

                # prices = float(pri.text[1:5])
                propIDs = pro.attrib['id']
                mid1 = tr.contents[2]
                bedrooms = int(mid1.contents[0])
                if len(tr.contents) > 5:
                    mid1 = tr.contents[6]
                    bathrooms = int(mid1.contents[0])
                else:
                    bathrooms = 0
                if len(tr.contents) > 9:
                    mid1 = tr.contents[10]
                    car_parks = int(mid1.contents[0])
                else:
                    car_parks = 0

                insert_data = (
                "INSERT INTO rentprice(PropID,Price,Bedrooms,Bathrooms,Carparks,ZIPCODE)" " VALUES(%s,%s,%s,%s,%s,%s)")
                data_format = (propIDs, prices, bedrooms, bathrooms, car_parks, postZIPCODE)
                # insert_data = ("INSERT INTO rentprice (PropID, Bedrooms)" " VALUES(%s, %s)")
                # data_format = (propIDs, bedrooms)

                cursor.execute(insert_data, data_format)
                db.commit()

            count = count +1 # increase page number
            list_num = str(count)
    print('Finished...')







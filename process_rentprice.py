#!/usr/bin/env python
# coding=utf-8

import pymysql
import sklearn as sk
from scipy.stats import norm
import numpy as np
import matplotlib.pyplot as plt

def close_connection(connection, cursor):
    connection.close
    cursor.close

def delete_database(query, connection, cursor):
    cursor.execute(query)
    connection.commit()
    close_connection(db, cursor)

def create_database(db_name, connection, cursor):
    query = "CREATE DATABASE {} DEFAULT CHARACTER SET utf8".format(db_name)
    cursor.execute(query)
    # use the db_name as the default database
    connection.commit()  # accept the changes


def obtain_data(table_name,postcode):
    # -----select data 'price should be reasonable, postcode is 2032'
    sql = "SELECT Price, Bedrooms, ZIPCODE FROM {} WHERE price > 100 AND price < 5000 AND ZIPCODE LIKE {}".format(table_name, postcode)



    # record price zip and bedroom
    prices = []
    postcodes = []
    bedrooms = []
    try:
        cursor.execute(sql)
        print("count:", cursor.rowcount)
        rows = cursor.fetchall()
        for row in rows:
            price, bedroom, postcode = row
            prices.append(price)
            postcodes.append(postcode)
            bedrooms.append(bedroom)

    except:
        print('error')
    return prices,postcodes, bedrooms

#obtain data

if __name__ == '__main__':

    db = pymysql.connect(host="localhost", user="root", password="your_password", port=3306) # connect your mysql
    cursor = db.cursor()


    #set Default CHARACTER SET utf8
    db_name = 'houserelated'
    table_name = 'rentprice'


    cursor.execute("USE {}".format(db_name))

    #-----first show the columns of this talbe
    # query = 'SHOW columns FROM {}'.format(table_name)
    # cursor.execute(query=query)


    # use inner join to show price postcode and suburb
    sql = ("SELECT price, ZIPCODE, suburb "
            "FROM rentprice "
            "INNER JOIN auspostcode " 
            "on rentprice.ZIPCODE=auspostcode.postcode "
            "WHERE ZIPCODE='2032' AND suburb='kingsford' AND price BETWEEN 100 AND 5000;")
    # join this two tables to show suburb name in each selected raw. You can also try left/right join, full join or even
    # self join and cartesian join

    prices = []
    try:
        cursor.execute(sql)
        print("count:", cursor.rowcount)
        rows = cursor.fetchall()
        for row in rows:
            price,_, _ = row
            prices.append(price)

    except:
        print('error')

    # -----select data 'price should be reasonable, postcode is 2032'
    prices_kinsford, _, bedrooms_kingsford= obtain_data(table_name, 2032)
    prices_syd, _, bedrooms_syd = obtain_data(table_name, 2000)

    #-----------process data----------#

    # close database
    close_connection(db, cursor)

    # ------------------------process data----------------------------#

    prices_2beds_kingsford = [price for price, bedroom in zip(prices_kinsford,bedrooms_kingsford) if bedroom == 2]
    mean_prc = np.mean(prices_2beds_kingsford) #-------------the average
    # plot the distribution of prices
    fig, ax = plt.subplots()
    n, bins, patches = plt.hist(prices_2beds_kingsford, bins=30, histtype='step', density=True,color='g')
    # plt.show()


    # use guassian model to fit the data, to observe the uncertainty
    # prices_gaussian = skm.GaussianMixture(n_components=1, max_iter = 100)
    mu, std = norm.fit(prices_2beds_kingsford)
    # Plot the PDF.

    x = np.linspace(200, 1800, 100)
    p = norm.pdf(x, mu, std)

    ax.plot(x, p, 'b', linewidth=2, label='Kingsford')
    title = "Fit results: mu = %.2f,  std = %.2f" % (mu, std)
    plt.title(title)
    plt.ylabel('probability')
    plt.xlabel('price/AUD')
    leg = ax.legend();


    plt.show()







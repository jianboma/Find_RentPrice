#!/usr/bin/env python
# coding=utf-8

import pymysql
from pandas import read_csv

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

if __name__ == '__main__':

    db = pymysql.connect(host="localhost", user="root", password="your_password", port=3306) # connect your mysql
    cursor = db.cursor()


    #set Default CHARACTER SET utf8
    db_name = 'houserelated'
    tablename = 'auspostcode'
    # create_database(db_name, db, cursor)
    cursor.execute("USE {}".format(db_name))

    #-------------delete a database---------------#
    # query = 'DROP DATABASE {}'.format(db_name)
    # delete_database(query, connection, cursor)



    #-----CREATE TABLE tablename (column1 datatype, ..., PRIMARY KEY ())
    sql = """CREATE TABLE auspostcode (
            postcode CHAR(20) NOT NULL,
            suburb CHAR(50),
            state CHAR(50),
            dc CHAR(50),
            type CHAR(50),
            lat FLOAT,
            lon FLOAT )"""
    # if you are confident about the following line, otherwise comment it
    sql_droptable = "DROP TABLE IF EXISTS {}".format(tablename)
    cursor.execute(sql_droptable)
    cursor.execute(sql)



    #---------read csv file------#
    addr_postid = read_csv('./Australian_Post_Codes_Lat_Lon.csv')
    for num in range(0, len(addr_postid)):
        postcode_single = str(addr_postid['postcode'][num])
        mid1 = str(addr_postid['suburb'][num])
        suburb_single = mid1.replace(' ', '+')
        state_single = str(addr_postid['state'][num])
        dc_single = str(addr_postid['dc'][num])
        type_single = str(addr_postid['type'][num])
        lat_single = float(str(addr_postid['lat'][num]))
        lon_single = float(str(addr_postid['lon'][num]))


        # insert data one raw
        insert_data = (
            "INSERT INTO auspostcode(postcode,suburb,state,dc,type,lat,lon)" " VALUES(%s,%s,%s,%s,%s,%s,%s)")
        data_format = (postcode_single, suburb_single, state_single, dc_single, type_single, lat_single, lon_single)
        # commit the insertion
        try:

            cursor.execute(insert_data, data_format)
            db.commit()
        except:
            print(postcode_single, suburb_single, state_single, dc_single, type_single, lat_single, lon_single)
            continue

    print('Finished...')


    #-------fetch data for testing-----#
    # you can use method fetchone to fetch one by one, or you can use fetchall

    sql = "SELECT suburb FROM auspostcode WHERE postcode='2032'"

    try:
        cursor.execute(sql)
        print("count:", cursor.rowcount)
        row = cursor.fetchone()
        while row:
            print("Row:", row)
            row = cursor.fetchone()
    except:
        print('error')

    # close the opened table
    close_connection(db, cursor)



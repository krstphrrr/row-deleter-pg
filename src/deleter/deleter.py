import pandas as pd
import os
import time
import logging
from psycopg2 import sql
import numpy as np

from src.utils.database_functions import db

def keyvalAdder(dict):
    start =''
    end = 'WHERE '
    for key,value in dict.items():
        if key!=list(dict.keys())[-1]:
            start+=f'"{key}" = \'{value}\' AND '
        else:
            start+=f'"{key}" = \'{value}\''
    end+=start
    return end


def rowDeleter(tablename:str,dev=True,**fields:str):

    # where to delete from: dev or public
    print(f"amount of key-value pairs:{len(fields)}")
    if dev==False:
        d = db('dima')
        keyword = "public"
    elif dev==True:
        d = db("dimadev")
        keyword = "dimadev"
    # creating connection/cursor object (for dev or public)
    con = d.str
    cur = con.cursor()
    # how to structure SQL depending on number of key-value pairs
    if len(fields)<2:
        print(f"deleting 1 key-value pair from postgres.{keyword}.{tablename}")
        for key,value in fields.items():
            singleField = f'''where "{key}"=\'{value}\';'''
            printOut = f' "{key}" = \'{value}\''
        # return f'DELETE FROM postgres.public."{tablename}" {singleField}'
        try:
            sqlStart = f"DELETE FROM postgres.{keyword}.\"{tablename}\" {singleField}"
            print(f"Removing {printOut}...")
            print(f"Using SQL verb : {sqlStart}")

            cur.execute(sqlStart)
            print("Done.")
            con.commit()

        except Exception as e:
            print(e)
            con = d.str
            cur = con.cursor()

    elif len(fields)>=2:
        print(f"deleting {len(fields)} key-value pairs from postgres.{keyword}.{tablename}")
        endVerb = keyvalAdder(fields)
        # return f'DELETE FROM postgres.public."{tablename}" {endVerb};'
        try:
            sqlStart = f"DELETE FROM postgres.{keyword}.\"{tablename}\" {endVerb};"
            print(f"Removing rows using various key-value pairs...")
            print(f"Using SQL verb : {sqlStart}")
            cur.execute(sqlStart)
            print("Done.")
            con.commit()

        except Exception as e:
            print(e)
            con = d.str
            cur = con.cursor()

import pandas as pd 
from datetime import date 
import os
from psycopg2 import Date
from sqlalchemy import create_engine
import sqlalchemy as sa
from sqlalchemy import MetaData, Table, Column, Integer, String
import time
import string
import random




while True:
   meta = MetaData()
   engine = create_engine('postgresql+psycopg2://sqqczmzw:tu3jEaooFIUGYb8A5bqwHRnFJ2oc0xNa@jelani.db.elephantsql.com/sqqczmzw')
#print (engine.table_names())
   insp = sa.inspect(engine)
   check=insp.has_table("Atif-Contact")
   if check==False:
      students = Table(
      'Atif-Contact', meta, 
      Column('FirstName', String), 
      Column('LastName', String), 
      Column('Email', String),
      Column('HubSpot_ID', String),
      Column('Create_Date', Date))
      meta.create_all(engine)

   ID_1= ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k=7))

   ID_2= ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k=7))
   fname=''.join(random.choices(string.ascii_uppercase, k=8))
   lname=''.join(random.choices(string.ascii_uppercase, k=4))
   
   fname2=''.join(random.choices(string.ascii_uppercase, k=8))
   lname2=''.join(random.choices(string.ascii_uppercase, k=4))
   

 

   try:
      query="INSERT INTO  `sqqczmzw`.`Atif-Contact` (`FirstName` ,`LastName` ,`Email` ,`HubSpot_ID`,`Create_Date`)  VALUES(%s,%s,%s,%s,%s)"
      my_data=[(fname,lname,'atif@abc.com',ID_1,date.today()),
            (fname2,lname2,'atif@xyz.com',ID_2,date.today())]
    
      id=engine.execute(query,my_data)
    
      print("Rows Added  = ",id.rowcount)
    
   except:
      print("Database error ")



   time.sleep(900)
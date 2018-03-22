#!/usr/bin/python
from datetime import date
from fuseColumns import order as order_req_cols
from fuseColumns import order_phone as order_phone_req_cols
from fuseColumns import order_address as order_address_req_cols
from fuseColumns import order_emails as order_email_req_cols
import pandas as pd
import pymysql

# today = str(date.today())
# print(today)


hostname = 'localhost'
port='3306'
# username = 'vishal.pandey'
username= 'root'
# password = 'KXH0EV3aV1GkiP10bSy4cPn1lY'
password='verizon'
database = 'verizon'
# Simple routine to run a query on a database and print the results:
myConnection = pymysql.connect( host=hostname, user=username, passwd=password, db=database )

#fetch all orders
order_df = pd.read_sql("SELECT "+ ','.join(order_req_cols)+" FROM orders", con=myConnection)
order_df['source_publisher'] = 'FUSE'

#Extract other data frames from orders

#agent
order_agent = order_df[['rep_id','id']]
order_agent['relation_type']='created'
order_agent.rename(columns={"id": "order_id"})

#name
order_name = order_df[['first_name','last_name','id']]
order_name = order_name.replace([None], [''], regex=True)
order_name['name'] = order_name[['first_name', 'last_name']].apply(lambda x: ' '.join(x), axis=1)
order_name['relation_type'] = 'has'
order_agent.rename(index=str, columns={"id": "order_id"})
order_name=order_name.drop(columns=['first_name', 'last_name'], axis=1)

#remove extra columns from order
order_df=order_df.drop(columns=['first_name', 'last_name', 'rep_id'], axis=1)

print(order_df)
print(order_name)
print(order_agent)

#fetch_all_phones
order_phone = pd.read_sql("SELECT "+ ','.join(order_phone_req_cols)+" FROM order_phone_numbers", con=myConnection)
order_phone['relation_type'] = 'has'
print(order_phone)

#fetch_all_address
order_address = pd.read_sql("SELECT "+ ','.join(order_address_req_cols)+" FROM order_addresses", con=myConnection)
order_address['relation_type'] = 'has'
order_address = order_address.replace([None], [''], regex=True)
order_address['address'] = order_address[['line_1', 'line_2']].apply(lambda x: ' '.join(x), axis=1)
order_address=order_address.drop(columns=['line_1', 'line_2'], axis=1)
print(order_address)

#fetch_email_address
order_email = pd.read_sql("SELECT "+ ','.join(order_email_req_cols)+" FROM order_emails", con=myConnection)
order_email['relation_type'] = 'has'
print(order_email)

#fetch_contact
#interaction_type = 1 i.e. only for phone call interactios

# select interaction_id, order_id,interaction_type_value,interaction_type_secondary_value,dialed_number,interaction_type_id
# from verizon.interaction_order
# LEFT JOIN fusecore.interactions
# ON verizon.interaction_order.interaction_id = fusecore.interactions.id
# WHERE interaction_type_id = 1

# select interaction_id, order_id,interaction_type_value,interaction_type_secondary_value,dialed_number,interaction_type_id,name
# from verizon.interaction_order
# LEFT JOIN verizon.orders
# on verizon.interaction_order.order_id = verizon.orders.id
# LEFT JOIN verizon.order_statuses
# on verizon.orders.order_status_id = verizon.order_statuses.id
# LEFT JOIN fusecore.interactions
# ON verizon.interaction_order.interaction_id = fusecore.interactions.id
# WHERE interaction_type_id = 1

order_contact = pd.read_sql("select order_id,interaction_type_value,interaction_type_secondary_value,dialed_number,name from verizon.interaction_order LEFT JOIN verizon.orders on verizon.interaction_order.order_id = verizon.orders.id LEFT JOIN verizon.order_statuses on verizon.orders.order_status_id = verizon.order_statuses.id LEFT JOIN fusecore.interactions ON verizon.interaction_order.interaction_id = fusecore.interactions.id WHERE interaction_type_id = 1", con=myConnection)
order_contact['phone_relation_type'] = 'has'
print(order_contact)

myConnection.close()
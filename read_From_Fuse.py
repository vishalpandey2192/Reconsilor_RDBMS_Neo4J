#!/usr/bin/python
from datetime import date
from datetime import datetime , timedelta
from fuseColumns import order as order_req_cols
from fuseColumns import order_phone as order_phone_req_cols
from fuseColumns import order_address as order_address_req_cols
from fuseColumns import order_emails as order_email_req_cols
from Logging import Logging
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
# myConnection = pymysql.connect( host=hostname, user=username, passwd=password, db=database )

def get_timestamp(datetime):
    # print(int(datetime.strftime("%s"))*1000)
    # return int(datetime.strftime("%s"))*1000
    # print(datetime.timestamp()*1000)
    return int(datetime.timestamp())

def add_client_to_order(order_id):
    return "VZN-"+str(order_id)

def fetch_from_fuse(starting_date,ending_date):
    print("running fetch from fuse")
    myConnection = pymysql.connect(host=hostname, user=username, passwd=password, db=database)

    #fetch all orders
    query_order = "SELECT "+ ','.join(order_req_cols)+" FROM orders WHERE created_at BETWEEN '"+ str(starting_date) + "' AND '" + str(ending_date)+"'"
    logging = Logging(__name__)
    logging.set_log_message("Ran the following query to get Fuse Orders Data, " + query_order, 'info')
    print("query",query_order)
    order_df = pd.read_sql(query_order, con=myConnection)
    order_df['id']=order_df['id'].apply(add_client_to_order)
    order_df['source_publisher'] = 'FUSE'
    order_df['time_order_created'] = ''
    order_df['created_at'] = order_df['created_at'].apply(get_timestamp)
    order_df['updated_at'] = order_df['updated_at'].apply(get_timestamp)

    #Extract other data frames from orders

    #agent
    order_agent = order_df[['rep_id','id','created_at','updated_at']]
    order_agent['relation_type']='created'
    order_agent.rename(columns={"id": "order_id"},inplace=True)
    order_agent.rename(columns={"rep_id": "agent_id"},inplace=True)

    #name
    order_name = order_df[['first_name','last_name','id','created_at','updated_at']]
    order_name = order_name.replace([None], [''], regex=True)
    order_name['name'] = order_name[['first_name', 'last_name']].apply(lambda x: ' '.join(x), axis=1)
    order_name['relation_type'] = 'has'
    order_name.rename(index=str, columns={"id": "order_id"},inplace=True)
    order_name=order_name.drop(columns=['first_name', 'last_name'], axis=1)

    #customer_fuse
    order_customer_fuse = order_df[['customer_id','id','created_at','updated_at']]
    order_customer_fuse['relation_type'] = 'associated'
    order_customer_fuse.rename(index=str, columns={"id": "order_id"},inplace=True)


    #remove extra columns from order
    order_df=order_df.drop(columns=['first_name', 'last_name', 'rep_id','customer_id'], axis=1)
    order_df.rename(columns={"id": "order_id"},inplace=True)

    # print("Fuse Orders")
    # print(order_df)
    # print("Fuse Name")
    # print(order_name)
    # print("Fuse Agent")
    # print(order_agent)
    # print("Customer Fuse")
    # print(order_customer_fuse)

    #fetch_all_phones
    phone_query = "SELECT "+ ','.join(order_phone_req_cols)+" FROM order_phone_numbers WHERE created_at BETWEEN '"+ str(starting_date) + "' AND '" + str(ending_date)+"'"
    order_phone = pd.read_sql(phone_query, con=myConnection)
    logging = Logging(__name__)
    logging.set_log_message("Ran the following query to get Fuse Phone Data, " + phone_query, 'info')
    order_phone['relation_type'] = 'has'
    order_phone['order_id']=order_phone['order_id'].apply(add_client_to_order)
    dp = {1: 'primary', 2: 'home', 3: 'cell', 4: 'office', 5: 'alternative'}
    order_phone["phone_type_id"].replace(dp, inplace=True)
    order_phone.rename(columns={"phone_number": "phone"}, inplace=True)
    order_phone.rename(columns={"phone_type_id": "type"}, inplace=True)
    order_phone['created_at'] = order_phone['created_at'].apply(get_timestamp)
    order_phone['updated_at'] = order_phone['updated_at'].apply(get_timestamp)
    # print("Fuse Phone")
    # print(order_phone)

    #fetch_all_address
    query_address = "SELECT "+ ','.join(order_address_req_cols)+" FROM order_addresses WHERE created_at BETWEEN '"+ str(starting_date) + "' AND '" + str(ending_date)+"'"
    order_address = pd.read_sql(query_address, con=myConnection)
    logging = Logging(__name__)
    logging.set_log_message("Ran the following query to get Fuse Address Data, " + query_address, 'info')

    order_address['order_id']=order_address['order_id'].apply(add_client_to_order)
    order_address['relation_type'] = 'has'
    order_address['country'] = 'USA'
    order_address = order_address.replace([None], [''], regex=True)
    order_address['address'] = order_address[['line_1', 'line_2']].apply(lambda x: ' '.join(x), axis=1)
    order_address=order_address.drop(columns=['line_1', 'line_2'], axis=1)
    order_address.rename(columns={"postal_code": "zip_code"},inplace=True)
    di = {1:'primary',2:'residential',3:'shipping',4:'billing',5:'service'}
    order_address["address_type_id"].replace(di, inplace=True)
    order_address.rename(columns={"address_type_id": "address_type"},inplace=True)
    order_address['created_at'] = order_address['created_at'].apply(get_timestamp)
    order_address['updated_at'] = order_address['updated_at'].apply(get_timestamp)
    # print("Fuse Address")
    # print(order_address)

    #fetch_email_address
    query_email = "SELECT "+ ','.join(order_email_req_cols)+" FROM order_emails WHERE created_at BETWEEN '"+ str(starting_date) + "' AND '" + str(ending_date)+"'"
    order_email = pd.read_sql(query_email, con=myConnection)
    logging = Logging(__name__)
    logging.set_log_message("Ran the following query to get Fuse Email Data, " + query_email, 'info')
    order_email['order_id']=order_email['order_id'].apply(add_client_to_order)
    order_email['relation_type'] = 'has'
    order_email['created_at'] = order_email['created_at'].apply(get_timestamp)
    order_email['updated_at'] = order_email['updated_at'].apply(get_timestamp)
    # print("Fuse Email")
    # print(order_email)


    contact_query = "select order_id,interaction_type_value,interaction_type_secondary_value,dialed_number,name,interactions.created_at,interactions.updated_at from verizon.interaction_order LEFT JOIN verizon.orders on verizon.interaction_order.order_id = verizon.orders.id LEFT JOIN verizon.order_statuses on verizon.orders.order_status_id = verizon.order_statuses.id LEFT JOIN fusecore.interactions ON verizon.interaction_order.interaction_id = fusecore.interactions.id WHERE interaction_type_id = 1 AND interactions.created_at BETWEEN '"+ str(starting_date) + "' AND '" + str(ending_date)+"'"
    order_contact = pd.read_sql(contact_query, con=myConnection)
    logging = Logging(__name__)
    logging.set_log_message("Ran the following query to get Fuse Contact Data, " + contact_query, 'info')
    order_contact['order_id']=order_contact['order_id'].apply(add_client_to_order)
    order_contact['phone_relation_type'] = 'has'
    order_contact['time_start_contact'] = ''
    order_contact['created_at'] = order_contact['created_at'].apply(get_timestamp)
    order_contact['updated_at'] = order_contact['updated_at'].apply(get_timestamp)
    order_contact.rename(columns={"name": "order_relation_type"},inplace=True)
    order_contact.rename(columns={"dialed_number": "phone"},inplace=True)
    order_contact.rename(columns={"interaction_type_value": "contact_id"},inplace=True)
    order_contact.rename(columns={"interaction_type_secondary_value": "master_contact_id"},inplace=True)

    # print("Fuse Contact")
    # print(order_contact)

    myConnection.close()

    return {'order': order_df, 'address': order_address, 'contact': order_contact,
            'email': order_email, 'customer_fuse': order_customer_fuse, 'phone': order_phone, 'agent': order_agent, 'name':order_name}


# fetch_from_fuse()


from neo4j.v1 import GraphDatabase
from datetime import datetime, timedelta
import pandas as pd
from Logging import Logging

class ClearingHouseDb:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def get_last_one_day_data_from_clearinghouse(self,starting_date,ending_date):
        with self._driver.session() as session:
            # now = datetime.now()
            #
            # # now = now.replace(hour=11, minute=59, second=00, microsecond=00)
            # ending_date = now.timestamp()
            #
            # starting_date = (now - timedelta(days=7
            #                                  )).timestamp()

            result_order_json = {}
            row = []
            df_address = pd.DataFrame()
            df_order = pd.DataFrame()
            df_contact = pd.DataFrame()
            df_agent = pd.DataFrame()
            df_birth_date = pd.DataFrame()
            df_email = pd.DataFrame()
            df_customer_fuse = pd.DataFrame()
            df_phone = pd.DataFrame()
            df_name = pd.DataFrame()
            # query = "Match (n)-[r]-(c) where n.created_at > " + str(
            #     int(starting_date)) + " and n.created_at < " + str(
            #     int(
            #         ending_date)) + " and labels(n)=['order'] return DISTINCT labels(n), properties(n), labels(c),properties(c), type(r)"
            # print(query)

            try:
                with session.begin_transaction() as tx:
                    query = "Match (n)-[r]-(c) where n.created_at > " + str(
                        int(starting_date))  + " and n.created_at < " + str(
                        int(
                            ending_date)) + " and labels(n)=['order'] return DISTINCT labels(n), properties(n), labels(c),properties(c), type(r)"
                    print(query)

                    logging = Logging(__name__)
                    logging.set_log_message("Ran the following query to get past 24hrs order data, "+ query, 'info')
                    for record in tx.run(query):
                        if (row is not None):

                            if (record[1]['order_id'] not in row):
                                row.append(record[1]['order_id'])
                                list_properties = [record[1]['order_id']]
                                list_keys = ['order_id']
                                list_properties.append(
                                    record[1]['source_publisher'] if 'source_publisher' in record[1].keys() else '')
                                list_keys.append('source_publisher')
                                list_properties.append(
                                    record[1]['time_order_created'] if 'time_order_created' in record[1].keys() else '')
                                list_keys.append('time_order_created')
                                list_properties.append(
                                    record[1]['created_at'] if 'created_at' in record[1].keys() else '')
                                list_keys.append('created_at')
                                list_properties.append(
                                    record[1]['updated_at'] if 'updated_at' in record[1].keys() else '')
                                list_keys.append('updated_at')
                                df_o = pd.DataFrame([list_properties], columns=list_keys)
                                df_order = df_order.append(df_o)

                            if (str(record[2]) == "['address']"):
                                # print("add",record[3])
                                list_properties = [record[1]['order_id']]
                                list_keys = ['order_id']
                                list_properties.append(record[3]['address'] if 'address' in record[3].keys() else '')
                                list_keys.append('address')
                                list_properties.append(record[3]['address_type'] if 'address_type' in record[3].keys() else '')
                                list_keys.append('address_type')
                                list_properties.append(record[3]['zip_code'] if 'zip_code' in record[3].keys() else '')
                                list_keys.append('zip_code')
                                list_properties.append(record[3]['city'] if 'city' in record[3].keys() else '')
                                list_keys.append('city')
                                list_properties.append(record[3]['state'] if 'state' in record[3].keys() else '')
                                list_keys.append('state')
                                list_properties.append(record[3]['country'] if 'country' in record[3].keys() else '')
                                list_keys.append('country')
                                list_properties.append(record[4])
                                list_keys.append("relation_type")
                                list_properties.append(
                                    record[3]['created_at'] if 'created_at' in record[3].keys() else '')
                                list_keys.append('created_at')
                                list_properties.append(
                                    record[3]['updated_at'] if 'updated_at' in record[3].keys() else '')
                                list_keys.append('updated_at')
                                df_a = pd.DataFrame([list_properties], columns=list_keys)
                                df_address = df_address.append(df_a)

                            if (str(record[2]) == "['contact']"):
                                list_properties = [record[1]['order_id']]
                                list_keys = ['order_id']
                                list_properties.append(record[3]['contact_id'] if 'contact_id' in record[3].keys() else '')
                                list_keys.append('contact_id')
                                list_properties.append(record[3]['time_start_contact'] if 'time_start_contact' in record[3].keys() else '')
                                list_keys.append('time_start_contact')
                                list_properties.append(
                                    record[3]['master_contact_id'] if 'master_contact_id' in record[3].keys() else '')
                                list_keys.append('master_contact_id')
                                list_properties.append(record[4])
                                list_keys.append("order_relationship_type")

                                #getting associated phones for contact
                                query = "Match (n{contact_id:"+str(record[3]['contact_id'])+"})-[r:has]->(c:phone) where labels(n)=['contact'] return DISTINCT properties(c)"
                                #print(query)
                                result = tx.run(query)
                                if result.peek() != None:
                                    for phone_record in tx.run(query):
                                        list_properties.append(phone_record[0]["phone"])
                                        list_keys.append("phone")
                                else:
                                    list_properties.append('')
                                    list_keys.append("phone")

                                list_properties.append('has')
                                list_keys.append("phone_relationship_type")

                                list_properties.append(
                                    record[3]['created_at'] if 'created_at' in record[3].keys() else '')
                                list_keys.append('created_at')
                                list_properties.append(
                                    record[3]['updated_at'] if 'updated_at' in record[3].keys() else '')
                                list_keys.append('updated_at')
                                df_c = pd.DataFrame([list_properties], columns=list_keys)

                                df_contact = df_contact.append(df_c)

                            # print("phone",record[2])
                            if (str(record[2]) == "['phone']"):
                                list_properties = [record[1]['order_id']]
                                list_keys = ['order_id']
                                list_properties.append(record[3]['phone'] if 'phone' in record[3].keys() else '')
                                list_keys.append('phone')
                                list_properties.append(record[3]['type'] if 'type' in record[3].keys() else '')
                                list_keys.append('type')
                                list_properties.append(record[4])
                                list_keys.append("relation_type")
                                list_properties.append(
                                    record[3]['created_at'] if 'created_at' in record[3].keys() else '')
                                list_keys.append('created_at')
                                list_properties.append(
                                    record[3]['updated_at'] if 'updated_at' in record[3].keys() else '')
                                list_keys.append('updated_at')
                                df_p = pd.DataFrame([list_properties], columns=list_keys)
                                df_phone = df_phone.append(df_p)

                            if (str(record[2]) == "['name']"):
                                list_properties = [record[1]['order_id']]
                                list_keys = ['order_id']
                                list_properties.append(record[3]['name'] if 'name' in record[3].keys() else '')
                                list_keys.append('name')
                                list_properties.append(record[4])
                                list_keys.append("relation_type")
                                list_properties.append(
                                    record[3]['created_at'] if 'created_at' in record[3].keys() else '')
                                list_keys.append('created_at')
                                list_properties.append(
                                    record[3]['updated_at'] if 'updated_at' in record[3].keys() else '')
                                list_keys.append('updated_at')
                                df_n = pd.DataFrame([list_properties], columns=list_keys)
                                df_name = df_name.append(df_n)

                            if (str(record[2]) == "['birth_date']"):
                                list_properties = [record[1]['order_id']]
                                list_keys = ['order_id']
                                list_properties.append(record[3]['birth_date'] if 'birth_date' in record[3].keys() else '')
                                list_keys.append('birth_date')
                                list_properties.append(record[4])
                                list_keys.append("relation_type")
                                list_properties.append(
                                    record[3]['created_at'] if 'created_at' in record[3].keys() else '')
                                list_keys.append('created_at')
                                list_properties.append(
                                    record[3]['updated_at'] if 'updated_at' in record[3].keys() else '')
                                list_keys.append('updated_at')
                                df_bd = pd.DataFrame([list_properties], columns=list_keys)
                                df_birth_date = df_birth_date.append(df_bd)

                            if (str(record[2]) == "['email']"):
                                list_properties = [record[1]['order_id']]
                                list_keys = ['order_id']
                                list_properties.append(record[3]['email'] if 'email' in record[3].keys() else '')
                                list_keys.append('email')
                                list_properties.append(record[4])
                                list_keys.append("relation_type")
                                list_properties.append(
                                    record[3]['created_at'] if 'created_at' in record[3].keys() else '')
                                list_keys.append('created_at')
                                list_properties.append(
                                    record[3]['updated_at'] if 'updated_at' in record[3].keys() else '')
                                list_keys.append('updated_at')
                                df_e = pd.DataFrame([list_properties], columns=list_keys)
                                df_email = df_email.append(df_e)

                            if (str(record[2]) == "['agent']"):
                                list_properties = [record[1]['order_id']]
                                list_keys = ['order_id']
                                list_properties.append(record[3]['agent_id'] if 'agent_id' in record[3].keys() else '')
                                list_keys.append('agent_id')
                                list_properties.append(record[4])
                                list_keys.append("relation_type")
                                # list_properties.append(
                                #     record[3]['created_at'] if 'created_at' in record[3].keys() else '')
                                # list_keys.append('created_at')
                                # list_properties.append(
                                #     record[3]['updated_at'] if 'updated_at' in record[3].keys() else '')
                                # list_keys.append('updated_at')
                                df_a = pd.DataFrame([list_properties], columns=list_keys)
                                df_agent = df_agent.append(df_a)

                            if (str(record[2]) == "['customer_fuse']"):
                                list_properties = [record[1]['order_id']]
                                list_keys = ['order_id']
                                list_properties.append(
                                    record[3]['customer_id'] if 'customer_id' in record[3].keys() else '')
                                list_keys.append('customer_id')
                                list_properties.append(record[4])
                                list_keys.append("relation_type")
                                list_properties.append(
                                    record[3]['created_at'] if 'created_at' in record[3].keys() else '')
                                list_keys.append('created_at')
                                list_properties.append(
                                    record[3]['updated_at'] if 'updated_at' in record[3].keys() else '')
                                list_keys.append('updated_at')
                                df_c = pd.DataFrame([list_properties], columns=list_keys)
                                df_customer_fuse = df_customer_fuse.append(df_c)

                                # order_id = record[1]['order_id']
                                # if(order_id != None and order_id != ''):
                                #
                                #      if order_id in result_order_json.keys():
                                #          orders_properties_dict = result_order_json[order_id]
                                #          record[3]['relation_type'] = record[4]
                                #          orders_properties_dict[str(record[2])] = record[3]
                                #          result_order_json[order_id] = orders_properties_dict
                                #      else:
                                #
                                #          orders_properties_dict = {}
                                #          record[3]['relation_type'] = record[4]
                                #          orders_properties_dict[str(record[2])] = record[3]
                                #          result_order_json[order_id] = orders_properties_dict

                    # print(df_order)
                    # print("--------------------------------------------------------")
                    # print(df_address)
                    # print("--------------------------------------------------------")
                    # print(df_contact)
                    # print("--------------------------------------------------------")
                    # print(df_birth_date)
                    # print("--------------------------------------------------------")
                    # print(df_email)
                    # print("--------------------------------------------------------")
                    # print(df_customer_fuse)
                    # print("--------------------------------------------------------")
                    # print(df_phone)
                    # print("--------------------------------------------------------")
                    # print(df_agent)
                    # print("--------------------------------------------------------")
                    df_order = df_order.reset_index()
                    del df_order['index']
                    df_address = df_address.reset_index()
                    del df_address['index']
                    df_contact = df_contact.reset_index()
                    del df_contact['index']
                    df_birth_date = df_birth_date.reset_index()
                    del df_birth_date['index']
                    df_email = df_email.reset_index()
                    del df_email['index']
                    df_customer_fuse = df_customer_fuse.reset_index()
                    del df_customer_fuse['index']
                    df_phone = df_phone.reset_index()
                    del df_phone['index']
                    df_agent = df_agent.reset_index()
                    del df_agent['index']
                    df_name = df_name.reset_index()
                    del df_name['index']

                    return {'order': df_order, 'address': df_address, 'contact': df_contact, 'birth_date': df_birth_date,
                            'email': df_email, 'customer_fuse': df_customer_fuse, 'phone': df_phone, 'agent': df_agent,'name': df_name}
            except:
                logging = Logging(__name__)
                logging.set_log_message("Error occured while executing query", 'error')

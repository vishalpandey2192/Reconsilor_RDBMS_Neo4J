from neo4j.v1 import GraphDatabase
from datetime import datetime
from Logging import Logging

class BackFillingClearingHouse():
    direction = ''
    node_type = ''
    relation_type = ''
    props = {}

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def backfill_clearinghouse(self, data):
        if len(data) == 0:
            logging = Logging(__name__)
            logging.set_log_message("Empty data returned from fuse", 'info')
            return

        # data = [
        #     {'DN-3679213': {
        #         'order': {'order_id': 'DN-3679213', 'source_publisher': 'FUSE', 'time_order_created': '1234567788'},
        #         # 'address': [{'address': '2s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
        #         #               'relation_type': 'has','address_type':'primary'}
        #         #             {'address': '0s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
        #         #               'relation_type': 'has','address_type':'shipping'},
        #         #             {'address': '0s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
        #         #              'relation_type': 'has', 'address_type': 'shipping'},
        #         #             {'address': '0s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
        #         #              'relation_type': 'has', 'address_type': 'shipping'},
        #         #             {'address': '0s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
        #         #              'relation_type': 'has', 'address_type': 'shipping'},
        #         #             {'address': '0e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
        #         #              'relation_type': 'has', 'address_type': 'shipping'},
        #         #             {'address': '0s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
        #         #              'relation_type': 'has', 'address_type': 'shipping'},
        #         #             {'address': '0s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
        #         #              'relation_type': 'has', 'address_type': 'shipping'},
        #         #
        #                      # ],
        #         # 'email':[
        #         #          {'email': 'abec@gmail.com', 'relation_type': 'has'},
        #         #          {'email': 'abcd@gmail.com', 'relation_type': 'has'},
        #         #          ],
        #         # 'birth_date' :  {'birth_date': '1991-12-30', 'relation_type': 'has'},
        #         'agent':[{'agent_id': '1009', 'relation_type': 'created'}],
        #         # 'customer_fuse' : {'customer_id': 'AcbCD', 'relation_type': 'associated'},
        #         # 'phone':[{'phone': '6464', 'type': 'primary', 'relation_type': 'has'}
        #         #          ],
        #         # 'name' : {'name': 'tanveen', 'relation_type': 'has'},
        #         # 'contact': [{'contact_id': '1005', 'master_contact_id': '001', 'phone': '6464581292',
        #         #              'order_relation_type': 'cancelled', 'phone_relation_type': 'has',
        #         #              'time_start_contact': '1234567788'}
        #         #
        #         #             ]
        #     }}
        #     # ,
        #     #     {'ATT-5379285': {
        #     #         'order': {'order_id': 'ATT-5379285', 'source_publisher': 'FUSE'},
        #     #         'address': {'address': '4s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
        #     #                     'type': 'primary', 'relation_type': 'has'},
        #     #         'name': {'name': 'test_user', 'relation_type': 'has'},
        #     #     }},
        #     #     {'000033': {
        #     #         'order': {'order_id': '000033', 'source_publisher': 'FUSE'},
        #     #         'address': {'address': '471s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
        #     #                     'type': 'primary', 'relation_type': 'has'},
        #     #         'email': {'email': 'abc@gmail.com', 'relation_type': 'has'},
        #     #
        #     #
        #     # }}
        # ]

        logging = Logging(__name__)
        logging.set_log_message("Mismatch data to be updated/created in graphdb "+str(data), 'info')

        with self._driver.session() as session:
            with self._driver.session() as tx:
                for prop in data:
                    for k, v in prop.items():
                        # ? print(k)
                        query = "MATCH (x:order{order_id:'" + k + "'}) return x"
                        # print(str(query))
                        result = tx.run(query)

                        if result.peek() is not None:
                            logging = Logging(__name__)
                            logging.set_log_message("Order Id: " + k + " will be UPDATED",
                                                    'info')
                            self.get_all_node_props_by_order_id(k, prop)

                        else:
                            logging = Logging(__name__)
                            logging.set_log_message("Order Id: " + k + " will be CREATED",
                                                                       'info')
                            self.node_type = 'order'
                            formatted_props = self.format_properties(v['order'])

                            query = "CREATE (:order"+formatted_props+")"
                            result = tx.run(query)

                            logging = Logging(__name__)
                            logging.set_log_message("Executing query : "+query,
                                                    'info')

                            query = "MATCH (x:order{order_id:'" + k + "' , source_publisher: 'FUSE'}) return x"
                            result3 = tx.run(query)

                            if result3.peek() is not None:
                                self.get_all_node_props_by_order_id(k, prop)

        return;

    def format_properties(self, properties):
        formatted_props = '{'
        # print("inside")
        print(properties.items())
        for k, v in properties.items():
            if (self.node_type == 'contact' and k == 'phone') or k == 'relation_type':
                pass
            elif (
                k == 'created_at' or k == 'updated_at' or k == 'time_order_created' or k == 'time_start_contact'):
                if v != '':
                    if len(str(v)) == 10:
                        v = v*1000
                    v = int(v)
                    k = k.replace("'", "")
                    formatted_props = formatted_props + k + " : " + str(v) + ","
                else:
                    formatted_props = formatted_props + k + " : ''" + ","
            else:

                if (k != 'relation_type' and k != 'order_relation_type' and k != 'phone_relation_type' and k != 'order_id' and self.node_type != 'order'):
                    k = k.replace("'", "")
                if "'" in str(v):
                    v = v.replace("'", "\\'")

                formatted_props = formatted_props + k + " : '" + str(v) + "',"




        formatted_props = formatted_props[0:len(formatted_props) - 1] + "}"
        return formatted_props

    def set_contact_node_props(self, i_prop):
        self.direction = 'IN'
        self.props = {}
        self.relation_type = i_prop['order_relation_type']
        self.props['phone_relation_type'] = i_prop['phone_relation_type']
        self.props['phone'] = i_prop['phone']


    def set_other_except_contact_node_props(self, i_prop):
        self.relation_type = i_prop['relation_type']
        if self.node_type == 'agent':
            self.direction = "IN"
        else:
            self.direction = "OUT"
        self.props = None

    def get_all_node_props_by_order_id(self, order_id, properties):
        print(properties)
        for k, v in properties.items():

            for prop_k, prop_v in v.items():
                self.node_type = prop_k

                if (prop_k != 'order'):
                    if type(prop_v) == list:
                        length_of_list = len(prop_v)
                        with self._driver.session() as session:
                            with session.begin_transaction() as tx:
                                query = "Match (n{order_id:'" + order_id + "'})-[r]-(c:" + self.node_type + ") return properties(c)"
                                # query = "Match (n{order_id:'123457'})-[r]-(c) where labels(n)=['order'] return DISTINCT labels(n), properties(n), labels(c),properties(c), type(r) limit 100"
                                # print(query)
                                result = tx.run(query)

                                if result.peek() is None:
                                    if self.node_type == 'address' or self.node_type == 'email' or self.node_type == 'phone' or self.node_type == 'agent' or self.node_type == 'contact':

                                        for i_prop in prop_v:
                                            formatted_props = self.format_properties(i_prop)

                                            logging = Logging(__name__)
                                            logging.set_log_message(
                                                "Order Id: " + k + " properties ADDED are " + formatted_props,
                                                'info')

                                            if self.node_type == 'contact':
                                                self.set_contact_node_props(i_prop)
                                                self.backfill_by_creating_nodes(order_id, formatted_props)

                                            else:
                                                self.set_other_except_contact_node_props(i_prop)

                                                self.backfill_by_creating_nodes(order_id, formatted_props)



                                else:
                                    # print("update")
                                    x = type(result._records)
                                    len_of_results = len(result._records)
                                    # if len_of_results == length_of_list :
                                    for i in range(0, length_of_list):
                                        formatted_props = self.format_properties(prop_v[i])

                                        logging = Logging(__name__)
                                        logging.set_log_message(
                                            "Order Id: " + k + " properties ADDED : " + formatted_props,
                                            'info')

                                        if self.node_type == 'address' or self.node_type == 'email' or self.node_type == 'agent' or self.node_type == 'phone':

                                            self.set_other_except_contact_node_props(prop_v[i]
                                                                                  )

                                            # if self.node_type == 'email' or self.node_type == 'agent':
                                            self.backfill_by_creating_nodes(order_id,
                                                                                formatted_props
                                                                                )

                                            # else:
                                            #     self.backfill_by_updating_nodes(order_id,
                                            #                                     formatted_props,
                                            #                                     result._records[i][0]
                                            #                                     )

                                        if self.node_type == 'contact':
                                            self.set_contact_node_props(prop_v[i])
                                            # if result._records[i][0]['contact_id'] != prop_v[i]['contact_id']:
                                            #     self.backfill_by_updating_nodes(order_id,
                                            #                                     formatted_props,
                                            #                                     result._records[i][0]
                                            #
                                            #                                     )
                                            #
                                            # else:
                                            self.backfill_by_creating_nodes(order_id,
                                                                                formatted_props
                                                                                )

                    else:
                        formatted_props = self.format_properties(prop_v)
                        if str(prop_k) is 'contact':
                            self.relation_type = prop_v['order_relation_type']
                        else:
                            self.relation_type = prop_v['relation_type']
                        with self._driver.session() as session:
                            with session.begin_transaction() as tx:
                                query = "Match (n{order_id:'" + order_id + "'})-[r:" + self.relation_type + "]-(c:" + self.node_type + ") return properties(c)"
                                # query = "Match (n{order_id:'123457'})-[r]-(c) where labels(n)=['order'] return DISTINCT labels(n), properties(n), labels(c),properties(c), type(r) limit 100"
                                # print(query)
                                result = tx.run(query)

                                if result.peek() is None:
                                    logging = Logging(__name__)
                                    logging.set_log_message(
                                        "Order Id: " + order_id + " properties ADDED : " + formatted_props,
                                        'info')
                                    if self.node_type == 'birth_date' or self.node_type == 'name':
                                        self.direction = "OUT"
                                        self.backfill_by_creating_nodes(order_id, formatted_props,
                                                                        )

                                    if self.node_type == 'customer_fuse':
                                        self.direction = 'IN'
                                        self.props = None
                                        self.backfill_by_creating_nodes(order_id, formatted_props,
                                                                        )


                                else:
                                    logging = Logging(__name__)
                                    logging.set_log_message(
                                        "Order Id: " + order_id + " properties UPDATED :  " + formatted_props,
                                        'info')
                                    for old_props in result:
                                        if self.node_type == 'birth_date' or self.node_type == 'name':
                                            self.direction = "OUT"
                                            self.backfill_by_updating_nodes(order_id, formatted_props,
                                                                            old_props[0])

                                        if self.node_type == 'customer_fuse':
                                            self.props = None
                                            self.direction = 'IN'
                                            self.backfill_by_updating_nodes(order_id, formatted_props,
                                                                            old_props[0]
                                                                            )


    def backfill_by_updating_nodes(self,order_id, new_properties, old_properties
                                   ):
        with self._driver.session() as session:
            with session.begin_transaction() as tx:

                if (len(old_properties) > 0):
                    old_properties = self.format_properties(old_properties)
                else:
                    old_properties = ''
                properties = new_properties[0:len(new_properties) - 1] + ', updated_at:' + str(
                    int(datetime.now().timestamp() * 1000)) + "}"
                if self.node_type is 'contact' or self.node_type is 'agent' or self.node_type is 'customer_fuse':
                    query = "MATCH (c:" + self.node_type + old_properties + ")-[r:" + self.relation_type + "]->(n { order_id: '" + order_id + "' })SET c = " + properties

                else:
                    query = "MATCH (n { order_id: '" + order_id + "' })-[r:" + self.relation_type + "]->(c:" + self.node_type + old_properties + ")SET c = " + properties

                result = tx.run(query)
                if self.node_type is 'contact':
                    query = "MATCH (x:" + self.node_type + properties + "), (n:phone) WHERE n.phone = '" + self.props[
                        'phone'] + \
                            "' MERGE(x)-[r:" + self.props['phone_relation_type'] + "]->(n) return r"
                    print(query)
                    result = tx.run(
                        query)

    def backfill_by_creating_nodes(self, order_id, properties):
        with self._driver.session() as session:
            with session.begin_transaction() as tx:

                try:

                    properties = properties[0:len(properties) - 1] + "}"

                    query = "MERGE (x:" + self.node_type + properties + ")"
                    result = tx.run(query)

                    logging = Logging(__name__)
                    logging.set_log_message(
                        'Executing query : '+query,
                        'info')

                    if (self.direction == "OUT"):
                        query = "MATCH(n:order),(x:" + self.node_type + properties + ") WHERE n.order_id = '" + order_id + \
                                "' MERGE(n)-[r:" + self.relation_type + "]->(x) return r"
                        # print(query)
                        result = tx.run(query)
                        logging = Logging(__name__)
                        logging.set_log_message(
                            'Executing query : ' + query,
                            'info')

                    elif (self.node_type == "contact" and self.direction == "IN"):
                        query = "MATCH (x:" + self.node_type + properties + "), (n:order) WHERE n.order_id = '" + order_id + \
                                "' MERGE(x)-[r:" + self.relation_type + "]->(n) return r"
                        # print(query)
                        result = tx.run(
                            query)

                        logging = Logging(__name__)
                        logging.set_log_message(
                            'Executing query : ' + query,
                            'info')

                        query = "Match (n{phone:'" + str(self.props['phone'])  + "'}) return n"
                        result = tx.run(
                            query)
                        if result.peek() is None:

                            query = "MERGE (n:phone{phone:'" + str(self.props['phone']) + "'})"
                            result = tx.run(query)

                        query = "MATCH (x:" + self.node_type + properties + "), (n:phone) WHERE n.phone = '" + self.props['phone'] + \
                                "' MERGE(x)-[r:" + self.props['phone_relation_type'] + "]->(n) return r"
                        # print(query)
                        result = tx.run(
                            query)

                        logging = Logging(__name__)
                        logging.set_log_message(
                            'Executing query : ' + query,
                            'info')

                    elif (self.direction == "IN"):
                        query = "MATCH (x:" + self.node_type + properties + "), (n:order) WHERE n.order_id = '" + order_id + "' MERGE(x)-[r:" + self.relation_type + "]->(n) return r"
                        result = tx.run(query)

                        logging = Logging(__name__)
                        logging.set_log_message(
                            'Executing query : ' + query,
                            'info')

                        # print(properties)
                except:
                    logging = Logging(__name__)
                    logging.set_log_message(
                        'Failed to execute query',
                        'error')

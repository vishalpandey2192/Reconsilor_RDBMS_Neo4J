from neo4j.v1 import GraphDatabase
from datetime import datetime

class BackFillingClearingHouse():

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def backfill_clearinghouse(self, data):
        data = [
            {'JEA-86024':{
            'order':{'order_id': 'JEA-86024', 'source_publisher': 'FUSE'},
            'address': {'address': '472s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
                          'type': 'primary', 'relation_type': 'has'},
            'email':{'email':'tanveen@gmail.com','relation_type':'has'},
            'birth_date' :  {'birth_date': '1992-12-30', 'relation_type': 'has'}
        }},
            {'86024': {
                'order': {'order_id': '86024', 'source_publisher': 'FUSE'},
                'address': {'address': '471s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
                            'type': 'primary', 'relation_type': 'has'},
                'email': {'email': 'abc@gmail.com', 'relation_type': 'has'}
            }}
        ]
        # data['order'] = [{'order_id':'JEA-86024','source_publisher':'FUSE'},{'order_id':'86024','source_publisher':'FUSE'}]
        # data['address']=[{'address':'471s 1350e','city':'salt lake city','state':'utah','zip_code':'84102','type':'primary','relation_type':'has','order_id':'123457'},
        #                  {'address': '471s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
        #                   'type': 'primary', 'relation_type': 'has', 'order_id': 'JEA-86024'}]
        # data['email']=[{'email':'tanveenbharaj92@gmail.com','relation_type':'has','order_id':'123457'}]
        # data['phone']=[{'phone':'6464581282','type':'primary','relation_type':'has','order_id':'123457'},{'phone':'6464566681282','type':'primary','relation_type':'has','order_id':'123457'}]
        # data['birth_date']=[{'birth_date':'6464581282','relation_type':'has','order_id':'123457'}]
        # data['name'] = [{'name': 'tanveen', 'relation_type': 'has', 'order_id': '123457'}]
        # data['agent'] = [{'agent_id': '1001', 'relation_type': 'created', 'order_id': '123457'}]
        # data['customer_fuse'] = [{'customer_id': 'ABCD', 'relation_type': 'cancelled', 'order_id': '123457'}]
        # data['contact'] = [{'contact_id': 'T1001', 'master_contact_id': '001','phone':'6464581282','order_relation_type': 'scheduled', 'phone_relation_type':'has','order_id': '123457'}]


        with self._driver.session() as session:
            with session.begin_transaction() as tx:
                # print(str(data['order'].keys()))
                for props in data:
                    for k,v in props.items():
                        print(k)
                        query = "MATCH (x{order_id:'" +k+"'}) return x"
                        print(str(query))
                        result = tx.run(query)

                        if result.peek() is not None:
                            print("update")
                            order_props = self.get_all_node_props_by_order_id(k,props)
                            # self.backfill_by_updating_nodes(order_props, direction="OUT")
                        else:
                            print("create")

                        # for k,v in data.items():
                        #     node_type = k
                        #     props = v
                        #     for prop in props:
                        #         if node_type == 'address' or node_type == 'email' or node_type == 'phone' or node_type == 'birth_date' or node_type == 'name':
                        #             self.backfill_by_creating_nodes(node_type, prop, direction="OUT")
                        #
                        #         if node_type == 'agent' or node_type == 'customer_fuse' or node_type == 'contact' :
                        #             self.backfill_by_creating_nodes(node_type, prop, direction="IN")
                        #
                        #         else:
                        #             self.backfill_by_creating_nodes(node_type, prop, direction="")



        return ;

    def format_properties(self,properties):
        formatted_props='{'
        print("inside")
        for k,v in properties.items():
            if(k != 'relation_type'):
                print(k,"=",v)
                if(k == 'created_at' or k == 'updated_at'):
                    v = int(v)
                    k =k.replace("'","")
                    formatted_props = formatted_props + k + " : " + str(v) + ","
                else:
                    k = k.replace("'", "")
                    formatted_props = formatted_props + k +" : '"+str(v)+"',"

        formatted_props = formatted_props[0:len(formatted_props)-1]+"}"
        print("properties",formatted_props)
        return formatted_props

    def get_all_node_props_by_order_id(self, order_id, properties):
        print(properties)
        for k,v in properties.items():

            for prop_k,prop_v in v.items():
                node_type = prop_k

                if(prop_k != 'order'):
                    print("key", str(prop_k))
                    print("val", str(prop_v))
                    formatted_props = self.format_properties(prop_v)
                    relation_type = prop_v['relation_type']
                    with self._driver.session() as session:
                        with session.begin_transaction() as tx:
                            query ="Match (n{order_id:'"+order_id+"'})-[r:"+relation_type+"]-(c:"+node_type+") return properties(c)"
                            # query = "Match (n{order_id:'123457'})-[r]-(c) where labels(n)=['order'] return DISTINCT labels(n), properties(n), labels(c),properties(c), type(r) limit 100"
                            print(query)
                            result = tx.run(query)

                            if result.peek() is None:
                                if node_type == 'address' or node_type == 'email' or node_type == 'phone' or node_type == 'birth_date' or node_type == 'name':
                                    print("my name is "+node_type)
                                    self.backfill_by_creating_nodes(node_type, order_id,formatted_props, relation_type, direction="OUT")

                                if node_type == 'agent' or node_type == 'customer_fuse' or node_type == 'contact' :
                                    self.backfill_by_creating_nodes(node_type, order_id,formatted_props, relation_type, direction="IN")

                                else:
                                    self.backfill_by_creating_nodes(node_type, order_id,formatted_props, relation_type, direction="")
                                print("create")
                            else:
                                print("update")
                                for old_props in result:
                                    if node_type == 'address' or node_type == 'email' or node_type == 'phone' or node_type == 'birth_date' or node_type == 'name':
                                        print("old props ", str(old_props[0]))
                                        self.backfill_by_updating_nodes(node_type, order_id,formatted_props, old_props[0],relation_type, direction="OUT")

                                    if node_type == 'agent' or node_type == 'customer_fuse' or node_type == 'contact' :
                                        self.backfill_by_updating_nodes(node_type, order_id,formatted_props, old_props[0], relation_type, direction="IN")

                                    else:
                                        self.backfill_by_updating_nodes(node_type, order_id,formatted_props, old_props[0], relation_type, direction="")
                        # else:
                        #     print()

            # node_type = props[0]
            # node_props = props[1]
            # print("hi",props[1])
            # for k, v in props[1]:
            #     # if(k == 'order_id' and v == order_id):
            #     #     print()
            #     print(props[1])

            # if(props['order_id'])

    def backfill_by_updating_nodes(self, node_type,order_id, new_properties , old_properties, relation_type, direction):
        with self._driver.session() as session:
            with session.begin_transaction() as tx:


                if(len(old_properties)>0):
                    old_properties = self.format_properties(old_properties)
                else:
                    old_properties =''
                # properties = properties [0:len(properties)-2]
                properties = new_properties[0:len(new_properties)-1] +', created_at:'+str(int(datetime.now().timestamp()*1000))+', updated_at:'+str(int(datetime.now().timestamp()*1000))+"}"
                print("new", properties)
                print("old", old_properties)

                query = "MATCH (n { order_id: '" + order_id + "' })-[r:"+relation_type+"]-(c:"+node_type+old_properties+")SET c = "+properties
                # query ="MATCH (n:"+node_type+old_properties+") SET n = "+properties
                print(query)
                result = tx.run(query)
                # query = "MATCH(n:"+node_type+") with properties(n) as updated_node set n = "+properties+" return n"
                # print("update query", query)
                # result = tx.run(query)

                # creating relationships
                # if(direction=="OUT"):
                #     query = "MATCH(n:"+node_type+") where {order_id:'"+order_id+"})"
                #
                #     MATCH(p: Person
                #     {name: 'Keanu Reeves'})
                #     WITH
                #     p, properties(p) as snapshot
                #     SET
                #     p.name = 'The One'
                #     RETURN
                #     snapshot
                #
                #     print(query)
                #     result = tx.run(query)
                #
                # elif (node_type == "contact" and direction == "IN"):
                #     result = tx.run(
                #         "MATCH (x:" + node_type + "{" + properties + "}), (n:order) WHERE n.order_id = '" + order_id +
                #         "' MERGE(x)-[r:" + properties['order_relation_type'] + "]->(n) return r")
                #
                #     result = tx.run(
                #         "MATCH (x:" + node_type + "{" + properties + "}), (n:phone) WHERE n.phone = '" + properties[
                #             'phone'] +
                #         "' MERGE(x)-[r:" + properties['phone_relation_type'] + "]->(n) return r")
                #
                # elif (direction == "IN"):
                #     result = tx.run(
                #         "MATCH (x:" + node_type + "{" + properties + "}), (n:order) WHERE n.order_id = '" + order_id +
                #         "' MERGE(x)-[r:" + relation_type + "]->(n) return r")

                # print(properties)

    def backfill_by_creating_nodes(self, node_type,order_id, properties , relation_type, direction):
        with self._driver.session() as session:
            with session.begin_transaction() as tx:
                # properties = ''
                # print(props)
                # for k, v in props.items():
                #     if (node_type == 'contact'):
                #         if (k != 'phone_relation_type' and k != 'phone' and k != 'order_relation_type' and k != 'order_id'):
                #             k = k.replace("''", "")
                #             properties = properties + k + ":'" + v + "' , "
                #
                #     elif (k != 'relation_type' and k != 'order_id'):
                #         k = k.replace("''","")
                #         properties = properties + k + ":'" +v + "' , "
                #
                #     elif (node_type == 'order'):
                #         k = k.replace("''", "")
                #         properties = properties + k + ":'" + v + "' , "


                # properties = properties [0:len(properties)-2]
                properties = properties[0:len(properties)-1] +', created_at:'+str(int(datetime.now().timestamp()*1000))+', updated_at:'+str(int(datetime.now().timestamp()*1000))+"}"
                print("hi", properties)
                #check if node exists
                #if exists then update else create a new one

                result = tx.run("MERGE (x:" + node_type +properties+")")
                # print()
                #
                # creating relationships
                if(direction=="OUT"):
                    query = "MATCH(n:order),(x:"+node_type + properties + ") WHERE n.order_id = '"+order_id+ \
                            "' MERGE(n)-[r:"+relation_type+"]->(x) return r"
                    print(query)
                    result = tx.run(query)

                elif (node_type == "contact" and direction == "IN"):
                    result = tx.run(
                        "MATCH (x:" + node_type + "{" + properties + "}), (n:order) WHERE n.order_id = '" + order_id +
                        "' MERGE(x)-[r:" + properties['order_relation_type'] + "]->(n) return r")

                    result = tx.run(
                        "MATCH (x:" + node_type + "{" + properties + "}), (n:phone) WHERE n.phone = '" + properties[
                            'phone'] +
                        "' MERGE(x)-[r:" + properties['phone_relation_type'] + "]->(n) return r")

                elif (direction == "IN"):
                    result = tx.run(
                        "MATCH (x:" + node_type + "{" + properties + "}), (n:order) WHERE n.order_id = '" + order_id +
                        "' MERGE(x)-[r:" + relation_type + "]->(n) return r")

                # print(properties)




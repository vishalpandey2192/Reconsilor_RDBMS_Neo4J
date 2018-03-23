from neo4j.v1 import GraphDatabase
from datetime import datetime

class BackFillingClearingHouse():

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

        if result3.peek() is not None:
            print("NOT NONE")

    def backfill_clearinghouse(self, data):
        data = [
            {'DN-3679205':{
            'order':{'order_id': 'DN-3679205', 'source_publisher': 'FUSE'},
            'address': {'address': '472s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
                          'type': 'primary', 'relation_type': 'has'},
            'email':{'email':'tanveen@gmail.com','relation_type':'has'},
            'birth_date' :  {'birth_date': '1992-12-31', 'relation_type': 'has'},
            'agent':{'agent_id': '1003', 'relation_type': 'created'},
            'customer_fuse' : {'customer_id': 'ABCD', 'relation_type': 'cancelled'},
            'phone':{'phone': '6464581282', 'type': 'primary', 'relation_type': 'has'},
            'name' : {'name': 'tanveen', 'relation_type': 'has'},
            'contact': {'contact_id': '1003', 'master_contact_id': '001', 'phone': '6464581282',
                            'order_relation_type': 'scheduled', 'phone_relation_type': 'has'}
        }},
            {'ATT-5379285': {
                'order': {'order_id': 'ATT-5379285', 'source_publisher': 'FUSE'},
                'address': {'address': '4s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
                            'type': 'primary', 'relation_type': 'has'},
                'name': {'name': 'test_user', 'relation_type': 'has'},
            }},
            {'000033': {
                'order': {'order_id': '000033', 'source_publisher': 'FUSE'},
                'address': {'address': '471s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102',
                            'type': 'primary', 'relation_type': 'has'},
                'email': {'email': 'abc@gmail.com', 'relation_type': 'has'},


        }}
        ]

        with self._driver.session() as session:
            with self._driver.session() as tx:
                # print(str(data['order'].keys()))
                for props in data:
                    for k,v in props.items():
                        print(k)
                        query = "MATCH (x:order{order_id:'" +k+"'}) return x"
                        print(str(query))
                        result = tx.run(query)

                        if result.peek() is not None:
                            print("update", props)
                            order_props = self.get_all_node_props_by_order_id(k,props)
                        else:
                            print("create")
                            query = "CREATE (:order{order_id: '" + k + "', source_publisher: 'FUSE', created_at: "+ \
                                    str(int(datetime.now().timestamp()))+"000"+" })"
                            result = tx.run(query)
                            print(query)
                            print(result.peek())
                            # if result.peek() is not None:
                            print("successful order creation")
                            print("multiple props ", props)
                            query = "MATCH (x:order{order_id:'" + k + "' , source_publisher: 'FUSE'}) return x"
                            print(str(query))
                            result3 = tx.run(query)

                            if result3.peek() is not None:
                                print("NOT NONE")
                                order_props = self.get_all_node_props_by_order_id(k, props)

        return ;

    def format_properties(self,properties):
        formatted_props='{'
        print("inside")
        for k,v in properties.items():
            # if(k != 'relation_type' or k !='order_relation_type' or k != 'phone_relation_type'):
            print(k,"=",v)
            if(k == 'created_at' or k == 'updated_at' or k == 'contact_id'):
                v = int(v)
                k =k.replace("'","")
                formatted_props = formatted_props + k + " : " + str(v) + ","
            else:
                if (k != 'relation_type' and k != 'order_relation_type' and k != 'phone_relation_type'):
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
                    if str(prop_k) is 'contact':
                        print(" yes contact")
                        relation_type = prop_v['order_relation_type']
                    else:
                        print(" no contact")
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
                                    direction = "OUT"
                                    self.backfill_by_creating_nodes(node_type, order_id,formatted_props, relation_type, direction, None)

                                if node_type == 'agent' or node_type == 'customer_fuse' or node_type == 'contact' :
                                    print("my name is " + node_type)
                                    direction = 'IN'
                                    if node_type == 'contact':
                                        props={}
                                        props['phone_relation_type'] = prop_v['phone_relation_type']
                                        props['phone'] = prop_v['phone']
                                    else:
                                        props = None
                                    self.backfill_by_creating_nodes(node_type, order_id,formatted_props, relation_type, direction,props)

                                # else:
                                #     self.backfill_by_creating_nodes(node_type, order_id,formatted_props, relation_type, direction="")
                                print("create")
                            else:
                                print("update")
                                for old_props in result:
                                    if node_type == 'address' or node_type == 'email' or node_type == 'phone' or node_type == 'birth_date' or node_type == 'name':
                                        print("old props ", str(old_props[0]))
                                        direction = "OUT"
                                        self.backfill_by_updating_nodes(node_type, order_id,formatted_props, old_props[0],relation_type, direction, None)

                                    if node_type == 'agent' or node_type == 'customer_fuse' or node_type == 'contact' :
                                        direction = "IN"
                                        if node_type == 'contact':
                                            props = {}
                                            props['phone_relation_type'] = prop_v['phone_relation_type']
                                            props['phone'] = prop_v['phone']
                                            print("phone props", props)
                                        else:
                                            props = None
                                        self.backfill_by_updating_nodes(node_type, order_id,formatted_props, old_props[0], relation_type, direction, props)

    def backfill_by_updating_nodes(self, node_type,order_id, new_properties , old_properties, relation_type, direction,props):
        print("phone props", str(props))
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

                if node_type is 'contact' or node_type is 'agent' or node_type is 'customer_fuse':
                    query = "MATCH (c:"+node_type+old_properties+")-[r:"+relation_type+"]->(n { order_id: '" + order_id + "' })SET c = "+properties

                else:
                    query = "MATCH (n { order_id: '" + order_id + "' })-[r:" + relation_type + "]->(c:" + node_type + old_properties + ")SET c = " + properties
                # query ="MATCH (n:"+node_type+old_properties+") SET n = "+properties

                print(query)
                result = tx.run(query)
                if node_type is 'contact':

                    query = "MATCH (x:" + node_type + properties + "), (n:phone) WHERE n.phone = '" + props[
                        'phone'] + \
                            "' MERGE(x)-[r:" + props['phone_relation_type'] + "]->(n) return r"
                    print(query)
                    result = tx.run(
                        query)


    def backfill_by_creating_nodes(self, node_type,order_id, properties , relation_type, direction,props):
        with self._driver.session() as session:
            with session.begin_transaction() as tx:

                # properties = properties [0:len(properties)-2]
                properties = properties[0:len(properties)-1] +', created_at:'+str(int(datetime.now().timestamp()*1000))+', updated_at:'+str(int(datetime.now().timestamp()*1000))+"}"
                print("hi create", properties)
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
                    query = "MATCH (x:" + node_type + properties + "), (n:order) WHERE n.order_id = '" + order_id +\
                            "' MERGE(x)-[r:" + relation_type + "]->(n) return r"
                    print(query)
                    result = tx.run(
                        query)
                    query = "MATCH (x:" + node_type  + properties + "), (n:phone) WHERE n.phone = '" + props['phone'] +\
                            "' MERGE(x)-[r:" + props['phone_relation_type'] + "]->(n) return r"
                    print(query)
                    result = tx.run(
                        query)

                elif (direction == "IN"):
                    result = tx.run(
                        "MATCH (x:" + node_type  + properties + "), (n:order) WHERE n.order_id = '" + order_id +
                        "' MERGE(x)-[r:" + relation_type + "]->(n) return r")

                # print(properties)




from neo4j.v1 import GraphDatabase

class BackFillingClearingHouse():

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def backfill_clearinghouse(self, data):
        data['order'] = [{'order_id':'123457'}]
        data['address']=[{'address':'471s 1350e','city':'salt lake city','state':'utah','zip_code':'84102','type':'primary','relation_type':'has','order_id':'123457'}]
        data['email']=[{'email':'tanveenbharaj92@gmail.com','relation_type':'has','order_id':'123457'}]
        data['phone']=[{'phone':'6464581282','type':'primary','relation_type':'has','order_id':'123457'},{'phone':'6464566681282','type':'primary','relation_type':'has','order_id':'123457'}]
        data['birth_date']=[{'birth_date':'6464581282','relation_type':'has','order_id':'123457'}]
        data['name'] = [{'name': 'tanveen', 'relation_type': 'has', 'order_id': '123457'}]
        data['agent'] = [{'agent_id': '1001', 'relation_type': 'created', 'order_id': '123457'}]
        data['customer_fuse'] = [{'customer_id': 'ABCD', 'relation_type': 'cancelled', 'order_id': '123457'}]
        data['contact'] = [{'contact_id': 'T1001', 'master_contact_id': '001','phone':'6464581282','order_relation_type': 'scheduled', 'phone_relation_type':'has','order_id': '123457'}]

        for k,v in data.items():
            node_type = k
            props = v
            for prop in props:
                if node_type == 'address' or node_type == 'email' or node_type == 'phone' or node_type == 'birth_date' or node_type == 'name':
                    self.backfill_by_creating_nodes(node_type, prop, direction="OUT")

                if node_type == 'agent' or node_type == 'customer_fuse' or node_type == 'contact' :
                    self.backfill_by_creating_nodes(node_type, prop, direction="IN")

                else:
                    self.backfill_by_creating_nodes(node_type, prop, direction="")



        return ;

    #     # backfilling entire new order, expect dictionaries of each node
    #     order = {}
    #     order['order_id'] = '123456'
    #     order_node = self.backfill_order(order['order_id'])
    #
    #     #address -- have to handle multiple address
    #     address={}
    #     address['address']="470s 1350e"
    #     address['city']="salt lake city"
    #     address['state']="utah"
    #     address['zip_code']="84102"
    #     address['country']="USA"
    #     address['type'] = 'primary'
    #     address['relation_type'] = 'has'
    #     address['order_id'] = '123456'
    #     self.backfill_by_creating_nodes('address', address,direction="OUT")
    #
    #     #email -- have to handle multiple emails
    #     emails = {}
    #     emails['email'] = "tanveenbharaj92@gmail.com"
    #     emails['relation_type'] = 'has'
    #     emails['order_id'] = '123456'
    #     self.backfill_by_creating_nodes('email', emails,direction="OUT")
    #
    #     # phones -- have to handle multiple phones
    #     phones = {}
    #     phones['phone'] = "6464581282"
    #     phones['type'] = "primary"
    #     phones['relation_type'] = 'has'
    #     phones['order_id'] = '123456'
    #     self.backfill_by_creating_nodes('phone', phones,direction="OUT")
    #
    #     # birth_date -- have to handle multiple birth_date
    #     birth_date = {}
    #     birth_date['birth_date'] = "6464581282"
    #     birth_date['relation_type'] = 'has'
    #     birth_date['order_id'] = '123456'
    #     self.backfill_by_creating_nodes('birth_date', birth_date,direction="OUT")
    #
    #     # name -- have to handle multiple name
    #     name = {}
    #     name['name'] = "tanveen"
    #     name['relation_type'] = 'has'
    #     name['order_id'] = '123456'
    #     self.backfill_by_creating_nodes('name', name,direction="OUT")
    #
    #     # agent -- have to handle multiple agent
    #     agent = {}
    #     agent['agent_id'] = "1001"
    #     agent['relation_type'] = 'created'
    #     agent['order_id'] = '123456'
    #     self.backfill_by_creating_nodes('agent', agent,direction="IN")
    #
    #     # customer_fuse -- have to handle multiple customer_fuse
    #     customer_fuse = {}
    #     customer_fuse['customer_id'] = "ABCD"
    #     customer_fuse['relation_type'] = 'cancelled'
    #     customer_fuse['order_id'] = '123456'
    #     self.backfill_by_creating_nodes('customer_fuse', customer_fuse,direction="IN")
    #
    #     # contact -- have to handle multiple contact
    #     contact = {}
    #     contact['contact_id'] = "T1001"
    #     contact['master_contact_id'] = '0001'
    #     contact['order_relation_type'] = 'scheduled'
    #     contact['phone'] = "6464581282"
    #     contact['phone_relation_type'] = 'has'
    #     contact['order_id'] = '123456'
    #     self.backfill_by_creating_nodes('contact', contact,direction="IN")
    #
    # def backfill_order(self,order_id):
    #     with self._driver.session() as session:
    #         with session.begin_transaction() as tx:
    #             result = tx.run("MERGE (n:order {order_id: '"+order_id+"'})")
    #             print(result)
    #             return result

    def backfill_by_creating_nodes(self, node_type, props , direction):
        with self._driver.session() as session:
            with session.begin_transaction() as tx:
                properties = ''
                print(node_type)
                for k, v in props.items():
                    if (node_type == 'contact'):
                        if (k != 'phone_relation_type' and k != 'phone' and k != 'order_relation_type' and k != 'order_id'):
                            k = k.replace("''", "")
                            properties = properties + k + ":'" + v + "' , "

                    elif (k != 'relation_type' and k != 'order_id'):
                        k = k.replace("''","")
                        properties = properties + k + ":'" +v + "' , "

                    elif (node_type == 'order'):
                        k = k.replace("''", "")
                        properties = properties + k + ":'" + v + "' , "

                properties = properties [0:len(properties)-2]
                result = tx.run("MERGE (x:" + node_type + "{"+properties+"})")

                # creating relationships
                if(direction=="OUT"):
                    print("hi",props['order_id'])
                    result = tx.run("MATCH(n:order),(x:"+node_type+ "{" + properties + "}) WHERE n.order_id = '"+props['order_id']+
                                                                "' MERGE(n)-[r:"+props['relation_type']+"]->(x) return r")

                elif (node_type == "contact" and direction == "IN"):
                    result = tx.run(
                        "MATCH (x:" + node_type + "{" + properties + "}), (n:order) WHERE n.order_id = '" + props['order_id'] +
                        "' MERGE(x)-[r:" + props['order_relation_type'] + "]->(n) return r")

                    result = tx.run(
                        "MATCH (x:" + node_type + "{" + properties + "}), (n:phone) WHERE n.phone = '" + props[
                            'phone'] +
                        "' MERGE(x)-[r:" + props['phone_relation_type'] + "]->(n) return r")

                elif (direction == "IN"):
                    result = tx.run(
                        "MATCH (x:" + node_type + "{" + properties + "}), (n:order) WHERE n.order_id = '" + props['order_id'] +
                        "' MERGE(x)-[r:" + props['relation_type'] + "]->(n) return r")

                print(properties)




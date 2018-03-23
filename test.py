from neo4j.v1 import GraphDatabase
from datetime import datetime , timedelta

uri = "bolt://stage-neo4j.clearlinkdata.com:7687"
driver = GraphDatabase.driver(uri, auth=("tanveen", "tanveenisgreatvishalisaight"))

def getLastOneDayFromClearingHouse():
    with driver.session() as session:
        now = datetime.now()
        now = now.replace(hour=11, minute=59, second=00, microsecond=00)
        ending_date = now.timestamp()
        starting_date = (now - timedelta(days=1)).timestamp()

        result_order_json = []

        with session.begin_transaction() as tx:
            query = "Match (n) where n.created_at > " + str(starting_date) + " and n.created_at < " + str(
                ending_date) + " and labels(n)=['order'] return n"
            #query="Match (n) where labels(n)=['order'] return n Limit 10"
            print(query)
            for record in tx.run(query):
                order_id = record[0]['order_id']
                if(order_id != None and order_id != ''):
                    order_dict = {}
                    order_dict['order_id'] = order_id

                    query = "MATCH (n { order_id: '" + order_id + "' })-[r]-(c) RETURN DISTINCT labels(c),properties(c) "
                    for related_node in tx.run(query):
                        if str(related_node[0]) in order_dict.keys():
                            order_dict[str(related_node[0])] = properties_list.append(related_node[1])
                        else :
                            properties_list = []
                            properties_list = related_node[1]
                            order_dict[str(related_node[0])] = properties_list
                        result_order_json.append(order_dict)


print(getLastOneDayFromClearingHouse())

data={}
def create_missing_node_data(type,input):
    node_order_id=input['order_id']
    cols = input.index
    if node_order_id not in data:
        data[node_order_id]={}

    if type != 'order' or type != 'name':

        if type not in data[node_order_id]:
            data[node_order_id][type] = []
        temp={}
        for columns in cols:
            temp[columns] = input[columns]
        data[node_order_id][type].append(temp)
    else:
        if type not in data[node_order_id]:
            data[node_order_id][type]={}
            for columns in cols:
                data[node_order_id][type][columns] = input[columns]
    # print(data)

def push_to_be_created_nodes():
    print(data)
    return


    # print(type)
    # data={}
    # data['order'] = [{'order_id': '123457'}]
    # data['address'] = [
    #     {'address': '471s 1350e', 'city': 'salt lake city', 'state': 'utah', 'zip_code': '84102', 'type': 'primary',
    #      'relation_type': 'has', 'order_id': '123457'}]
    # data['email'] = [{'email': 'tanveenbharaj92@gmail.com', 'relation_type': 'has', 'order_id': '123457'}]
    # data['phone'] = [{'phone': '6464581282', 'type': 'primary', 'relation_type': 'has', 'order_id': '123457'},
    #                  {'phone': '6464566681282', 'type': 'primary', 'relation_type': 'has', 'order_id': '123457'}]
    # data['birth_date'] = [{'birth_date': '6464581282', 'relation_type': 'has', 'order_id': '123457'}]
    # data['name'] = [{'name': 'tanveen', 'relation_type': 'has', 'order_id': '123457'}]
    # data['agent'] = [{'agent_id': '1001', 'relation_type': 'created', 'order_id': '123457'}]
    # data['customer_fuse'] = [{'customer_id': 'ABCD', 'relation_type': 'cancelled', 'order_id': '123457'}]
    # data['contact'] = [
    #     {'contact_id': 'T1001', 'master_contact_id': '001', 'phone': '6464581282', 'order_relation_type': 'scheduled',
    #      'phone_relation_type': 'has', 'order_id': '123457'}]
    #
    # print(input)
    return
import pandas as pd
from Logging import Logging

character_lower_req = ['name','address','email','city','country','state','address','relation_type','phone_type','address_type']
# from read_From_Fuse import order_df as fuse_order_df
# from read_From_Fuse import order_name as name
# from read_From_Fuse import order_address as address

from node_creator import create_missing_node_data
from node_creator import push_to_be_created_nodes


#
# #----------------check order node--------#
# fuse_order_df.rename(columns={"id": "order_id"},inplace=True)
#
# graph_order_df = fuse_order_df.copy(deep=True)
#
# #check order consistency
# fuse_df_crude=fuse_order_df[1:5]
# graph_df_crude=graph_order_df[1:5]

#print(fuse_df_crude,graph_df_crude)


#make id as index

#print(fuse_df_crude,graph_df_crude)


#change fuse frame such that a complete record is changed and a record has some changes
# fuse_df_changed=fuse_order_df[1:5]
#
# #change a value of existing record
# fuse_df_changed['source_publisher'][1]='MARTECH'
#
# #add a new record
# fuse_df_changed.loc[len(fuse_df_changed)+1]=["ADT-2213","2018-01-31 14:10:12","2018-01-31 14:10:12","FUSE"]
# #
#
#
# def compare_dated_orders(fuse_df,graph_df):
#
#     #make id as a key
#     fuse_df = fuse_df.set_index("id", drop=False)
#     graph_df = graph_df.set_index("id", drop=False)
#
#     order_check_flag = graph_df.equals(fuse_df)
#
#     #print(fuse_df.index)
#     #print(fuse_df.loc[3000028])
#
#     if not order_check_flag:
#         print("difference in frames")
#         # handle case if id is string
#         # TODO
#         for id_val in fuse_df.index:
#             if id_val in graph_df.index:
#                 for column in graph_df:
#                     if fuse_df.loc[id_val][column] != graph_df.loc[id_val][column]:
#                         print(fuse_df.loc[id_val])
#                         break
#                 pass
#             else:
#                 #all to the list to be added
#                 print(fuse_df.loc[id_val])
#
#     else:
#         print("both frames are similar")
#
#     return 0
#check if two frames are same
#print(compare_dated_orders(fuse_df_crude,graph_df_crude))
#check when two frames are different


# print(compare_dated_orders(fuse_df_changed,graph_df_crude))


#-------------------- CHECK NAME NODE -------------------_#
# name=name[1:10]
# name_graph = name.copy(deep=True)
# name_fuse = name.copy(deep=True)
#
# name_fuse_changed = name_fuse.copy(deep=True)
# name_fuse_changed['name'][1] = 'Vishal Pandey'
# name_fuse_changed.loc[10]=["3000030","2018-01-31 16:21:05", "2018-01-31 16:21:05","Tanveen Singh Bharaj","has"]
#
#
# def checkFrames2(fuse_input_df,graph_input_df,type):
#     fuse_input_df = fuse_input_df.set_index("order_id", drop=False)
#     graph_input_df = graph_input_df.set_index("order_id", drop=False)
#     check_flag  = graph_input_df.equals(fuse_input_df)
#     if check_flag:
#         print("input frames are same")
#     else:
#         print("input frame different")
#         for id_val in fuse_input_df.index:
#             if id_val in graph_input_df.index:
#                 for column in graph_input_df:
#                     if column != 'created_at' or column != 'updated_at':
#                         if fuse_input_df.loc[id_val][column] != graph_input_df.loc[id_val][column]:
#                             create_missing_node_data(type,fuse_input_df.loc[id_val])
#                             break
#
#             else:
#                 #all to the list to be added
#                 create_missing_node_data(type,fuse_input_df.loc[id_val])
#
#     return 0

#check with same frames
#checkFrames(name_fuse,name_graph)


#-------------------- CHECK ADDRESS NODE -------------------_#
# address=address[1:10]
# address_graph = address.copy(deep=True)
# address_fuse = address.copy(deep=True)
#
# address_fuse_changed = address_fuse.copy(deep=True)
# address_fuse_changed['address'][1] = '111 S 120 E'
# address_fuse_changed['address'][2] = 'wdw S dw E'
# address_fuse_changed['address'][3] = 'wdwdwdw S dw E'
# address_fuse_changed.loc[10]=["Salt Lake City","UT","1","84102","3000027","2018-01-31 14:24:20" ,"2018-01-31 14:24:20","has","470 S 1300 E"]
# # address_fuse_changed.loc[10]=["Odgen","UT","1","84112","3000034","2018-01-31 14:24:20" ,"2018-01-31 14:24:20","has","470 S 1300 E"]
# address_fuse_changed.loc[11]=["Provo","UT","1","84102","3000001","2018-01-31 14:24:20" ,"2018-01-31 14:24:20","has","Main Street"]

# print("address_fuse_changed")
# print(address_fuse_changed)

def to_str(val):
    return str(val)

def checkFrames(fuse_input_df,graph_input_df,type):
    if fuse_input_df.empty:
        return
    if "order_id" in fuse_input_df.columns:
        fuse_input_df['order_id'] = fuse_input_df['order_id'].apply(to_str)
        fuse_input_df = fuse_input_df.set_index("order_id", drop=False)
    if "agent_id" in fuse_input_df.columns:
        fuse_input_df['agent_id'] = fuse_input_df['agent_id'].apply(to_str)
    if "order_id" in graph_input_df.columns:
        graph_input_df = graph_input_df.set_index("order_id", drop=False)
    logging = Logging(__name__)
    logging.set_log_message("Checking Data in the " + type, 'info')
    seen = []
    check_flag  = graph_input_df.equals(fuse_input_df)
    if check_flag:
        print("input frames are same")
    else:
        print("input frame different")

        for id_val in fuse_input_df.index:
            if id_val in graph_input_df.index:
                fuse_id_data=fuse_input_df.loc[id_val]
                graph_id_data=graph_input_df.loc[id_val]
                fuse_shape=fuse_id_data.shape
                graph_shape=graph_id_data.shape
                #handling all with singe address but chnages values
                if len(fuse_shape)!=2 and len(graph_shape)!=2 and id_val not in seen:
                    seen.append(id_val)
                    for column in graph_input_df:
                        if column != 'created_at' and column != 'updated_at':
                            if column in character_lower_req:
                                fuse_id_data[column].lower()
                                graph_id_data[column].lower()
                            if fuse_id_data[column]!=graph_id_data[column]:
                                create_missing_node_data(type, fuse_input_df.loc[id_val])
                elif id_val not in seen:
                    seen.append(id_val)
                    # print(fuse_id_data)
                    # print(graph_id_data)
                    for i in range(len(fuse_id_data)):
                        temp=fuse_id_data.iloc[[i]]
                        if len(graph_shape)!=2:
                            #print(graph_id_data)
                            for column in temp:
                                if column != 'created_at' and column != 'updated_at':
                                    if temp[column][0] != graph_id_data[column]:
                                        create_missing_node_data(type,temp.loc[id_val])
                                        break
                        # print(temp)


            else:
                if id_val not in seen:
                    seen.append(id_val)
                    count_rows=fuse_input_df.loc[id_val].shape
                    if len(count_rows)>1:
                        data=fuse_input_df.loc[id_val]
                        for i in range(len(data)):
                            temp=data.iloc[[i]]
                            create_missing_node_data(type, temp.loc[id_val])

                    else:
                        create_missing_node_data(type, fuse_input_df.loc[id_val])

        # for id_val in fuse_input_df.index:
        #     print(fuse_input_df.loc[(id_val)].count)

            # print(graph_input_df.index)
            # if id_val in graph_input_df.index:
            #     fuse_data = fuse_input_df.loc[str(id_val)]
            #     graph_data = graph_input_df.loc[str(id_val)]
            #
            #     for i in range(len(fuse_data)):
            #         for column in fuse_data:
            #             if fuse_data[column] != graph_data[column]:
            #                 create_missing_node_data(type, fuse_input_df.loc[id_val])
            # else:
            #     create_missing_node_data(type, fuse_input_df.loc[id_val])

        # explored = []
        # for id_val in fuse_input_df.index:
        #     if id_val in graph_input_df:
        #         if id_val not in explored:
        #             explored.append(id_val)
        #             all_values_fuse=fuse_input_df.loc[id_val]
        #             all_values_graph=graph_input_df.loc[id_val]
        #             multiple_fuse = all_values_fuse.shape
        #             multiple_graph = all_values_graph.shape
        #             if len(multiple_fuse) < 2 and len(multiple_fuse) < 2:
        #                 if all_values_graph.equals(all_values_fuse):
        #                     pass
        #                 else:
        #                     create_missing_node_data(type, fuse_input_df.loc[id_val])
        #             else:
        #                 if all_values_graph.equals(all_values_fuse):
        #                     pass
        #                 else:
        #                     create_missing_node_data(type, fuse_input_df.loc[id_val])
        #
        #             if all_values_graph.equals(all_values_fuse):
        #                 pass
        #             else:
        #                 print(all_values_fuse,all_values_graph)
        #     else:
        #         create_missing_node_data(type, fuse_input_df.loc[id_val])
        #
        #


            # multiple=fuse_input_df.loc[id_val].shape
            #
            # if len(multiple) == 2 and id_val not in explored:
            #     explored.append(id_val)
            #     multiple_values=fuse_input_df.loc[id_val]
            #     pass
            #
            # if id_val in graph_input_df.index:
            #
            #     # for column in graph_input_df:
            #     #     a=fuse_input_df.loc[id_val][column]
            #     #     print(fuse_input_df.loc[id_val][column])
            #     #     if fuse_input_df.loc[id_val][column] != graph_input_df.loc[id_val][column]:
            #     #         create_missing_node_data(type,fuse_input_df.loc[id_val])
            #     #         break
            #     #     else:
            #     #         print(fuse_input_df.loc[id_val][column],graph_input_df.loc[id_val][column])
            #     pass
            #
            # else:
            #     #all to the list to be added
            #     create_missing_node_data(type,fuse_input_df.loc[id_val])

    return 0

#check with same frames
#checkFrames(name_fuse,name_graph)
#-------------END OF ADDRESS NODE ------#
#check with inconsistent frames
#pass the type of frame like name, address
# checkFrames(name_fuse_changed,name_graph,"name")
# checkFrames(address_fuse_changed,address_graph,"address")
# checkFrames(fuse_df_changed,graph_df_crude,"order")
# push_to_be_created_nodes()




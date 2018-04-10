from datetime import datetime, timedelta
from read_From_Fuse import fetch_from_fuse
from clearinghouse import ClearingHouseDb as ClearingHouseDb
from backfilling_clearinghouse import  BackFillingClearingHouse as BackFillingClearingHouse
from Logging import Logging
from comparator import checkFrames
from node_creator import push_to_be_created_nodes

def main():
    uri = "bolt://uat-neo4j.clearlinkdata.com:7687"
    user = "neo4j"
    password = "infoman!1cl"

    now = datetime.now()
    now = now.replace(hour=11, minute=59, second=00, microsecond=00)
    ending_date = now
    starting_date = (now - timedelta(days=23))
    starting_date_timestamp = int(starting_date.timestamp())*1000
    ending_date_timestamp = int(ending_date.timestamp())*1000



    # now = datetime.now()
    # now = now.replace(hour=11, minute=59, second=00, microsecond=00)
    # ending_date = now.timestamp()
    # # print(ending_date)
    clearinghouse_obj= None
    # fuse_data = None
    try:
        clearinghouse_obj = ClearingHouseDb(uri, user, password)
        graph_data = clearinghouse_obj.get_last_one_day_data_from_clearinghouse(starting_date_timestamp,ending_date_timestamp)
        fuse_data = fetch_from_fuse(starting_date,ending_date)
        print(graph_data["order"])
        print(fuse_data["order"])

        checkFrames(fuse_data["order"], graph_data["order"], "order")
        checkFrames(fuse_data["name"], graph_data["name"], "name")
        checkFrames(fuse_data["address"], graph_data["address"], "address")
        checkFrames(fuse_data["phone"], graph_data["phone"], "phone")
        checkFrames(fuse_data["agent"], graph_data["agent"], "agent")
        checkFrames(fuse_data["contact"], graph_data["contact"], "contact")
        checkFrames(fuse_data["customer_fuse"], graph_data["customer_fuse"], "customer_fuse")
        checkFrames(fuse_data["email"], graph_data["email"], "email")
        inconsistent_data = push_to_be_created_nodes()
        backfill_obj = BackFillingClearingHouse(uri, user, password)
        backfill_obj.backfill_clearinghouse(inconsistent_data)

    except Exception as e :
        # pass
        logging = Logging(__name__)
        logging.set_log_message(str(e), 'error')





    # print(clearinghouse_data)



if __name__ == "__main__":
    main()
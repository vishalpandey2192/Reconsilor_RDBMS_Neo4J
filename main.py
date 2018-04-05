from datetime import datetime
from read_From_Fuse import fetch_from_fuse
from clearinghouse import ClearingHouseDb as ClearingHouseDb
from backfilling_clearinghouse_2 import  BackFillingClearingHouse as BackFillingClearingHouse
from Logging import Logging

def main():
    uri = "bolt://uat-neo4j.clearlinkdata.com:7687"
    user = "neo4j"
    password = "infoman!1cl"

    now = datetime.now()
    now = now.replace(hour=11, minute=59, second=00, microsecond=00)
    ending_date = now.timestamp()
    # print(ending_date)
    clearinghouse_obj= None
    try:
        clearinghouse_obj = ClearingHouseDb(uri, user, password)
        graph_data = clearinghouse_obj.get_last_one_day_data_from_clearinghouse()
        fuse_data = fetch_from_fuse()
        print(graph_data["order"])
        print(fuse_data["order"])
        backfill_obj = BackFillingClearingHouse(uri, user, password)
        backfill_obj.backfill_clearinghouse(data={})
    except :
        # pass
        logging = Logging(__name__)
        logging.set_log_message("The client is unauthorized due to authentication failure", 'error')



    # print(clearinghouse_data)



if __name__ == "__main__":
    main()
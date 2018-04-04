from datetime import datetime
from clearinghouse import ClearingHouseDb as ClearingHouseDb
from backfilling_clearinghouse import  BackFillingClearingHouse as BackFillingClearingHouse

def main():
    uri = "bolt://uat-neo4j.clearlinkdata.com:7687"
    user = "neo4j"
    password = "infoman!1cl"
    now = datetime.now()
    now = now.replace(hour=11, minute=59, second=00, microsecond=00)
    ending_date = now.timestamp()
    print(ending_date)
    clearinghouse_obj = ClearingHouseDb(uri, user, password)
    clearinghouse_data = clearinghouse_obj.get_last_one_day_data_from_clearinghouse()
    # print(clearinghouse_data)

    backfill_obj = BackFillingClearingHouse(uri, user, password)
    backfill_data = backfill_obj.backfill_clearinghouse(data={})

if __name__ == "__main__":
    main()
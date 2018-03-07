
from clearinghouse import ClearingHouseDb as ClearingHouseDb
from backfilling_clearinghouse import  BackFillingClearingHouse as BackFillingClearingHouse
import pandas as pd

def main():
    uri = "bolt://stage-neo4j.clearlinkdata.com:7687"
    user = "tanveen"
    password = "tanveenisgreatvishalisaight"

    clearinghouse_obj = ClearingHouseDb(uri, user, password)
    clearinghouse_data = clearinghouse_obj.get_last_one_day_data_from_clearinghouse()
    print(clearinghouse_data)

    backfill_obj = BackFillingClearingHouse(uri, user, password)
    backfill_data = backfill_obj.backfill_clearinghouse(data={})

if __name__ == "__main__":
    main()
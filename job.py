import datetime 
from classes import DataParser, SparkWorker, InventoryCursor
from time import sleep 
from os.path import join 


JARPATH = "/Users/alex/Desktop/postgresql-42.5.1.jar"
SLEEP_TIME = 86400 / 2 #every 12 hours 
DATA_DIRNAME = "warehoused_data"
ATTRACTION_DATA_FILENAME = "attraction_data_warehoused_event_data.json"
EVENT_DATA_FILENAME = "event_data_warehoused_event_data.json"
ATTRACTION_DF_FILENAME = join(DATA_DIRNAME, "attractions_df.csv")
EVENT_DF_FILENAME = join(DATA_DIRNAME, "events_df.csv")
CURRENT_TIME = str(datetime.datetime.today().strftime("%Y%m%d_%H%M%S"))
EVENTS_TABLENAME = "events_" + CURRENT_TIME
ATTRACTIONS_TABLENAME = "attractions_" + CURRENT_TIME
INVENTORY_TABLENAME = "inventory"
AGGREGATED_TABLENAME = "aggregatedData_" + CURRENT_TIME


def EXECUTE_DATAPARSER_MAIN(dp:DataParser=None):
    if dp is None:
        dp = DataParser()
    attractions = dp.parse_attraction_data(ATTRACTION_DATA_FILENAME, 200, save_filename=ATTRACTION_DF_FILENAME)
    events = dp.parse_event_data(EVENT_DATA_FILENAME, 200, save_filename=EVENT_DF_FILENAME)
    return attractions, events 
    
    
def EXECUTE_SPARKERWORKER_MAIN(sparkworker:SparkWorker=None):
    #here, agg_df represents "aggregated dataframe" aka the primary "dataseed" that we join all tables to
    if sparkworker is None:
        sparkworker = SparkWorker()
    spark = sparkworker.spark_start(JARPATH)
    events_df = sparkworker.write_dataframe_to_postgres(spark, 
                                                        sparkworker.read_dataframe(spark, EVENT_DF_FILENAME), 
                                                        EVENTS_TABLENAME, 
                                                        save_csv=True)
    attractions_df = sparkworker.write_dataframe_to_postgres(spark, 
                                                             sparkworker.read_dataframe(spark, ATTRACTION_DF_FILENAME), 
                                                             ATTRACTIONS_TABLENAME, 
                                                             save_csv=True, 
                                                             spark_df_dirname="attraction_spark_df") 
    inventory_df = sparkworker.write_dataframe_to_postgres(spark, 
                                            spark.createDataFrame(InventoryCursor().execute_inventory_call()), 
                                            tablename=INVENTORY_TABLENAME, 
                                            save_csv=True, 
                                            spark_df_dirname="inventory_spark_df")
    aggregate_df = sparkworker.write_dataframe_to_postgres(spark, 
                                                           sparkworker.join_dfs_on_feature(events_df, inventory_df, "eventId", "event_id"), 
                                                           tablename=AGGREGATED_TABLENAME, 
                                                           save_csv=True, 
                                                           spark_df_dirname="aggregatedData_spark_df")
    sparkworker.join_on_attraction_id(attractions_df, aggregate_df)
    
    
    

if __name__ == "__main__":
    try:
        EXECUTE_DATAPARSER_MAIN()
        EXECUTE_SPARKERWORKER_MAIN()
        print(f"\033[93mJob done with job! Sleeping for {SLEEP_TIME} seconds...\033[1m")
        #sleep(SLEEP_TIME)
    except ConnectionError as e:
        print("Sorry! must be connected to an internet gateway to use the Ticketmaster API!")
        print(e)
    
    
        


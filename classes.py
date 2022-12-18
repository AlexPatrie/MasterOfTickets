import os 
import requests
import json
import pandas as pd 
import pyspark.pandas as pp 
import datetime 
import hashlib
from typing import Optional
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *


class InventoryCursor:
    
    def __init__(self,
                 warehoused_data_dir="warehoused_data",
                 config_filepath=None):
        self.api_url = 'https://app.ticketmaster.com/'
        self.warehoused_data_dir = warehoused_data_dir
        
    
    def _read_config(self, json_filepath):
        with open(json_filepath) as f:
            data = json.load(f)
        key, secret = data.values()
        return (key, secret)
    
    
    def _generate_inventory_api_call_string(self, config_filepath, package="inventory-status", version="v1",
                                    resource="availability?", event_ids=None, api_key=None):
        #ids = [v for i, v in pd.read_csv("warehoused_data/events_df.csv")["event_id"].items()]
        ids = "events=" + ",".join(str(e) for e in [v for i, v in pd.read_csv("warehoused_data/events_df.csv")["event_id"].items()])
        url = os.path.join(self.api_url, package, version, resource) + ids + "&apikey="
        config = self._read_config(config_filepath)[0]
        url += config
        return url 
    
    
    def execute_inventory_call(self, saved_data_filename="warehoused_inventory_data.json"):
        url = self._generate_inventory_api_call_string(config_filepath="login.json")
        raw_data_list = requests.post(url).json()
        with open(os.path.join(self.warehoused_data_dir, saved_data_filename), "w") as fp:
            json.dump(raw_data_list, fp, indent=4)
        return raw_data_list
##########################################################################
    
    
    
class DiscoveryCursor:
    
    def __init__(self,
                 config_filepath=None,
                 fetch_data=False,
                 warehoused_data_dir="warehoused_data",
                 batch_size:Optional[int]=20,
                 consumer_key:Optional[str]=None,
                 consumer_secret:Optional[str]=None,
                 json_filepath:Optional[str]="/Users/alex/Desktop/roseChain/warehoused_data/warehoused_event_data.json",
                 name_filter:Optional[str]=None,
                 fetchby_events=False,
                 fetchby_attractions=False):
        
        self.fetchby_events = fetchby_events
        self.fetchby_attractions = fetchby_attractions
        self.api_url = 'https://app.ticketmaster.com/'
        self.warehoused_data_dir = warehoused_data_dir
        assert batch_size <= 200 
        self.batch_size = batch_size
        if config_filepath is None:
            self.consumer_key = consumer_key
            self.consumer_secret = consumer_secret
        if fetchby_events:
            api_call_string = self.generate_api_call_string(config_filepath, event_name=name_filter)
        elif fetchby_attractions:
            api_call_string = self.generate_api_call_string(config_filepath, resource="attractions.json?")
        if fetch_data:
            self.data_dict = self.execute_request_to_json(api_call_string, self.warehoused_data_dir)
        else:
            with open(json_filepath) as f:
                self.data_dict = json.load(f)
        #parse out df by feature?
           
           
    def _read_config(self, json_filepath):
        with open(json_filepath) as f:
            data = json.load(f)
        key, secret = data.values()
        return (key, secret)
    
    
    def generate_api_call_string(self, config_filepath=None, batch_size=None, package="discovery", version="v2", 
                                 resource="events.json?", event_name=None, api_key:Optional[str]=None):
        url = os.path.join(self.api_url, package, version, resource)
        if batch_size is None:
            batch_size = self.batch_size
        size = f"size={str(batch_size)}"
        if event_name is not None:
            url = url + f"&keyword={event_name}"
        url = url + size + "&apikey="
        if config_filepath is None:
            config = api_key
        elif config_filepath is None and api_key is None:
            config = self.consumer_key
        else:
            config = self._read_config(config_filepath)
        url = url + config[0]
        return url 
    #/discovery/v2/attractions.json?size=1&apikey=TinnDOaeOYCxYJOJo1vPIuNuzZqp6pWy
    
    
    def _generate_inventory_api_call(self, config_filepath, package="inventory-status", version="v1",
                                    resource="availability?", event_ids=None, api_key=None):
        #ids = [v for i, v in pd.read_csv("warehoused_data/events_df.csv")["event_id"].items()]
        ids = "events=" + ",".join(str(e) for e in [v for i, v in pd.read_csv("warehoused_data/events_df.csv")["event_id"].items()])
        url = os.path.join(self.api_url, package, version, resource) + ids + "&apikey="
        config = self._read_config(config_filepath)[0]
        url += config
        return url 
    
    
    def execute_inventory_request(self, saved_data_filename="warehoused_inventory_data.json"):
        url = self._generate_inventory_api_call(config_filepath="login.json")
        raw_data_list = requests.post(url).json()
        with open(os.path.join(self.warehoused_data_dir, saved_data_filename), "w") as fp:
            json.dump(raw_data_list, fp, indent=4)
    
    
    def execute_request_to_json(self, api_call_string, data_dir, json_filename="warehoused_event_data.json"):
        raw_data = requests.get(api_call_string)
        data_dict = raw_data.json()
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)
        if self.fetchby_attractions:
            json_filename = "attraction_data_" + json_filename
        elif self.fetchby_events:
            json_filename = "event_data_" + json_filename
        with open(os.path.join(data_dir, json_filename), 'w') as f:
            json.dump(data_dict, f, indent=4)
        return data_dict
    
    
    def _GET_events_per_country(self, country="US"):
        key = self._read_config("login.json")[0]
        url = f"https://app.ticketmaster.com/discovery-feed/v2/events.json?apikey={key}&countryCode={country}"
        raw_event_request = requests.get(url)
        raw_event_data_dict = raw_event_request.json()
        with open(os.path.join(self.warehoused_data_dir, f"all{country}_events.json"), "w") as f:
            raw_data_dict = json.dump(raw_event_data_dict, f, indent=4)
        return raw_data_dict 
##################END TICKETMASTERCURSOR CLASS@################################################ 



class DataParser:
    def __init__(self, fetch_data:bool=True):
        """NOTE: DataParser parses pd.DataFrame() types ONLY"""
        self.fetch_data = fetch_data
    
    def parse_attraction_data(self, raw_data_filename, batch_size, config_filepath="login.json", save_filename=None):
        tmAttractions = DiscoveryCursor(config_filepath=config_filepath, batch_size=batch_size, fetch_data=self.fetch_data, fetchby_attractions=True)
        with open(os.path.join("warehoused_data", raw_data_filename), "r") as fp:
            data_list = json.load(fp)["_embedded"]["attractions"]
        working_data_dict = {}
        for i, event in enumerate(data_list):
            working_data_dict[i+1] = event
        extract = lambda f: [e[f] for e in working_data_dict.values()]
        dataseed = {"attraction_id":extract("id"), "attraction_name":extract("name"), 
                    "total_upcoming_attraction_events":[e["_total"] for e in extract("upcomingEvents")],
                    "upcoming_attraction_ticketmaster_events":[e["ticketmaster"] if "ticketmaster" in e.keys() \
                                                                                else "NA" for e in extract("upcomingEvents")],
                    "attraction_genre":[t[0]["genre"]["name"] for t in extract("classifications")]}
        df = pd.DataFrame(dataseed)
        df.to_csv(save_filename)
        return df 
 
 
    def parse_event_data(self, raw_data_filename, batch_size:Optional[int]=100, config_filepath="login.json", save_filename=None):
        tmEvents = DiscoveryCursor(config_filepath=config_filepath, batch_size=batch_size, fetch_data=self.fetch_data, fetchby_events=True)
        with open(os.path.join("warehoused_data", raw_data_filename), "r") as fp:
            event_data_list = json.load(fp)["_embedded"]["events"]
        working_data_dict = {}
        for i, event in enumerate(event_data_list):
            working_data_dict[i+1] = event
        extract_single_feature = lambda f: [e[f] for e in working_data_dict.values()] 
        extract_inner_list = lambda f1, f2, outer: [t[0][f1][f2] if type(t) == list else t[f1][f2] for t in extract_single_feature(outer)]
        extract_dicts = lambda f, l: [e[f] if f in e.keys() else "NA" for e in l]
        extract_dict_feature = lambda f, l: [t[0][f] if type(t)==list else t[f] for t in l]
        priceranges = self._extract_outer_feature("priceRanges", event_data_list, priceranges=True)
        sales_list = self._extract_outer_feature("sales", event_data_list, sales=True)
        public = self._fill_empty_feature(extract_dicts("public", sales_list), "startDateTime", "endDateTime")
        presales = self._transform_presales(sales_list)
        extract_promoter = lambda f, l: [{'id':'NA', 'name':'NA', 'description':'NA'} if f not in e.keys() else e[f] for e in l]
        event_promoters = [e["name"] for e in extract_promoter("promoter", event_data_list)]
        dataseed = {"event_name":extract_single_feature("name"), 
                    "event_id":extract_single_feature("id"),
                    "event_type":extract_inner_list("genre", "name", "classifications"), 
                    "trans_min":extract_dict_feature("min", priceranges), 
                    "trans_max":extract_dict_feature("max", priceranges),
                    "presale_start":extract_dict_feature("startDateTime", presales),
                    "presale_end":extract_dict_feature("endDateTime", presales),
                    "pub_sale_start":extract_dict_feature("startDateTime", public),
                    "pub_sale_end":extract_dict_feature("endDateTime", public),
                    "event_date":extract_inner_list("start", "localDate", "dates"),
                    "event_promoter":event_promoters, 
                    "event_attraction_ids":[str(e).replace("[","").replace("]","").replace("'", "") for e in self._parse_attraction_ids(event_data_list)]}
        df = pd.DataFrame(dataseed)
        df.to_csv(save_filename)
        return df 
    
    
    def _extract_outer_feature(self, feature_name:str, datalist, priceranges=False, sales=False):
        feature_list = []
        if priceranges:
            null = {"type":"NA", "currency":"NA", "min":0, "max":0}
        elif sales:
            null = {"startDateTime":"NA", "startTBD":"NA", "startTBA":"NA", "endDateTime":"NA"}
        for i, event in enumerate(datalist):
            if type(event) == list:
                for v in event:
                    feature_list.append(v[feature_name])
            elif feature_name in event.keys():    
                feature_list.append(event[feature_name])
            else:
                feature_list.append(null)
        return feature_list
    
    
    def _fill_empty_feature(self, datalist, feature_name1:str, feature_name2:str=None, feature_name3:str=None):
        for e in datalist:
            if feature_name1 not in e.keys():
                e[feature_name1] = "NA"
            if feature_name2 is not None and feature_name2 not in e.keys():
                e[feature_name2] = "NA"
            if feature_name3 is not None and feature_name3 not in e.keys():
                e[feature_name3] = "NA"
        return datalist
    
    
    def _transform_presales(self, sales_list):
        presales = []
        for event in sales_list:
            if "presales" in event.keys():
                presales.append(event["presales"])
            else:
                presales.append({"description":"NA", "url":"NA", "startDateTime":"NA", "startTBD":"NA", "startTBA":"NA", "endDateTime":"NA", "name":"NA"})
        return presales
    
    
    def _parse_attraction_ids(self, datalist):
        _attractions = [e["_embedded"]["attractions"] for e in datalist]
        attractions = {}
        attractions__ = []
        for i, e in enumerate(_attractions):
            attractions[i] = [x["id"] for x in e]
        for a in attractions:
            attractions__.append(attractions[a])
        return attractions__
##########################################END DATA PARSER CLASS#################################




class SparkWorker:
    
    __config_json_filepath = "spark_config.json"
    __app_name = "roseChain"
    
    def __init__(self, 
                 config_json_filepath=__config_json_filepath,
                 app_name=__app_name):
        self.app_name = app_name
        config_filepath = self._create_configuration(config_json_filepath)
        self.config_dict = self._read_configuration_file(config_filepath)
        self.connection_url = self._extract_connection_url(self.config_dict)
      
        
    def spark_start(self, jdbc_jarpath, master_url="local[*]", appname=None, log_level="ERROR"):
        """instantiate a SparkSession through the use of a jdbc jarpath. Jar MUST be added to your postgres PATH to be functional"""
        def create_session(jdbc_jarpath, master, app_name) -> SparkSession:
            spark = SparkSession.builder.config("spark.jars", jdbc_jarpath).master(master).appName(app_name).getOrCreate()                                       
            return spark 
        if appname is None:
             appname = self.app_name
        spark = create_session(jdbc_jarpath, master_url, appname)
        self._set_logging(spark, log_level)       
        return spark    
        
        
    def _set_logging(self, spark:SparkSession, log_level:Optional[str]=None) -> None:
        spark.sparkContext.setLogLevel(log_level) if isinstance(log_level, str) else None 
            
            
    def _create_configuration(self, spark_config_filepath) -> str:
        config = {"driver":"jdbc", "dialect":"postgresql", "hostname":"localhost", "port_id":"5432", "db_name":"MasterOfTickets",
                  "driver_type":"org.postgresql.Driver", "username":"postgres", "password":"07141989"}
        if not os.path.exists(spark_config_filepath):
            with open(spark_config_filepath, "w") as fp:
                json.dump(config, fp, indent=5)
        else:
            None 
        return spark_config_filepath
    
    
    def _read_configuration_file(self, spark_config_filepath) -> dict:
        #driver:dialect://hostname:portId/db_name
        with open(spark_config_filepath, "r") as fp:
            return json.load(fp)
        
        
    def _extract_connection_url(self, config_dict):
        return config_dict["driver"]+":"+config_dict["dialect"]+"://"+config_dict["hostname"] \
                                    +":"+config_dict["port_id"]+"/"+config_dict["db_name"]
    
    
    def read_dataframe(self, spark:SparkSession, data_filepath:str=None, data=None):
        return spark.read.option("inferSchema", True).option("header", True).csv(data_filepath) if data is None \
                                                                                                else spark.createDataFrame(data, schema=data.columns)

    
    def write_dataframe_to_postgres(self, spark:SparkSession, spark_df, tablename=None, connection_string=None, save_csv=False, spark_df_dirname="event_spark_df"):
        """this method takes a spark df and saves it to a csv AND to postgres
           :param: spark_df: Spark dataframe to be saved
           :param: connection_string(Optional): can be customized with a pg connection string. If none, will default to the vals passed in config dict
           :param: spark_df_dirname(Optional): savename for the spark dataframe partitioned files"""
        year = self._check_fetch_year(2022)
        time = str(datetime.datetime.today().strftime("%Y_%m_%d_%H%M%S"))
        if tablename is None:
            tablename = "warehoused_data_fetch_" + time
        if connection_string is None:
            connection_string = self.connection_url
        if type(spark_df) == pd.DataFrame:
            spark_df = spark.createDataFrame(spark_df)
        spark.sql(f"DROP TABLE IF EXISTS {tablename};")
        spark_df.select("*").write.mode("overwrite").format("jdbc")\
                .option("header", True).option("url", connection_string)\
                .option("driver", self.config_dict["driver_type"]).option("dbtable", tablename)\
                .option("user", self.config_dict["username"])\
                .option("password", self.config_dict["password"]).save()
        spark_df_dirname = os.path.join("warehoused_data", spark_df_dirname)
        write_df = lambda sdn, prt: spark_df.coalesce(prt).write.mode("overwrite").option("header", True).csv(sdn) if spark_df.rdd.getNumPartitions() > prt and save_csv \
                                                else spark_df.write.mode("overwrite").option("header", True).csv(sdn)
        write_df(spark_df_dirname, 1)
        for root, _, files in os.walk(spark_df_dirname):
            for f in files:
                filepath = os.path.join(root, f)  
                if not filepath.endswith("v"):
                    os.remove(filepath) 
        return spark_df
    
    
    def _check_fetch_year(self, fetch_year):
        from datetime import datetime 
        this_year = int(datetime.today().strftime("%Y-%m-%d %H:%M:%S")[:4])
        different_year = False 
        if fetch_year != this_year:
            different_year = True
            difference = this_year - int(fetch_year) 
            print(f"{difference} YEARS DIFFERENCE")
        return str(fetch_year + difference) if different_year else str(fetch_year)
    
    
    def join_dfs_on_feature(self, event_df, inventory_df, old_colname=None, new_colname=None):
        return pd.merge(left=event_df.toPandas(), 
                        right=inventory_df.toPandas()\
                                          .rename({old_colname:new_colname}, axis=1), 
                        on=[new_colname])
        
    
    def join_on_attraction_id(self, attractions_df, aggregated_df):
        attractions = attractions_df.select("attraction_id", "attraction_name").toPandas()                    
        aggregate = aggregated_df[['event_name', 'event_attraction_ids']].toPandas()
        #func = lambda: [attractions["attraction_name"] if a in aggregate["event_attraction_ids"] else None for a in attractions["attraction_id"]]
        #shared_ids = func()   
        print("*")
        print(attractions)
        print(aggregate)
        print(type(attractions), type(aggregate))
###################################END SPARKWORKER CLASS###################################


"""
1. get argmax of trans_max and trans_min for each attraction in attractions_df: (highestMax, lowestMin for a in attractions)
    example: out of all jadakiss concerts, what was the highest selling and lowest selling ticketPrice respectively, as WELL
                                                                                   as the respective event_id for each the lowest
                                                                                   and the highest?
2. f
"""


class TicketBlock:
    def __init__(self):
        '''the use of blockchain for the purpose of securing election polling results'''
        self.chain = []
        self.construct_blockchain(proof=1, previous_hash='0')
       
    def construct_blockchain(self, proof, previous_hash):
        block = {'index' : len(self.chain) + 1,
                 'timestamp' : str(datetime.datetime.now()),
                 'proof' : proof,
                 'previous_hash' : previous_hash}
        self.chain.append(block)
        return block

    def get_previous_block(self):
        last_block = self.chain[-1]
        return last_block 
    
    def proof_of_work(self, previous_proof):
        new_proof = 1
        check_proof = False 
        while check_proof is False:
            hash_operation = hashlib\
                             .sha256(str(new_proof ** 2 - previous_proof ** 2)\
                             .encode()).hexdigest()
            if hash_operation[:4] == '0000':
                check_proof = True
            else:
                new_proof += 1
        return new_proof 

    def hash(self, block):
        encoded_block = json.dumps(block, sort_keys=True).encode()
        return hashlib.sha256(encoded_block).hexdigest()
###########################END BLOCK CLASS#############################    
    


#let us try an event specific search:
#Ticket master api requests have the following format:
    #https://app.ticketmaster.com/{package}/{version}/{resource}.json?apikey=**{API key}
    #for example: https://app.ticketmaster.com/discovery/v1/events.json?apikey=4dsfsf94tyghf85jdhshwge334

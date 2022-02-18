from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DecimalType,DateType,ArrayType
from pyspark.sql.functions import col, concat_ws
import sys
import boto3

# Libraries used 
from pyspark.sql.functions import concat_ws,col,sha2
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import to_date,col
import json
import s3fs
import pyarrow.parquet as pq

sc = SparkSession.builder.appName('Test1').getOrCreate()

#fetching the app-config file to get the respective columns for transformation
def fetchConfig(app_config):
    app_config_data = sc.sparkContext.textFile(app_config).collect()
    dataString = ''.join(app_config_data)
    config_Data = json.loads(dataString)
    return config_Data

class sparkcode:
    def __init__(self,dataset):

        self.source_format=jsonData["ingest-"+dataset]["source"]["file-format"]
        self.source_loc=jsonData["ingest-"+dataset]["source"]["data-location"]
        self.destination_format=jsonData["ingest-"+dataset]["source"]["file-format"]
        self.destination_loc=jsonData["ingest-"+dataset]["destination"]["data-location"]


        #function to read the data from the source
    def read(self,source_loc,data_path,source_format):
        try:
            if(source_format == 'parquet'):
                df = sc.read.parquet(source_loc + "/" + data_path)
                return df
            if(source_format == 'csv'):
                df = sc.read.csv(source_loc + "/" + data_path)
                return df
        except Exception as e:
            return e
   
#function to write the data to the destination
    def write(self,df,destination_loc,destination_format,data_path):
        try:
            if(destination_format == 'parquet'):
                df.write.parquet(destination_loc+'/'+data_path)
            if(destination_format == 'csv'):
                df.write.csv(destination_loc+'/'+data_path)
        except Exception as e:
            return e

    def SparkConfig(self,spark_conf_loc):
        s3 = boto3.resource('s3')

        data = spark_conf_loc.split('//')[1].split('/')
        bucket = data[0]
        key =  "/".join(data[1:])
        
        s3 = boto3.resource('s3')
        content_object = s3.Object(bucket,key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        jsonData=json.loads(file_content)
        spark_property = jsonData[0]
        spark_property = spark_property['Properties']
        config_list = list()
        for i in spark_property:
            config_list.append((i,spark_property[i]))
        sc.sparkContext._conf.setAll(config_list)


if __name__=='__main__':
    app_config = sys.argv[1]
    dataset = sys.argv[2]
    data_path = sys.argv[3]
    spark_config = sys.argv[4]

    # app_config = "s3://abileshlandingzone/conf/app_conf.json"
    # dataset = "Actives"
    # data_path = "2020/Feb/1/final_active_dataset.parquet"

    jsonData = fetchConfig(app_config)
    object1 = sparkcode(dataset)
    object1.SparkConfig(spark_config)
    df = object1.read(object1.source_loc,data_path,object1.source_format)
    object1.write(df,object1.destination_loc,object1.destination_format,data_path)
sc.stop()

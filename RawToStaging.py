from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DecimalType,DateType,ArrayType
from pyspark.sql.functions import col, concat_ws
import sys
import boto3
sc = SparkSession.builder.appName('Test1').getOrCreate()

# Libraries used 
from pyspark.sql.functions import concat_ws,col,sha2
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import to_date,col
import json

#fetching the app-config file to get the respective columns for transformation
def fetchConfig(app_config):
    app_config_data = sc.sparkContext.textFile(app_config).collect()
    dataString = ''.join(app_config_data)
    config_Data = json.loads(dataString)
    return config_Data

class sparkcode:
    def __init__(self,dataset):


        self.mask_source_format=jsonData["mask-"+dataset]["source"]["file-format"]
        self.mask_source_loc=jsonData["mask-"+dataset]["source"]["data-location"]
        self.mask_destination_format=jsonData["mask-"+dataset]["destination"]["file-format"]
        self.mask_destination_loc=jsonData["mask-"+dataset]["destination"]["data-location"]

        self.mask_Columns=jsonData["mask-"+dataset]["masking-cols"]
        self.transform_Columns=jsonData["mask-"+dataset]["datatype-update-cols"]
        self.partition_Columns=jsonData["mask-"+dataset]["partition-cols"]

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
    def write_with_partition(self,df,destination_loc,destination_format,data_path):
        try:
            if(destination_format == 'parquet'):
                df.write.parquet(destination_loc+'/'+data_path)
            if(destination_format == 'csv'):
                df.write.csv(destination_loc+'/'+data_path)
        except Exception as e:
            return e

    def SparkConfig(self,spark_conf_loc):
        s3 = boto3.resource('s3')

        # path = spark_conf_loc.replace(":","").split("/")
        # s3obj = s3.Object(path[2], "/".join(path[3:]))
        # body = s3obj.get()['Body'].read()
        # sparkjson = json.loads(body)
        # spark_property = sparkjson['Properties']
        # config_list = list()



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

class transform:
    def masking(self,df,mask_Columns):
        for columns in mask_Columns:
            df = df.withColumn(columns,sha2(col(columns),256))
        return df

    def casting(self,df,transform_Columns):
        for item in transform_Columns:
            datatype = item.split(':')[1]
            column = item.split(':')[0]
            if(datatype=="DecimalType"):
                scale_value=item.split(':')[3]
                df = df.withColumn(column,df[column].cast(DecimalType(scale=int(scale_value))))
            if(datatype=="StringType" and item.split(':')[2] == "ArrayType"):
                df= df.withColumn(column,concat_ws(",",col(column)))
            if(datatype=="StringType"):
                df= df.withColumn(column,df[column].cast(StringType()))
        return df

if __name__=='__main__':

    # app_config = sys.argv[1]
    # dataset = sys.argv[2]
    # data_path = sys.argv[3]
    # spark_config = sys.argv[4]
    app_config = "s3://abileshlandingzone/conf/app_conf.json"
    dataset = "Actives"
    data_path = "2020/Feb/1/final_active_dataset.parquet"
    spark_config = "s3://abileshlandingzone/conf/spark_conf.json"
    jsonData = fetchConfig(app_config)
    object1 = sparkcode(dataset)
    object2 = transform()
    object1.SparkConfig(spark_config)
    df = object1.read(object1.mask_source_loc,data_path,object1.mask_source_format)
    df = object2.masking(df,object1.mask_Columns)
    df = object2.casting(df,object1.transform_Columns)
    object1.write_with_partition(df,object1.mask_destination_loc,object1.mask_destination_format,data_path)     


#object1.pre_validation(object1.source_loc,data_path,object1.destination_loc)
sc.stop()

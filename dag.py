from airflow import settings
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor

from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
# from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
# from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

from airflow.models import Connection
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from datetime import datetime,timedelta
import decimal
import json
import boto3
import s3fs
import pyarrow.parquet as pq

def get_AppConfig(**kwargs):
        app_config = kwargs['dag_run'].conf.get('app_config')
        data = app_config.split('//')[1].split('/')
        bucket = data[0]
        key =  "/".join(data[1:])
        
        s3 = boto3.resource('s3')
        content_object = s3.Object(bucket,key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        jsonData=json.loads(file_content)
        return jsonData

def CopyFromLandingToRaw(**kwargs):
    ti = kwargs['ti']
    dataset = kwargs['dag_run'].conf.get('dataset')
    data_path = kwargs['dag_run'].conf.get('data_path')
    jsonData = ti.xcom_pull(task_ids='read_AppConfig')
    source_path = jsonData['ingest-'+dataset]['source']['data-location']+'/'+data_path
    dest_path = jsonData['ingest-'+dataset]['destination']['data-location']+'/'+data_path
    s3_client = boto3.client(source_path.replace(":","").split('//')[0])
    print("source_path",source_path)
    print("dest_path",dest_path)

    source_path = source_path.split('//')[1].split('/')
    dest_path = dest_path.split('//')[1].split('/')

    source_bucket = source_path[0]
    source_file_path = "/".join(source_path[1:]) 

    destination_bucket = dest_path[0]
    destination_file_path = "/".join(dest_path[1:])

    print("source_bucket",source_bucket)
    print("source_file_path",source_file_path)
    print("destination_bucket",destination_bucket)
    print("destination_file_path",destination_file_path)
 
    s3_client.copy_object(
        CopySource = {'Bucket': source_bucket, 'Key': source_file_path},
        Bucket = destination_bucket,
        Key = destination_file_path 
        )

def wait_for_cluster_creation(cluster_id):
    emr = boto3.client("emr",region_name='us-east-1')
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)

class LivyConnection(BaseOperator):
    template_fields = ['job_flow_id']
    template_ext = ()
    @apply_defaults
    def __init__(self,
                job_flow_id,
                aws_conn_id='aws_default',
                *args,
                **kwargs)-> None:
        super().__init__(*args, **kwargs)
        self.job_flow_id = job_flow_id,
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()
        response = emr.describe_cluster(ClusterId=self.job_flow_id[0])
        create_conn( "livy_default", "livy", response['Cluster']['MasterPublicDnsName'] ,8998)

def create_conn(conn_id, conn_type, host, port):
    #create a connection object
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        port=port
    )
    # get the session
    session = settings.Session()
    #retrieve the connection and check if connection already exists
    conn_name = session\
    .query(Connection)\
    .filter(Connection.conn_id == conn.conn_id)\
    .first()

    if str(conn_name) == str(conn_id):
        print("Connection", conn_id ,"already created with host: ",conn_name.host)
        session.delete(conn_name)
        session.commit()
    session = settings.Session()
    session.add(conn)
    print(Connection.log_info(conn))
    print("Connection", conn_id," is created with host: ",conn.host)
    session.commit()

def pre_validation_method(**kwargs):
    try:

        app_config = kwargs['dag_run'].conf.get('app_config')
        dataset = kwargs['dag_run'].conf.get('dataset')
        data_path = kwargs['dag_run'].conf.get('data_path')
        # app_config = "s3://abileshlandingzone/conf/app_conf.json"
        # dataset = "Actives"
        # data_path = "/"+"2020/Feb/1/final_active_dataset.parquet"
        data = app_config.split('//')[1].split('/')
        bucket = data[0]
        key =  "/".join(data[1:])
        
        s3 = boto3.resource('s3')
        content_object = s3.Object(bucket,key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        jsonData=json.dumps(file_content)

        fs = s3fs.S3FileSystem()
        df_landingzone =pq.ParquetDataset(jsonData['ingest-'+dataset]['source']['data-location']+'/'+data_path, filesystem=fs).read_pandas().to_pandas()
        df_rawzone =pq.ParquetDataset(jsonData['ingest-'+dataset]['destination']['data-location']+'/'+data_path, filesystem=fs).read_pandas().to_pandas()

        if(df_landingzone.shape[0] ==  df_rawzone.shape[0]):
            for columns in df_rawzone.columns:
                if(df_landingzone[columns].count() == df_rawzone[columns].count()):
                    print("Validation Success!! ")
                else:
                    print("Validation Failed!! ")
        else:
            print("Data Not Available in Raw Zone!! ")
    except Exception as e:
        return e

def post_validation_method():
    try:

        app_config = "s3://abileshlandingzone/conf/app_conf.json"
        dataset = "Actives"
        data_path = "2020/Feb/1/final_active_dataset.parquet"
       
        data = app_config.split('//')[1].split('/')
        bucket = data[0]
        key =  "/".join(data[1:])
        
        s3 = boto3.resource('s3')
        content_object = s3.Object(bucket,key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        jsonData=json.loads(file_content)

   
        fs = s3fs.S3FileSystem()
        df_rawzone =pq.ParquetDataset(jsonData['mask-'+dataset]['source']['data-location']+'/'+data_path, filesystem=fs).read_pandas().to_pandas()
        df_stagingzone =pq.ParquetDataset(jsonData['mask-'+dataset]['destination']['data-location']+'/'+data_path, filesystem=fs).read_pandas().to_pandas()

        if(df_stagingzone.shape[0] != 0):
            for columns in df_stagingzone.columns:
                if(df_rawzone[columns].count() == df_stagingzone[columns].count()):
                    print("Validation Success!! ")
                else:
                    print("Validation Failed!! ")
        else:
            print("Data Not Available in Staging Zone!! ")

    
    
        transform_Columns = jsonData["mask-"+dataset]["datatype-update-cols"]
        for item in transform_Columns:
            datatype = item.split(':')[1]
            column = item.split(':')[0]
            print("datatype !! ",datatype)
            print("column !! ",column)


            if(datatype=="DecimalType"):
                scale_value=item.split(':')[3]
                print("scale_value !! ",scale_value)
                print("column",df_stagingzone[column][0],"type",type(df_stagingzone[column][0]))
                if(isinstance(df_stagingzone[column][0], decimal.Decimal) and (str(abs(df_stagingzone[column][0].as_tuple().exponent)) == scale_value)):
                    print("Validation Success for column : ",column)
                else:
                    print("datatype didn't match")
            if(datatype=="StringType"):
                print("column",df_stagingzone[column][0],"type",type(df_stagingzone[column][0]))
                if(isinstance(df_stagingzone[column][0], str)):
                    print("Validation Success for column : ",column)
                else:
                    print("datatype didnt match")
    except Exception as e:
        return e





default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 2),
    'max_active_runs': 1
}


JOB_FLOW_OVERRIDES = {
    "Name": "Abilesh Cluster",
    "LogUri":"s3://aws-logs-552490513043-us-east-1/elasticmapreduce/j-2XQ65ITPIWU4X/",
    "ReleaseLabel": "emr-5.34.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}, {"Name": "JupyterHub"}, {"Name": "Hive"}, {"Name": "JupyterEnterpriseGateway"}, {"Name": "Zeppelin"}, {"Name": "Livy"}],

    "Instances": {
        "Ec2KeyName": "abilesh_keypair",
        "Ec2SubnetId": "subnet-00e05a665123876c9",
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    'BootstrapActions':[{
            'Name': 'Custom action',
            'ScriptBootstrapAction': {
                'Path': 's3://abileshlandingzone/packages.sh'
            }
        }],
    "JobFlowRole": "ap_ec2-admin",
    "ServiceRole": "ap_emrADMIN",
}

with DAG(
    dag_id='codepipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:


    read_AppConfig = PythonOperator(
    task_id='read_AppConfig',
    python_callable= get_AppConfig,
    )

    # LandingToRaw = PythonOperator(
    # task_id='LandingToRaw',
    # python_callable= CopyFromLandingToRaw,
    # )

    LandingToRaw = LivyOperator(
    task_id='LandingToRaw',
    file='{{ dag_run.conf.get("LandingToRaw") }}',
    class_name='com.example.SparkApp',
        args=[
        '{{ dag_run.conf.get("app_config") }}',
        '{{ dag_run.conf.get("dataset") }}',
        '{{ dag_run.conf.get("data_path") }}',
        '{{ dag_run.conf.get("spark_config") }}',
        ],
    livy_conn_id ='livy_default',
    )
# Create an EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
    )

    t1 = PythonOperator(
        task_id='wait',
        python_callable= wait_for_cluster_creation,
        op_kwargs={'cluster_id':create_emr_cluster.output},
    )

    create_livy_conn = LivyConnection(
        task_id='livy_connection',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}"
        )

    pre_validation = PythonOperator(
        task_id="pre_validation",
        python_callable= pre_validation_method,

    )

    RawToStaging = LivyOperator(
    task_id='RawToStaging',
    file='{{ dag_run.conf.get("RawToStaging") }}',
    class_name='com.example.SparkApp',
        args=[
        '{{ dag_run.conf.get("app_config") }}',
        '{{ dag_run.conf.get("dataset") }}',
        '{{ dag_run.conf.get("data_path") }}',
        '{{ dag_run.conf.get("spark_config") }}',
        ],
    livy_conn_id ='livy_default',
    )

    post_validation = PythonOperator(
        task_id="post_validation",
        python_callable= post_validation_method,
    )

    # Teardown EMR cluster
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
    )

    read_AppConfig  >> create_emr_cluster >> t1 >> create_livy_conn >> LandingToRaw >> pre_validation >> RawToStaging >> post_validation >> cluster_remover

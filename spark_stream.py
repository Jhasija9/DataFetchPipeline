import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cffi.model import StructType
from click import option

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StringType
from sqlalchemy import values
from pyspark.sql.types import StructType, StructField, StringType



def create_keyspace(session):
    #create keyspace here

    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)

    print("Keyspace created successfully!")


def create_table(session):
    #create table here
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """)
    print("Table created successfully!")

def insert_data(session, **kwargs):
    #insert here

    print("Inserting data into table...")
    user_id =kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    post_code = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')
    # dob = kwargs.get('dob')

    try:
        session.execute(""" INSERT INTO spark_sessions.created_users(
        id, first_name, last_name, gender, address, post_code, email, username, dob, registered_date, phone, picture))
        VALUES( % s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              post_code, email, username, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'Error inserting data {e}')


def connect_to_kafka(spark_conn):
    spark_df=None

    try:
        spark_df = spark_conn.readStream\
                   .format('kafka') \
                   .option('kafka.bootstrap.servers', 'localhost:9092') \
                   .option('subscribe', 'users_created') \
                    .option('startingOffsets', 'earliest') \
                    .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_spark_connection():
#creating_spark_connection
    s_conn= None

    try:
        s_conn= SparkSession.builder \
                .appName('SparkDataStreaming')\
                .config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,'
                                              'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,'
                                               'org.scala-lang:scala-library:2.12.18').config('spark.cassandra.connection.host', 'localhost')\
                .getOrCreate()
        s_conn.sparkContext.setLogLevel('ERROR')
        logging.info('Connected to Cassandra')
    except Exception as e:
        logging.error(f"Could not connect to Cassandra: {e}")



    return s_conn



def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField('id', StringType(), False),
        StructField('first_name', StringType(), False),
        StructField('last_name', StringType(), False),
        StructField('gender', StringType(), False),
        StructField('address', StringType(), False),
        StructField('post_code', StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False),
        # StructField("dob", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)")\
          .select(from_json("value", schema).alias("data")).select("data.*")
    print(sel)
    return sel
if __name__ == "__main__":
    spark_conn = create_spark_connection() #create spark connection

    if spark_conn is not None:
        spark_df= connect_to_kafka(spark_conn) #connect kafka with spark
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

    if session is not None:
        create_keyspace(session)
        create_table(session)
        streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
            .option('checkpointLocation', '/tmp/checkpoint')
            .option('keyspace','spark_streams')
            .option('table','created_users').start())

        streaming_query.awaitTermination()



        # insert_data(session)



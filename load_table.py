%python

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import *
from pyspark.sql.types import *

## read data from mount point
read_path = "/bmathew/clickstream/000000_0"

jsonSchema = StructType([
    StructField("http_vhost_name", StringType(), True),StructField("event_time", TimestampType(), True), StructField("page", StringType(), True),
    StructField("user", StringType(), True), StructField("session", StringType(), True) ])

df = spark.read.format("json").schema(jsonSchema).load(read_path) \
    .select(current_timestamp().alias('event_load_time')
            ,from_unixtime(unix_timestamp(from_utc_timestamp(from_unixtime(unix_timestamp()), "PST")),"yyyy-MM-dd HH:mm:ss").alias("event_insert_time")
            ,col("http_vhost_name").alias("domain_name")
            ,regexp_replace(regexp_replace(col("event_time"),'T',' '),'Z','').alias("event_time")
            ,get_json_object('user', '$.browserId').alias('user_id'),get_json_object('page', '$.country').alias('country')
            ,get_json_object('page', '$.division').alias('division'),get_json_object('page', '$.app').alias('page_app')
            ,get_json_object('page', '$.id').alias('page_id'),get_json_object('user', '$.browser').alias('browser')
            ,get_json_object('user', '$.os').alias('os'),get_json_object('user', '$.platform').alias('platform')) \
    .where("country IN ('MX','TW','PE','CO','UY','PR','CL','ID','CA','GB','JP','US','PA')")

df.write.format("parquet").mode("overwrite").saveAsTable("bmathew.test_databricks_api")

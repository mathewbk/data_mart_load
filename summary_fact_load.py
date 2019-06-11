# Databricks notebook source
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import col

read_path = '/mnt/bmathew-clickstream-data/000002_0'

df = spark.read.json(read_path)

parsed_fields = df.select( \
  col('http_vhost_name').alias('domain_name') \
 ,regexp_replace(regexp_replace("event_time","T"," "),"Z","").alias("event_time") \
 ,col('page.country').alias('country') \
 ,col('page.url').alias('page_url') \
 ,col('user.browser').alias('browser'))

parsed_fields.registerTempTable("summary_fact_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists bmathew.summary_fact;
# MAGIC create table bmathew.summary_fact using delta as (select country, count(1) as page_views from summary_fact_temp group by 1
# MAGIC );

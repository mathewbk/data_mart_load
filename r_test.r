# Databricks notebook source
# MAGIC %sql
# MAGIC create table  bmathew.dlt_clickstream_raw_new_sorted as select * from  bmathew.dlt_clickstream_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize bmathew.dlt_clickstream_raw_new_sorted zorder by user_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.*, b.* from bmathew.dlt_clickstream_raw a inner join bmathew.dlt_clickstream_raw_new b on a.user_id = b.user_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.*, b.* from bmathew.dlt_clickstream_raw_zorder_user_id a inner join  bmathew.dlt_clickstream_raw_new_sorted b on a.user_id = b.user_id

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize bmathew.dlt_users zorder by first_name

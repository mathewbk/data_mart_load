# Databricks notebook source
## set those Pub/Sub topic ID's and subscription ID's we want to ingest from
## set at least 1 pair
pub_sub_pairs = [
        ['test-topic-1', 'test-topic-sub-1'],
        ['test-topic-2', 'test-topic-sub-2']]

## location on local machine of JSON service account file
service_account_key = "/tmp/fe-dev-sandbox-117829db4acc.json"

## Google Cloud project ID
project_id = "fe-dev-sandbox"

## set max messages per batch and deadline
## batch more messages together to avoid too many small files
## set this wisely knowing the approximate size in bytes for each record
max_messages_per_batch = 20000
deadline = 60

## target GCS bucket and blob path to write messages
## subdirectories will be created under blob path for each topic
gcs_bucket = "binumathew_dev"
blob_path = "bkm_test"

# COMMAND ----------

from google.api_core import retry
from google.cloud import pubsub_v1
from google.cloud import storage
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import json
import datetime
from datetime import datetime, date
import time


## fields to retrieve from Pub/Sub
columns = ['message_id', 'body', 'attributes', 'publish_time', 'topic_id', 'subscription_id']

## set Spark settings
conf = (
        SparkConf()
        .setMaster("local[*]")
        .set("spark.hadoop.google.cloud.auth.service.account.enable", True)
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", service_account_key))
sc = SparkContext.getOrCreate(conf=conf)

## save DataFrame as Delta to Google Cloud Storage
def write_to_bucket(topic_id, subscription_id, message_df):
  
    save_path = f"gs://{gcs_bucket}/{blob_path}/{topic_id}/{subscription_id}/"
    try:
        message_df.write.mode('append').format("delta").save(save_path)
    except Exception as e:
        print(e)

## Pull messages from one subscription
def pull_message(info):
  
    topic_id = info[0]
    subscription_id = info[1]
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    print(f"Pulling messages from {subscription_path}")
    message_array = []
    message_id_list = []
    with subscriber:
        while True:
            try:
                #list all messages within deadline 
                response = subscriber.pull(request={"subscription": subscription_path, "max_messages": max_messages_per_batch},retry=retry.Retry(deadline = deadline),)
            
            except Exception as e:
                print(f"Error occurred {e}")
                time.sleep(5)
                continue
            
            ack_ids = []
            
            ## get messages 
            for received_message in response.received_messages:
                message = received_message.message
                if(message.message_id not in message_id_list):
                    message_data = (message.message_id, message.data.decode("utf-8", 'ignore') , json.dumps(dict(message.attributes)), str(message.publish_time), topic_id, subscription_id)
                    message_id_list.append(message.message_id)
                    message_array.append(message_data)
                
                ack_ids.append(received_message.ack_id)
            
            ## write messages when length of array size is >= max per batch
            if(len(message_array) >= max_messages_per_batch):
                ## save to Spark DataFrame
                message_df = spark.createDataFrame(message_array, columns)
                ## call function to save DataFrame as Delta file and write to Google Cloud Storage
                write_to_bucket(topic_id, subscription_id, message_df)
                message_array = []
                
            if (len(ack_ids) > 0):
                try:
                   ## Acknowledge the received messages
                   subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
                   message_id_list = []
                except:
                      pass
                                         
## main function
def main(pub_sub_pairs):

    print(f"\nPulling all messages from subscriptions")
    with ThreadPoolExecutor() as executor:
         results = executor.map(pull_message, pub_sub_pairs)

if __name__ == '__main__':

    main(pub_sub_pairs)



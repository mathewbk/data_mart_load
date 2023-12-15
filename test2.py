# Databricks notebook source
df = spark.read.format("delta")load("/tmp/bkm/data/files")

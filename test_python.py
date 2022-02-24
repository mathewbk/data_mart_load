# Databricks notebook source
df = spark.read.format('csv').load("gs://mybucket/mydir")

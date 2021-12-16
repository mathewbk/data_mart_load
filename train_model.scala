// Databricks notebook source
val product_ratings = sqlContext.sql("select * from bmathew.product_ratings")
val Array(training, test) = product_ratings.randomSplit(Array(0.8, 0.2))
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.mlflow.tracking.ActiveRun
import org.mlflow.tracking.MlflowContext
import java.io.{File,PrintWriter}
import org.mlflow.tracking.MlflowClient
import java.nio.file.{Paths,Files}
import org.mlflow.tracking.{MlflowClient,MlflowClientVersion}

 

val experimentName = "/Users/binu.mathew@databricks.com/GCP_CE_Demo"
def getOrCreateExperimentId(client: MlflowClient, experimentName: String) = {try { client.createExperiment(experimentName)} catch { case e: org.mlflow.tracking.MlflowHttpException => {client.getExperimentByName(experimentName).get.getExperimentId} } } 
val mlflow = new MlflowContext()
val client = mlflow.getClient
val experimentId = getOrCreateExperimentId(client, experimentName)
mlflow.setExperimentId(experimentId)
val run = mlflow.startRun()
val runId = run.getId()
val MaxIter = 100
val RegParam = 0.01
val als = new ALS().setMaxIter(MaxIter).setRegParam(RegParam).setUserCol("user_id").setItemCol("product_id").setRatingCol("rating")
run.logParam("MaxIter", MaxIter.toString)
run.logParam("RegParam", RegParam.toString)
val model = als.fit(training)
dbutils.fs.rm("/tmp/binu.mathew@databricks.com/mlflow_demo/recommender-system/spark-als-model", true)
model.write.overwrite().save("/tmp/binu.mathew@databricks.com/mlflow_demo/recommender-system/spark-als-model")
run.logArtifacts(Paths.get("/dbfs/tmp/binu.mathew@databricks.com/mlflow_demo/recommender-system/spark-als-model"),"spark-als-model")
run.endRun()
val user_recommendations = model.recommendForAllUsers(3)

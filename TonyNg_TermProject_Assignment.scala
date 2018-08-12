// Databricks notebook source
// MAGIC %md ## Final Term Project Assignment
// MAGIC ### Name: Tony Yu Ho Ng
// MAGIC ### Student Number: X071252
// MAGIC ### Date: July 28, 2018.
// MAGIC ### E-mail: tonyyhng@gmail.com

// COMMAND ----------

// Assumption, you already have the price and return data uploaded in your databrick workspace
val cm_dataPath = "/FileStore/tables/CM_Price_Return.csv"
val cm = sqlContext.read.format("com.databricks.spark.csv")
  .option("header","true")
  .option("inferSchema", "true")
  .load(cm_dataPath)


val sptsx_dataPath = "/FileStore/tables/GSPTSE_Price_Return.csv"
val sptsx = sqlContext.read.format("com.databricks.spark.csv")
  .option("header","true")
  .option("inferSchema", "true")
  .load(sptsx_dataPath)

val goog_dataPath = "/FileStore/tables/GOOG_Price_Return.csv"
val goog = sqlContext.read.format("com.databricks.spark.csv")
  .option("header","true")
  .option("inferSchema", "true")
  .load(goog_dataPath)


val ry_dataPath = "/FileStore/tables/RY_Price_Return.csv"
val ry = sqlContext.read.format("com.databricks.spark.csv")
  .option("header","true")
  .option("inferSchema", "true")
  .load(ry_dataPath)

val td_dataPath = "/FileStore/tables/TD_Price_Return.csv"
val td = sqlContext.read.format("com.databricks.spark.csv")
  .option("header","true")
  .option("inferSchema", "true")
  .load(td_dataPath)

val snp500_dataPath = "/FileStore/tables/GSPC_Price_Return.csv"
val snp500 = sqlContext.read.format("com.databricks.spark.csv")
  .option("header","true")
  .option("inferSchema", "true")
  .load(snp500_dataPath)


// COMMAND ----------

// take a look at the uploaded price and return data
cm.printSchema
td.printSchema
ry.printSchema

sptsx.printSchema
snp500.printSchema



// COMMAND ----------

display(sptsx)
//display(snp500)
//display(cm)
//display(td)
//display(ry)

// COMMAND ----------

// Step 1: prepare and clean up the data as needed before the analysis
// fill the na values as 0 on the price & return data

val cleaned_cm = cm.selectExpr("Date",
                       "cast(CM_Open as double) CM_Open",
                       "cast(CM_High as double) CM_High",
                       "cast(CM_Low as double) CM_Low",
                       "cast(CM_Close as double) CM_Close",
                       "cast(CM_Adj_Close as double) CM_Adj_Close",
                       "cast(CM_Volume as int) CM_Volume",
                       "cast(CM_Return as double) CM_Return",
                       "cast(CM_Percent_Return as double) CM_Percent_Return")

val cleaned_ry = ry.selectExpr("Date",
                       "cast(RY_Open as double) RY_Open",
                       "cast(RY_High as double) RY_High",
                       "cast(RY_Low as double) RY_Low",
                       "cast(RY_Close as double) RY_Close",
                       "cast(RY_Adj_Close as double) RY_Adj_Close",
                       "cast(RY_Volume as int) RY_Volume",
                       "cast(RY_Return as double) RY_Return",
                       "cast(RY_Percent_Return as double) RY_Percent_Return")

val cleaned_td = td.selectExpr("Date",
                       "cast(TD_Open as double) TD_Open",
                       "cast(TD_High as double) TD_High",
                       "cast(TD_Low as double) TD_Low",
                       "cast(TD_Close as double) TD_Close",
                       "cast(TD_Adj_Close as double) TD_Adj_Close",
                       "cast(TD_Volume as int) TD_Volume",
                       "cast(TD_Return as double) TD_Return",
                       "cast(TD_Percent_Return as double) TD_Percent_Return")


cleaned_cm.printSchema
cleaned_ry.printSchema
cleaned_td.printSchema


// COMMAND ----------

val cleaned_sptsx = sptsx.na.fill(0)
cleaned_sptsx.printSchema()
cleaned_sptsx.count


val cleaned_snp500 = snp500.na.fill(0)
cleaned_snp500.printSchema()
cleaned_snp500.count


// COMMAND ----------

//goog.printSchema

// COMMAND ----------

// join the 2 dataframes together to form a unified view of return data of cm and s&p tsx returns
//val stock_returns = cm.join(sptsx, cm.col("Date") === sptsx.col("Date"))
//only join the data when they have common dates
val cm_tsx_stock_returns = cleaned_cm.join(cleaned_sptsx, Seq("Date"))
display(cm_tsx_stock_returns)
cm_tsx_stock_returns.count
cm_tsx_stock_returns.printSchema

//val tech_stock_returns = goog.join(cleaned_sptsx, Seq("Date"))


// COMMAND ----------

// join the 2 dataframes together to form a unified view of return data of cm and s&p tsx returns
val ry_tsx_stock_returns = cleaned_ry.join(cleaned_sptsx, Seq("Date"))
display(ry_tsx_stock_returns)
ry_tsx_stock_returns.count
ry_tsx_stock_returns.printSchema


// COMMAND ----------

// join the 2 dataframes together to form a unified view of return data of cm and s&p tsx returns
val td_tsx_stock_returns = cleaned_td.join(cleaned_sptsx, Seq("Date"))
display(td_tsx_stock_returns)
td_tsx_stock_returns.count
td_tsx_stock_returns.printSchema


// COMMAND ----------

// join the 2 dataframes together to form a unified view of return data of cm and s&p tsx returns
val cm_snp500_stock_returns = cleaned_cm.join(cleaned_snp500, Seq("Date"))
display(cm_snp500_stock_returns)
cm_snp500_stock_returns.count
cm_snp500_stock_returns.printSchema


// COMMAND ----------

// join the 2 dataframes together to form a unified view of return data of cm and s&p tsx returns
val ry_snp500_stock_returns = cleaned_ry.join(cleaned_snp500, Seq("Date"))
display(ry_snp500_stock_returns)
ry_snp500_stock_returns.count
ry_snp500_stock_returns.printSchema


// COMMAND ----------

// join the 2 dataframes together to form a unified view of return data of cm and s&p tsx returns
val td_snp500_stock_returns = cleaned_td.join(cleaned_snp500, Seq("Date"))
display(td_snp500_stock_returns)
td_snp500_stock_returns.count
td_snp500_stock_returns.printSchema


// COMMAND ----------

cm_tsx_stock_returns.count
ry_tsx_stock_returns.count
td_tsx_stock_returns.count


cm_snp500_stock_returns.count
ry_snp500_stock_returns.count
td_snp500_stock_returns.count

// COMMAND ----------

//clean up the joined records when there is no data for the date on either stock

val cleaned_cm_tsx_stock_returns = cm_tsx_stock_returns.filter("CM_Percent_Return is not null and SPTSX_Percent_Return is not null")

val cleaned_ry_tsx_stock_returns = ry_tsx_stock_returns.filter("RY_Percent_Return is not null and SPTSX_Percent_Return is not null")

val cleaned_td_tsx_stock_returns = td_tsx_stock_returns.filter("TD_Percent_Return is not null and SPTSX_Percent_Return is not null")

val cleaned_cm_snp500_stock_returns = cm_snp500_stock_returns.filter("CM_Percent_Return is not null and SNP500_Percent_Return is not null")

val cleaned_ry_snp500_stock_returns = ry_snp500_stock_returns.filter("RY_Percent_Return is not null and SNP500_Percent_Return is not null")

val cleaned_td_snp500_stock_returns = td_snp500_stock_returns.filter("TD_Percent_Return is not null and SNP500_Percent_Return is not null")


//val cleaned_tech_stock_returns = tech_stock_returns.filter("GOOG_Percent_Return is not null and SPTSX_Percent_Return is not null")


// COMMAND ----------

cleaned_cm_tsx_stock_returns.count
cleaned_ry_tsx_stock_returns.count
cleaned_td_tsx_stock_returns.count


cleaned_cm_snp500_stock_returns.count
cleaned_ry_snp500_stock_returns.count
cleaned_td_snp500_stock_returns.count

//cleaned_tech_stock_returns.count

// COMMAND ----------

// we will be regression the index return with the stock return
val cm_tsx_featureCols = Array("SPTSX_Percent_Return")

val ry_tsx_featureCols = Array("SPTSX_Percent_Return")

val td_tsx_featureCols = Array("SPTSX_Percent_Return")

val cm_snp500_featureCols = Array("SNP500_Percent_Return")

val ry_snp500_featureCols = Array("SNP500_Percent_Return")

val td_snp500_featureCols = Array("SNP500_Percent_Return")




// COMMAND ----------

// VectorAssembler Assembles all of these columns into one single vector. To do this, set the input columns and output column. Then that assembler will be used to transform the prepped data to the final dataset.
// prepare the vector input for the features
import org.apache.spark.ml.feature.VectorAssembler

val cm_tsx_assembler = new VectorAssembler()
  .setInputCols(cm_tsx_featureCols)
  .setOutputCol("features")

val cm_tsx_finalPrep = cm_tsx_assembler.transform(cleaned_cm_tsx_stock_returns)

val ry_tsx_assembler = new VectorAssembler()
  .setInputCols(ry_tsx_featureCols)
  .setOutputCol("features")

val ry_tsx_finalPrep = ry_tsx_assembler.transform(cleaned_ry_tsx_stock_returns)


val td_tsx_assembler = new VectorAssembler()
  .setInputCols(td_tsx_featureCols)
  .setOutputCol("features")

val td_tsx_finalPrep = td_tsx_assembler.transform(cleaned_td_tsx_stock_returns)


val cm_snp500_assembler = new VectorAssembler()
  .setInputCols(cm_snp500_featureCols)
  .setOutputCol("features")

val cm_snp500_finalPrep = cm_snp500_assembler.transform(cleaned_cm_snp500_stock_returns)


val ry_snp500_assembler = new VectorAssembler()
  .setInputCols(ry_snp500_featureCols)
  .setOutputCol("features")

val ry_snp500_finalPrep = ry_snp500_assembler.transform(cleaned_ry_snp500_stock_returns)

val td_snp500_assembler = new VectorAssembler()
  .setInputCols(td_snp500_featureCols)
  .setOutputCol("features")

val td_snp500_finalPrep = td_snp500_assembler.transform(cleaned_td_snp500_stock_returns)



//val tech_finalPrep = assembler.transform(cleaned_tech_stock_returns)


// COMMAND ----------

display(cm_tsx_finalPrep)

display(ry_tsx_finalPrep)

display(td_tsx_finalPrep)

display(cm_snp500_finalPrep)

display(ry_snp500_finalPrep)

display(td_snp500_finalPrep)




// COMMAND ----------

// randomly split 30% and 70% on the data set for training and testing of the linear regression model on return
val Array(cm_tsx_training, cm_tsx_test) = cm_tsx_finalPrep.randomSplit(Array(0.7, 0.3))

// Going to cache the data to make sure things stay snappy!
cm_tsx_training.cache()
cm_tsx_test.cache()

println(cm_tsx_training.count())  
println(cm_tsx_test.count())


// randomly split 30% and 70% on the data set for training and testing of the linear regression model on return
val Array(ry_tsx_training, ry_tsx_test) = ry_tsx_finalPrep.randomSplit(Array(0.7, 0.3))

// Going to cache the data to make sure things stay snappy!
ry_tsx_training.cache()
ry_tsx_test.cache()

println(ry_tsx_training.count())  
println(ry_tsx_test.count())



// randomly split 30% and 70% on the data set for training and testing of the linear regression model on return
val Array(td_tsx_training, td_tsx_test) = td_tsx_finalPrep.randomSplit(Array(0.7, 0.3))

// Going to cache the data to make sure things stay snappy!
td_tsx_training.cache()
td_tsx_test.cache()

println(td_tsx_training.count())  
println(td_tsx_test.count())


val Array(cm_snp500_training, cm_snp500_test) = cm_snp500_finalPrep.randomSplit(Array(0.7, 0.3))

// Going to cache the data to make sure things stay snappy!
cm_snp500_training.cache()
cm_snp500_test.cache()

println(cm_snp500_training.count())  
println(cm_snp500_test.count())

val Array(ry_snp500_training, ry_snp500_test) = ry_snp500_finalPrep.randomSplit(Array(0.7, 0.3))

// Going to cache the data to make sure things stay snappy!
ry_snp500_training.cache()
ry_snp500_test.cache()

println(ry_snp500_training.count())  
println(ry_snp500_test.count())

val Array(td_snp500_training, td_snp500_test) = td_snp500_finalPrep.randomSplit(Array(0.7, 0.3))

// Going to cache the data to make sure things stay snappy!
td_snp500_training.cache()
td_snp500_test.cache()

println(td_snp500_training.count())  
println(td_snp500_test.count())






// COMMAND ----------

//val Array(tech_training, tech_test) = tech_finalPrep.randomSplit(Array(0.7, 0.3))

// Going to cache the data to make sure things stay snappy!
//tech_training.cache()
//tech_test.cache()

//println(tech_training.count())  
//println(tech_test.count())

// COMMAND ----------

// MAGIC %md ### Build a linear regression model to see if there is a relationship between the return of the stocks and the indexes

// COMMAND ----------

// print out the model parameters for linear regression
import org.apache.spark.ml.regression.LinearRegression

val cm_tsx_lrModel = new LinearRegression()
  .setLabelCol("CM_Percent_Return")
  .setFeaturesCol("features")
  .setElasticNetParam(0.5)

println("Printing out the model Parameters:")
println("-"*20)
println(cm_tsx_lrModel.explainParams)
println("-"*20)



val ry_tsx_lrModel = new LinearRegression()
  .setLabelCol("RY_Percent_Return")
  .setFeaturesCol("features")
  .setElasticNetParam(0.5)

println("Printing out the model Parameters:")
println("-"*20)
println(ry_tsx_lrModel.explainParams)
println("-"*20)

val td_tsx_lrModel = new LinearRegression()
  .setLabelCol("TD_Percent_Return")
  .setFeaturesCol("features")
  .setElasticNetParam(0.5)

println("Printing out the model Parameters:")
println("-"*20)
println(td_tsx_lrModel.explainParams)
println("-"*20)



val cm_snp500_lrModel = new LinearRegression()
  .setLabelCol("CM_Percent_Return")
  .setFeaturesCol("features")
  .setElasticNetParam(0.5)

println("Printing out the model Parameters:")
println("-"*20)
println(cm_snp500_lrModel.explainParams)
println("-"*20)



val ry_snp500_lrModel = new LinearRegression()
  .setLabelCol("RY_Percent_Return")
  .setFeaturesCol("features")
  .setElasticNetParam(0.5)

println("Printing out the model Parameters:")
println("-"*20)
println(ry_snp500_lrModel.explainParams)
println("-"*20)

val td_snp500_lrModel = new LinearRegression()
  .setLabelCol("TD_Percent_Return")
  .setFeaturesCol("features")
  .setElasticNetParam(0.5)

println("Printing out the model Parameters:")
println("-"*20)
println(td_snp500_lrModel.explainParams)
println("-"*20)

// COMMAND ----------

/*val tech_lrModel = new LinearRegression()
  .setLabelCol("GOOG_Percent_Return")
  .setFeaturesCol("features")
  .setElasticNetParam(0.5)

println("Printing out the model Parameters:")
println("-"*20)
println(tech_lrModel.explainParams)
println("-"*20)*/


// COMMAND ----------

import org.apache.spark.mllib.evaluation.RegressionMetrics
val cm_tsx_lrFitted = cm_tsx_lrModel.fit(cm_tsx_training)


val ry_tsx_lrFitted = ry_tsx_lrModel.fit(ry_tsx_training)


val td_tsx_lrFitted = td_tsx_lrModel.fit(td_tsx_training)


val cm_snp500_lrFitted = cm_snp500_lrModel.fit(cm_snp500_training)


val ry_snp500_lrFitted = ry_snp500_lrModel.fit(ry_snp500_training)


val td_snp500_lrFitted = td_snp500_lrModel.fit(td_snp500_training)


// COMMAND ----------

//import org.apache.spark.mllib.evaluation.RegressionMetrics
//val tech_lrFitted = tech_lrModel.fit(tech_training)

// COMMAND ----------

// try the data set for testing
val cm_tsx_holdout = cm_tsx_lrFitted
  .transform(cm_tsx_test)
  .selectExpr("prediction as raw_prediction", 
    "CM_Percent_Return",
    "SPTSX_Percent_Return",          
    "double(prediction - CM_Percent_Return) as diff_real_vs_prediction_return") 
display(cm_tsx_holdout)













// COMMAND ----------

val ry_tsx_holdout = ry_tsx_lrFitted
  .transform(ry_tsx_test)
  .selectExpr("prediction as raw_prediction", 
    "RY_Percent_Return",
    "SPTSX_Percent_Return",          
    "double(prediction - RY_Percent_Return) as diff_real_vs_prediction_return") 
display(ry_tsx_holdout)


// COMMAND ----------

val td_tsx_holdout = td_tsx_lrFitted
  .transform(td_tsx_test)
  .selectExpr("prediction as raw_prediction", 
    "TD_Percent_Return",
    "SPTSX_Percent_Return",          
    "double(prediction - TD_Percent_Return) as diff_real_vs_prediction_return") 
display(td_tsx_holdout)


// COMMAND ----------

val cm_snp500_holdout = cm_snp500_lrFitted
  .transform(cm_snp500_test)
  .selectExpr("prediction as raw_prediction", 
    "CM_Percent_Return",
    "SNP500_Percent_Return",          
    "double(prediction - CM_Percent_Return) as diff_real_vs_prediction_return") 
display(cm_snp500_holdout)


// COMMAND ----------

val ry_snp500_holdout = ry_snp500_lrFitted
  .transform(ry_snp500_test)
  .selectExpr("prediction as raw_prediction", 
    "RY_Percent_Return",
    "SNP500_Percent_Return",          
    "double(prediction - RY_Percent_Return) as diff_real_vs_prediction_return") 
display(ry_snp500_holdout)


// COMMAND ----------

val td_snp500_holdout = td_snp500_lrFitted
  .transform(td_snp500_test)
  .selectExpr("prediction as raw_prediction", 
    "TD_Percent_Return",
    "SNP500_Percent_Return",          
    "double(prediction - TD_Percent_Return) as diff_real_vs_prediction_return") 
display(td_snp500_holdout)

// COMMAND ----------

//val tech_holdout = tech_lrFitted
//  .transform(tech_test)
//  .selectExpr("prediction as raw_prediction", 
//    "GOOG_Percent_Return",
 //   "SPTSX_Percent_Return",          
  //  "double(prediction - GOOG_Percent_Return) as diff_real_vs_prediction_return") 
//display(tech_holdout)

// COMMAND ----------

// have to do a type conversion for RegressionMetrics
//print out the statistics of the linear regression
val cm_tsx_rm = new RegressionMetrics(
  cm_tsx_holdout.select("raw_prediction", "CM_Percent_Return").rdd.map(x =>
  (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

println("CM vs TSX MSE: " + cm_tsx_rm.meanSquaredError)
println("CM vs TSX MAE: " + cm_tsx_rm.meanAbsoluteError)
println("CM vs TSX RMSE Squared: " + cm_tsx_rm.rootMeanSquaredError)
println("CM vs TSX R Squared: " + cm_tsx_rm.r2)
println("CM vs TSX Explained Variance: " + cm_tsx_rm.explainedVariance + "\n")

// COMMAND ----------

// have to do a type conversion for RegressionMetrics
//print out the statistics of the linear regression
val cm_snp500_rm = new RegressionMetrics(
  cm_snp500_holdout.select("raw_prediction", "CM_Percent_Return").rdd.map(x =>
  (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

println("CM vs snp500 MSE: " + cm_snp500_rm.meanSquaredError)
println("CM vs snp500 MAE: " + cm_snp500_rm.meanAbsoluteError)
println("CM vs snp500 RMSE Squared: " + cm_snp500_rm.rootMeanSquaredError)
println("CM vs snp500 R Squared: " + cm_snp500_rm.r2)
println("CM vs snp500 Explained Variance: " + cm_snp500_rm.explainedVariance + "\n")

// COMMAND ----------

// have to do a type conversion for RegressionMetrics
//print out the statistics of the linear regression
val ry_tsx_rm = new RegressionMetrics(
  ry_tsx_holdout.select("raw_prediction", "RY_Percent_Return").rdd.map(x =>
  (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

println("RY vs TSX MSE: " + ry_tsx_rm.meanSquaredError)
println("RY vs TSX MAE: " + ry_tsx_rm.meanAbsoluteError)
println("RY vs TSX RMSE Squared: " + ry_tsx_rm.rootMeanSquaredError)
println("RY vs TSX R Squared: " + ry_tsx_rm.r2)
println("RY vs TSX Explained Variance: " + ry_tsx_rm.explainedVariance + "\n")

// COMMAND ----------

// have to do a type conversion for RegressionMetrics
//print out the statistics of the linear regression
val ry_snp500_rm = new RegressionMetrics(
  ry_snp500_holdout.select("raw_prediction", "RY_Percent_Return").rdd.map(x =>
  (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

println("RY vs snp500 MSE: " + ry_snp500_rm.meanSquaredError)
println("RY vs snp500 MAE: " + ry_snp500_rm.meanAbsoluteError)
println("RY vs snp500 RMSE Squared: " + ry_snp500_rm.rootMeanSquaredError)
println("RY vs snp500 R Squared: " + ry_snp500_rm.r2)
println("RY vs snp500 Explained Variance: " + ry_snp500_rm.explainedVariance + "\n")

// COMMAND ----------

// have to do a type conversion for RegressionMetrics
//print out the statistics of the linear regression
val td_tsx_rm = new RegressionMetrics(
  td_tsx_holdout.select("raw_prediction", "TD_Percent_Return").rdd.map(x =>
  (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

println("TD vs TSX MSE: " + td_tsx_rm.meanSquaredError)
println("TD vs TSX MAE: " + td_tsx_rm.meanAbsoluteError)
println("TD vs TSX RMSE Squared: " + td_tsx_rm.rootMeanSquaredError)
println("TD vs TSX R Squared: " + td_tsx_rm.r2)
println("TD vs TSX Explained Variance: " + td_tsx_rm.explainedVariance + "\n")

// COMMAND ----------

// have to do a type conversion for RegressionMetrics
//print out the statistics of the linear regression
val td_snp500_rm = new RegressionMetrics(
  td_snp500_holdout.select("raw_prediction", "TD_Percent_Return").rdd.map(x =>
  (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

println("TD vs snp500 MSE: " + td_snp500_rm.meanSquaredError)
println("TD vs snp500 MAE: " + td_snp500_rm.meanAbsoluteError)
println("TD vs snp500 RMSE Squared: " + td_snp500_rm.rootMeanSquaredError)
println("TD vs snp500 R Squared: " + td_snp500_rm.r2)
println("TD vs snp500 Explained Variance: " + td_snp500_rm.explainedVariance + "\n")

// COMMAND ----------

// have to do a type conversion for RegressionMetrics
//print out the statistics of the linear regression
//val tech_rm = new RegressionMetrics(
//  tech_holdout.select("raw_prediction", "GOOG_Percent_Return").rdd.map(x =>
//  (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

//println("MSE: " + tech_rm.meanSquaredError)
//println("MAE: " + tech_rm.meanAbsoluteError)
//println("RMSE Squared: " + tech_rm.rootMeanSquaredError)
//println("R Squared: " + tech_rm.r2)
//println("Explained Variance: " + tech_rm.explainedVariance + "\n")

// COMMAND ----------

// MAGIC %md ## End of Final Term Project Assignment 

// COMMAND ----------



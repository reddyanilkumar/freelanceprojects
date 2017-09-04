package analyzers

/**
  * Created by spyder on 28/10/16.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, LinearRegression}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._

object SalesRankAnalyzer {

  import org.apache.log4j._

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder().master("local[*]").config("spark.ui.port", "4070")
      .appName("Shoten: Best Selling Rank Reader")
      .getOrCreate()

    val sc = spark.sparkContext

    val data = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("mybsr.csv")


    val df = data.filter(data("Category") === "Electronics").withColumn("bsrtwice", lit(data("BSR") * 2)).withColumn("bsrthrice", lit(data("BSR") * 3))
      .withColumn("logbsr", lit(log(data("BSR"))))
      //.withColumn("salespermonth", lit(data("SalesPerDay") * data("SalesPerDay") * data("SalesPerDay")))

   val selectDF=   df.select(lit(log(df("SalesPerDay"))).as("label"), df("BSR"), df("bsrtwice"),df("logbsr"), df("bsrthrice"))

    selectDF.show(false)
    val assembler = new VectorAssembler().setInputCols(Array("BSR")).setOutputCol("features")

    val output = assembler.transform(selectDF) //.select("label","features")

    output.show(false)
    val lr = new GeneralizedLinearRegression()
      .setFamily("poisson")
      //.setLink("identity")
      .setMaxIter(100)
      .setRegParam(0.001)
    val lrModel = lr.fit(output)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics!
    // Explore this in the spark-shell for more methods to call
    val trainingSummary = lrModel.summary

//    println(s"numIterations: ${trainingSummary.totalIterations}")
//    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
//
//    trainingSummary.residuals.show()
//
//    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
//    println(s"MSE: ${trainingSummary.meanSquaredError}")
//    println(s"r2: ${trainingSummary.r2}")

    val results = lrModel.transform(output)

    results.show(false)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(results)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
  }


}

package analyzers

/**
  * Created by spyder on 28/10/16.
  */

import java.sql.Timestamp

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.dmg.pmml.True

object AmazonAnalyzer {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder().master("local[*]").config("spark.ui.port", "4070")
      .appName("Shoten: Best Selling Rank Reader")
      .getOrCreate()

    val schema = StructType(StructField("ProductName", StringType, false) :: StructField("Category", StringType, false) :: StructField("BSR", IntegerType, false) :: StructField("Sales", IntegerType, false) :: Nil)

    val rawData = spark.read.schema(schema).option("header", true).csv("bsr.csv")

    rawData.show(false)
    //take only ASIN, bullet_points, description, title
    import spark.implicits._

    println("Correlation between BSR and Sales " + rawData.stat.corr("BSR", "Sales"))

    val mlDf = rawData.map(row => {
      LabeledPoint(row.getInt(3), Vectors.dense(row.getInt(2)))
    })

   val newMlDF = spark.createDataFrame(mlDf.rdd,classOf[LabeledPoint])
    val linearRegression = new LinearRegression()
    linearRegression.setMaxIter(100)

    val linearModel = linearRegression.fit(newMlDF)

    println("Linear Model Coefficients: " + linearModel.coefficients)
    println("Linear Model Intercepts: " + linearModel.intercept)
    //load new data for predictions
    val predictions = linearModel.transform(newMlDF)
    predictions.show(false)






  }


}

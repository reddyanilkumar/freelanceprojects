package analyzers

/**
  * Created by spyder on 19/01/17.
  */


// scalastyle:off println

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd
import org.apache.spark.sql.functions.{concat, explode, lit}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.immutable.ListMap
import scala.util.matching.Regex

/**
  * amazon products description , main keywords with TF-IDF algorithm
  *
  */


object AmazonProductsCosine {

  val schema: StructType = new StructType()
    .add("asin", StringType)
    .add("features", new VectorUDT())

  val REMOVE_TAGS = new Regex("<.+?>")

  var dburl = "10.0.0.4"
  dburl = "localhost"


  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    run()
  }

  private def run(): Unit = {

    val spark = SparkSession
      .builder().master("local[*]").config("spark.ui.port", "4070")
      .appName("TF-IDF Product Analyzer")
      .getOrCreate()
    import spark.implicits._
    var result: DataFrame = null
    result = spark.read.parquet("/data/bigdata/output/cproducts")
    var vocabModel: CountVectorizerModel = null
    vocabModel = CountVectorizerModel.load("/data/bigdata/output/cvocab")
    if (null == result) {


      val amazonProducts = "amazon_product"

      var jdbcDF = spark.read
        .format("jdbc")
        .option("url", "jdbc:mysql://" + dburl + ":3306/table?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=false")
        .option("dbtable", amazonProducts)
        .load().na.drop(Array("bullet_points", "description", "title")).sample(true, 0.1)


      //we are merging title, bullet points and description and creating one column called docs
      val rawDataFrame = jdbcDF.withColumn("titleBullet", concat($"title", lit(" "), $"bullet_points")).withColumn("docs", concat($"titleBullet", lit(" "), $"description")).select("asin", "docs")
      //.sample(true,0.01)

      val df = rawDataFrame.map(row => {
        val asin = row.getString(0)
        val desc = row.getString(1)
        //remove all HTML tags
        val newdesc = REMOVE_TAGS.replaceAllIn(desc, "")
        (asin, newdesc)
      }).toDF("asin", "docs")

      df.cache()
      val tokenizer = new RegexTokenizer().setPattern("\\W").setMinTokenLength(3)
        .setInputCol("docs")
        .setOutputCol("rawTokens")
      val stopWordsRemover = new StopWordsRemover().setCaseSensitive(true)
        .setInputCol("rawTokens")
        .setOutputCol("tokens")
      val countVectorizer = new CountVectorizer()
        .setInputCol("tokens")
        .setOutputCol("rawFeatures")

      val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

      val pipeline = new Pipeline()
        .setStages(Array(tokenizer, stopWordsRemover, countVectorizer, idf))
      println("Running pipeline fit for TF-IDF keywords analyzer")
      val model = pipeline.fit(df)
      println("Running pipeline model transform for TF-IDF")
      result = model.transform(df)
      result.cache()
      // Run LDA.
      result.write.mode(SaveMode.Overwrite).parquet("/data/bigdata/output/cproducts")
      vocabModel = model.stages(2).asInstanceOf[CountVectorizerModel]
      vocabModel.write.overwrite().save("/data/bigdata/output/cvocab")
    }

    val vocab = vocabModel.vocabulary

    println("Total products=" + result.count())
    println("Collecting keywords ..")
    //    result.show(false)
    result = result.select("asin", "features")
   val xresult = result.rdd.map(row => {
          (row.getString(0), row.getAs[VectorUDT](1))
        })



//    val df = spark.createDataFrame(xresult, schema)
//
//    println("created dataframe with vectors")
//    val mlibdf = MLUtils.convertVectorColumnsFromML(df, "features")
//    println("created mllib vectors dataframe "+ mlibdf.count())
//    // mlibdf.show(false)
//    mlibdf.printSchema()

//    mlibdf.show(false)
//    val rowdf = mlibdf.rdd.map(row => {
//      println("got row " + row.getString(0))
//      // row.getAs[org.apache.spark.mllib.linalg.SparseVector](1)
//      row.getString(0)
//    }).count()
    //
    //    val rowMatrix = new RowMatrix(rowdf)
    //
    //    val rows = rowMatrix.numRows()
    //    val cols = rowMatrix.numCols()
    //    println(s" number of rows and cols is $rows x $cols")


  }


}

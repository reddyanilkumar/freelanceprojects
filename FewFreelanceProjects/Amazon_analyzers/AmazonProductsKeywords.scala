package analyzers

/**
  * Created by spyder on 19/01/17.
  */


// scalastyle:off println

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions.{concat, explode, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.immutable.ListMap
import scala.util.matching.Regex

/**
  * amazon products description , main keywords with TF-IDF algorithm
  *
  */


object AmazonProductsKeywords {

  val REMOVE_TAGS = new Regex("<.+?>")

  var dburl = "10.0.0.4"
 // dburl = "localhost"


  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    run()
  }

  private def run(): Unit = {

    val spark = SparkSession
      .builder().master("local[*]").config("spark.ui.port", "4070")
      .appName("TF-IDF Product Analyzer")
      .getOrCreate()

    var result: DataFrame = null //spark.read.parquet("/data/haggell/bigdata/output/products")
    var vocabModel: CountVectorizerModel = null
    //vocabModel = CountVectorizerModel.load("/data/haggell/bigdata/output/vocab")
    if (null == result) {

      import spark.implicits._
      val amazonProducts = "amazon_product"

      //     val jdbcDF = spark.read.option("header", true).option("inferSchema", "true").csv("./src/main/resources/products.txt")

      var jdbcDF = spark.read
        .format("jdbc")
        .option("url", "jdbc:mysql://" + dburl + ":3306/table?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=false")
        .option("dbtable", amazonProducts)
        .load().na.drop(Array("bullet_points", "description", "title"))


      //we are merging title, bullet points and description and creating one column called docs
      val rawDataFrame = jdbcDF.withColumn("titleBullet", concat($"title", lit(" "), $"bullet_points")).withColumn("docs", concat($"titleBullet", lit(" "), $"description")).select("asin", "docs")
      //.sample(true,0.01)

      val df = rawDataFrame.na.drop().map(row => {
        val asin = row.getString(0)
        val desc = row.getString(1)
        //remove all HTML tags
        val newdesc = REMOVE_TAGS.replaceAllIn(desc, " ")
        (asin, newdesc)
      }).toDF("asin", "docs")

      df.cache()
      val tokenizer = new RegexTokenizer().setPattern("\\W").setMinTokenLength(3)
        .setInputCol("docs")
        .setOutputCol("rawTokens")
      val stopWordsRemover = new StopWordsRemover().setCaseSensitive(true)
        .setInputCol("rawTokens")
        .setOutputCol("tokens")
      //  stopWordsRemover.setStopWords(stopWordsRemover.getStopWords)
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
    //  result.write.mode(SaveMode.Overwrite).parquet("/data/haggell/bigdata/output/products")
      vocabModel = model.stages(2).asInstanceOf[CountVectorizerModel]
   //   vocabModel.write.overwrite().save("/data/haggell/bigdata/output/vocab")
    }

    val vocab = vocabModel.vocabulary

    println("Total products=" + result.count())
    println("Collecting keywords ..")
    //    result.show(false)
    //   result.select("asin", "features").show(false)
    val xresult = result.select("features", "asin", "docs").rdd.map {
      row => {
        val asin = row.getString(1)
        val spv = row.getAs[SparseVector](0)
        val vindices = spv.indices
        val values = spv.values
        //   println("\n\n for asin " + asin + " with docs " + row.getString(2))
        val cache = collection.mutable.Map[String, Double]()
        for (curIndex <- vindices.indices) {
          val myval = vindices(curIndex)
          val word = vocab(myval)
          val strength = values(curIndex)
          //  println(s" strength of $word is $strength")
          cache += (word -> strength)
        }
        val fkeywords = ListMap(cache.toSeq.sortWith(_._2 > _._2): _*)
        //    println(s"keywords is $fkeywords")
        (asin, fkeywords.toList)
      }
    }
    println("Done collecting with result rows: " + xresult.count())

    import spark.implicits._
    var topics = xresult.toDF("asin", "keywords")
    topics.cache()
    //
    val finalkeys = topics.withColumn("terms", explode($"keywords")).drop($"keywords").map {
      row => {

        var asin = row.getString(0)
        var termStruct = row.getStruct(1)

        (asin, termStruct.getString(0), termStruct.getDouble(1))
      }

    }.toDF("asin", "term", "weight")
    finalkeys.cache()
    finalkeys.show(false)
    println("Total asin+keywords=" + finalkeys.count())
    finalkeys.write.mode(SaveMode.Overwrite).parquet("/data/bigdata/output/prodkeywords")




  }


}

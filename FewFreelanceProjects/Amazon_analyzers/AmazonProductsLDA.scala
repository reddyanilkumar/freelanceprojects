package analyzers

/**
  * Created by spyder on 19/01/17.
  */

// scalastyle:off println

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.util.matching.Regex


/**
  * amazon products clustered with Latent Dirichlet Allocation (LDA) app. Run on command line
  *
  */

//case class TopicData(id: Int, text: String, weight: Double)

object AmazonProductsLDA {

  val REMOVE_TAGS = new Regex("<.+?>")

  var dburl = "10.0.0.4"
//  dburl = "localhost"
  val keywords = 100
  val totalTopics = 300
  val maxIteration = 100

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    run()
  }

  private def run(): Unit = {
    println("Running LDA Analyzer on database")
    val spark = SparkSession
      .builder().master("local[*]").config("spark.ui.port", "4070").config("spark.driver.memory","3g").config("spark.executor.memory","6g")
        .config("spark.executor.instances", "4").config("spark.executor.cores", "5")
      .appName("AiHello: LDA Product Analyzer")
      .getOrCreate()

    import org.apache.spark.sql.functions.explode
    import spark.implicits._
    val amazonProducts = "amazon_product"

    //     val jdbcDF = spark.read.option("header", true).option("inferSchema", "true").csv("./src/main/resources/products.txt")
    //
    var jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://" + dburl + ":3306/table?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=false")
      .option("dbtable", amazonProducts)
      .option("user", "root")
      .option("password", "")
      .load().na.drop(Array("bullet_points", "description", "title"))

    jdbcDF.cache()
    println("We got total RAW data " + jdbcDF.count())
    //we are merging title, bullet points and description and creating one column called docs
    val rawDataFrame = jdbcDF.withColumn("titleBullet", concat($"title", lit(" "), $"bullet_points")).withColumn("docs", concat($"titleBullet", lit(" "), $"description")).select("asin", "docs")
    //.sample(true,0.01)
    rawDataFrame.cache()

    val df = rawDataFrame.map(row => {
      val asin = row.getString(0)
      val desc = row.getString(1)
      //remove all HTML tags
      val newdesc = REMOVE_TAGS.replaceAllIn(desc, "")
      (asin, newdesc)
    }).toDF("asin", "docs")
    println("Completed RAW dataframe TEXT concat ...")
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
      .setOutputCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stopWordsRemover, countVectorizer))
    println("Running pipeline fit for LDA")
    val model = pipeline.fit(df)
    println("Running pipeline model transform")
    val result = model.transform(df)
    result.cache()
    result.show()
    // Run LDA.
    val lda = new LDA().setK(totalTopics).setMaxIter(maxIteration).setOptimizer("online")
    //or new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)
    println("Fitting LDA ..")
    val ldaModel = lda.fit(result)
    println("Completed LDA FIT")
//    val ll = ldaModel.logLikelihood(result)
//    val lp = ldaModel.logPerplexity(result)

//    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
//    println(s"The upper bound on perplexity: $lp")
    println(s"Finished training LDA model.  Describing topics")
    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = keywords)

    val vocab = model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary
    println("Vocabulary SIZE is " + vocab.length)
    val xtopics = topicIndices.map(row => {
      val topic = row.getInt(0)
      val row2: mutable.WrappedArray[Integer] = row.getAs[mutable.WrappedArray[Integer]](1).array
      val row3m: mutable.WrappedArray[java.lang.Double] = row.getAs[mutable.WrappedArray[java.lang.Double]](2)
      val terms = row2.zip(row3m).map {
        case (term, weight) => {
          (vocab(term), weight)
        }
      }

      (topic, terms)

    })

    val topics = xtopics.withColumn("terms", explode($"_2")).drop("_2").map {
      row => {
        var topicNumber = row.getInt(0)
        var termStruct = row.getStruct(1)

        (topicNumber, termStruct.getString(0), termStruct.getDouble(1) * 100)
      }
    }.toDF("topicNumber", "term", "weight")


    topics.write.mode(SaveMode.Overwrite).parquet("/data/bigdata/output/xtopics")
    println("Done with topics")


    val transformed = ldaModel.transform(result)
    val finalDataSet = transformed.select("asin", "topicDistribution")


    val compdf = finalDataSet.map(row => {
      val asin = row.getString(0)
      val td = row.getAs[DenseVector](1).toArray

      val ret = td.zipWithIndex.map {
        case (tw, index) =>
          val realtw = tw * 100
          (index, realtw)
      }

      (asin, ret)
    })

    val finaldf = compdf.withColumn("termMapping", explode($"_2")).drop("_2")


    val reallyfinaldf = finaldf.map {
      row => {
        val asin = row.getString(0)
        val struct = row.getStruct(1)
        (asin, struct.getInt(0), struct.getDouble(1))
      }
    }.filter("_3 >1").toDF("asin", "topicindex", "weight")
    reallyfinaldf.show(false)
    reallyfinaldf.printSchema()
    reallyfinaldf.write.mode(SaveMode.Overwrite).parquet("/data/bigdata/output/xproductClassification")
    //TODO save this back to database


    println("Completed LDA Computation")


  }


}

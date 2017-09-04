package analyzers

/**
  * Created by spyder on 28/10/16.
  */

import java.sql.Timestamp

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object AmazonSalesAnalyzer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder().master("local[*]").config("spark.ui.port", "4070")
      .appName("Shoten: Database Reader")
      .getOrCreate()


    def products: DataFrame = spark.read.parquet("/data/bigdata/products")

    //take only ASIN, bullet_points, description, title
    import spark.implicits._
    /*
    struct<id:bigint,amenity_id:bigint,asin:string,brand:string,bullet_points:string,currency_code:string,
description:string,image_url:string,last_updated:timestamp,market_place:string,price:double,quantity:int,sku:string,
title:string,user_id:bigint>
     */
    val filterddata = products.as[(Long, Long, String, String, String, String, String, String, Timestamp, String, Double, Integer, String, String, Long)].map {
      row => {

        //ASIN, bullet points, description, title
        (row._3, row._5, row._7, row._14)
      }
    }.map {
      row => {
        //row._2 +" " + row._3 + " " + row._4

        val words = row._4.replaceAll("\\[", " ").replaceAll("\\]", "").replaceAll("\\(", " ").replaceAll("\\)", " "). replaceAll("[,./-]", " ").replaceAll("\\s{2,}", " ").trim()

        (row._1, words)
      }
    }.toDF("asin", "title")

    // We now have data as (ASIN, TEXT)
    filterddata.cache()
    filterddata.show(false)
    println("We have total rows= " + filterddata.count())
    //store as (ASIN, textvalue)

    // calculate TF-IDF?

    val tokenizer = new Tokenizer().setInputCol("title").setOutputCol("words")

    val remover = new StopWordsRemover().setInputCol(tokenizer.getOutputCol).setOutputCol("filtered")
    val hashingTF = new HashingTF().setInputCol(remover.getOutputCol).setOutputCol("rawFeatures")
    val idf = new IDF().setInputCol(hashingTF.getOutputCol).setOutputCol("features")


    val pipeline = new Pipeline("convertorPipeLine")
    pipeline.setStages(Array(tokenizer, remover, hashingTF, idf))

    val model = pipeline.fit(filterddata)
    val rescaledData = model.transform(filterddata)
    rescaledData.select("asin", "filtered", "rawfeatures").show(false)

   // rescaledData.show(false)
    // We now try to find all products that are similar to each other.

    //products.show(false)
    // TODO we have to move all these calculations into a pipeline module
    val rows: RDD[Vector] = rescaledData.rdd.map { row => {
      val v: SparseVector = row.getAs[SparseVector]("features")
      Vectors.fromML(v.compressed)
    }
    }
    rows.cache()
    println("Total rows is " + rows.count())
    val k = 100
    val threshold = 0.1
    val mat = new RowMatrix(rows)

    println("For row matrix rows=" + mat.numRows() + " cols is " + mat.numCols()  )


    mat.rows.map(vector => {
      println("Row matrix vector is " + vector)
    }).count()



    //    val svd = mat.computeSVD(k, computeU = true)
    //
    //    val topConceptTerms = LatentSemanticAnalyzer.topTermsInTopConcepts(svd, 10, 10, null)
    //    topConceptTerms.foreach(row => {
    //      println(s"row is $row")
    //
    //    })

    // val colSim = mat.columnSimilarities(threshold)

    // println (" sim colums = " + colSim)
    //  val approxEntries = colSim.entries.map { case MatrixEntry(i, j, v) => {((i, j), v)} }

    // println(" approx entries " + approxEntries)
  }


}

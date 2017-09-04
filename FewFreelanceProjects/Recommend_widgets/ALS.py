from pyspark.mllib.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import bround
import sys

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print >> sys.stderr, \
            "Usage: ALS.py <input-file> <output-file>"
        exit(-1)

    spark = SparkSession.builder \
	    .master("local") \
	    .appName("CF App") \
            .getOrCreate()   
    data = spark.read \
                     .option("header","true") \
                     .csv(sys.argv[1]) \
                     .withColumnRenamed("BOM No.","BOM") \
                     .withColumnRenamed("Part No.","Part").distinct()                                                           

    #Encoding strings
    indexer = StringIndexer(inputCol="BOM", outputCol="BOM Cat") 
    indexer1 = StringIndexer(inputCol="Part", outputCol="Part Cat")
    
    #Remove rows with empty parts
    data = data.na.drop(subset="Part")
  
    #Remove part with count less than 10
    result = data.groupBy('Part').count().filter('count>10')
    data = data.join(result,data.Part==result.Part,'leftsemi')

    data = indexer.fit(data).transform(data)
    data = indexer1.fit(data).transform(data)

    #Consider 1 as implicit feedback for all rows.
    data = data.withColumn("implicitValue", lit(1))
	
    #Training ALS
    model = ALS.trainImplicit(data['BOM Cat','Part Cat','implicitValue'], 10, 10, 0.01, -1, 0.1)
    predictions = model.predictAll(data['BOM Cat','Part Cat'].rdd)   

    #Output processing    
    predictions = predictions.toDF() \
			     .withColumnRenamed("user","BOM Cat") \
                             .withColumnRenamed("product","Part Cat") 
    predictions = predictions.withColumn('BOM Cat',predictions['BOM Cat'].cast("double")) \
                             .withColumn('Part Cat',predictions['Part Cat'].cast("double")) \
                             .withColumn('rating',bround(predictions["rating"], 3))
    predictions = data.join(predictions, [predictions["BOM Cat"] == data["BOM Cat"],predictions["Part Cat"] == data["Part Cat"]], "outer")
	
    #Save outputfile
    predictions['BOM','Part','rating'].distinct().na.drop(subset="Part").coalesce(1).write.format("csv").option("header", "true").save(sys.argv[2])

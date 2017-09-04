//####################################################################################
//#Author:Reddy Anil Kumar		                                             #
//#Description: This script sets the kafka properties and uses the tikaparse.scala   #
//#             script to parse the documents. After parsing and extracting the text #
//#             from documents, each documents text is sent as one message to        #
//#             kafka broker.                                                        #
//#@Input:  Path to the directory with documents to parse.                           #                                 
//####################################################################################

import java.util.concurrent.Future
import java.util.Properties;
import scala.util.Random;
import org.apache.kafka.clients.producer.{Callback,ProducerConfig, KafkaProducer, ProducerRecord}
import java.util.Date;
import org.apache.kafka.clients.producer.RecordMetadata
import java.io.File;
import java.nio.file.{DirectoryStream, Files,Path}
import org.apache.tika.metadata.Metadata
import org.apache.tika.sax.BodyContentHandler
import org.apache.tika.parser.{ParseContext, AutoDetectParser}
import java.io.ByteArrayInputStream
import java.io.FileInputStream
import java.io.InputStream
import java.io.IOException
import org.apache.tika.exception.TikaException
import org.apache.tika.parser.Parser
import org.apache.tika.parser.ParserDecorator
import org.xml.sax.ContentHandler
import org.xml.sax.SAXException
import java.util.concurrent.atomic._

object Producer {
  def main(args: Array[String]): Unit = {
    
    val props = new java.util.Properties()
    //Sets kafka properties.
    props.put("bootstrap.servers","localhost:9092")
    props.put("max.request.size","20000000")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    val producer = new KafkaProducer[String,String](props)
    val kafkaSender = new KafkaSender(props,producer)
    
    //Creating an object of custom TextExtractot class.
    val textExtractor = new TextExtractor()
    var extractedText :Document=null
   
    //Creates a tree of all the files in the directory provided as argument.Parses each file and sends to kafka broker.
    //Before sending,we are extracting content and metadata from the document and concatenating the metadata ,content with 
    //"Dathena" in between and sending it as one message.On the consumer side we can differentiate content and metadata 
    //with respect to string Dathena.
    textExtractor.getFileTree(new File("/home/anil/Anil/test")).foreach{x =>
      if(x.toString().endsWith(".zip"))  textExtractor.extractArchive(x,kafkaSender)
      else if(x.isFile()) {extractedText =textExtractor.extract(x.toPath());
       kafkaSender.sendMessage(x,extractedText.content +"\n"+extractedText.metadata)}}

    //Closing the producer object.
    producer.close()
 }

//Callback method to check whether a message is delivered successfully or not.
class KafkaSender(var configs: Properties, var producer: KafkaProducer[String,String])
    {

  var reqSeq: AtomicInteger = new AtomicInteger(0)

  def sendMessage(file:File,message:String) {
    val messageString = "Message No. " + reqSeq.incrementAndGet() + " at " + 
      new Date()
    val record = new ProducerRecord[String,String]("kafkatopic", message)
    producer.send(record, new Callback() {

      override def onCompletion(metadata: RecordMetadata, exception: Exception) {
        if (exception == null) {
          println("Message Delivered Successfully: " + messageString+"\n")
        } else {
          println("Message Could not be delivered : " + messageString + 
            ". Cause: " + 
            exception.getMessage)
        }
      }
    })
    println("Message Sent:"+ file.getName() + " with " + messageString)
  }
}

} 




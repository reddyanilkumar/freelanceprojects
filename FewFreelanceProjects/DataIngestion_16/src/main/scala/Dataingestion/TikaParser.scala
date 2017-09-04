//####################################################################################
//#Author:Reddy Anil Kumar		                                             #
//#Description: This script uses the tika API's to extract the text from documents.  #
//#             Included the API's tp parse all formats(ex:pdf,doc,xls etc.) and     #
//#             archive files as well.                                               #                                 
//####################################################################################

import collection.JavaConversions._
import java.io.InputStream
import java.nio.file.{DirectoryStream, Files, Path}
import org.apache.tika.metadata.Metadata
import org.apache.tika.sax.BodyContentHandler
import org.apache.tika.parser.{ParseContext, AutoDetectParser}
import java.io.ByteArrayInputStream
import java.io.File;
import java.io.FileInputStream
import java.io.IOException
import org.apache.tika.exception.TikaException
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.Parser
import org.apache.tika.parser.ParserDecorator
import org.xml.sax.ContentHandler
import org.xml.sax.SAXException
import org.apache.kafka.clients.producer.{ProducerRecord,KafkaProducer}
import Producer.KafkaSender

class Document(val metadata: Map[String, String], val content: String)

class TextExtractor {
  
 //Auto detects the parser for diffrent document formats.
 val parser = new AutoDetectParser()

 //Function that creates InputStream object from Path object(File path)
 //Input: File Path.
 //Output: Stream object.
 def extract(file: Path): Document = {
    val stream = Files.newInputStream(file)
    try {
      extract(Files.newInputStream(file))
    } finally {
      stream.close()
    }
  }
  
 //Fucntion that the parses the input stream.
 //Input: Stream Object.
 //Output: Document Object which contains content and metadata.
 def extract(inputStream: InputStream): Document = {
    val metadata = new Metadata()
    val parserContext = new ParseContext()
    val handler = new BodyContentHandler(-1)
    try{
    parser.parse(inputStream, handler, metadata, parserContext)
    }
    catch{
       case ex: Exception => {
            println("Please check the file, Parsing Exception.")
         }
  }
    finally{
      inputStream.close()
    }
    new Document(buildMetada(metadata), buildContent(handler))
  }

 //Funciton to build a Map of input data
 //Input: Meta data.
 //Output: Map collection with key,value pairs of the input metadata.
 def buildMetada(metadata: Metadata): Map[String, String] = {
    metadata.names().foldLeft(Map[String, String]()) {
      //(m, s) => m + (s -> metadata.get(s))
      (m, s) => m + (s -> metadata.get(s))
    }
  }

 //Function that converts the content to String
 //Input: Content.
 //Output: Content converted to String.
 def buildContent(content: BodyContentHandler): String = {
    content.toString
  }
 
 //Function to list the contents of a directory recursively 
 //Input: File or directory path.
 //Output: list of all files in the directory
 def getFileTree(f: File): Stream[File] = f #::
    (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree) else Stream.empty)

 //Function to extract the contents of archive file.
 //Input: Archive file path, KafkaProducer with configurations set.
 def extractArchive(file:File,producer:KafkaSender){
    val parser =new RecursiveMetadataParser(new AutoDetectParser(),producer,file)
    val context = new ParseContext()
    context.set(classOf[Parser], parser)
    val handler = new BodyContentHandler(-1)
    val metadata = new Metadata()
    val stream = new FileInputStream(file)
    try {
      parser.parse(stream, handler, metadata, context)
    } finally {
      stream.close()
    }
  }
 
 //Wrapper to extract the content from archive file recursively
 class RecursiveMetadataParser(parser: Parser,producer:KafkaSender,file:File) extends ParserDecorator(parser) {

    override def parse(stream: InputStream, 
        ignore: ContentHandler, 
        metadata: Metadata, 
        context: ParseContext) {
      val content = new BodyContentHandler(-1)
      super.parse(stream, content, metadata, context)
      if(metadata.get("Content-Type") != "application/zip"){
      producer.sendMessage(file,content.toString() +"\n"+
       buildMetada(metadata))
      }
    }
}
} 

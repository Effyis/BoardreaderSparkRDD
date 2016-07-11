package boardreader.spark.rdd
import org.apache.http.client.methods.HttpGet
import scala.util.parsing.json._
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.auth.BasicScheme

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.Logging
import org.apache.spark.rdd._
import scala.collection.JavaConversions._

class BoardreaderPartition(idx: Int) extends Partition {
	override def index: Int = idx
}

// TODO: Expose a jdbcRDD function in SparkContext and mark this as semi-private
/**
 * SampleRDD which reads from a REST API
 */
class BoardreaderRDD[T: ClassTag](
		sc: SparkContext,		
		url: String,	
		numPartitions: Int,
		mapRow: (String) => T = BoardreaderRDD.testIter _)
		extends RDD[T](sc, Nil) with Logging {

	override def getPartitions: Array[Partition] = 
	{
		(0 until numPartitions).map { i =>			
			new BoardreaderPartition(i)
	}.toArray
 }

override def compute(thePart: Partition, context: TaskContext): Iterator[T] =
{
	    val part = thePart.asInstanceOf[BoardreaderPartition]
	    
			val httpClient = new DefaultHttpClient()
	    val finalurl = url+"&limit=200";
	    var httpGet = new HttpGet(finalurl);
	   
	    val httpResponse = httpClient.execute(httpGet)
			val entity = httpResponse.getEntity()

			val inputStream = entity.getContent()
			var content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
			inputStream.close

			httpClient.getConnectionManager().shutdown()			

			val listOutput = List(content);
	    val output2 = listOutput.asInstanceOf[List[T]];
	    val it = output2.iterator;
	    return it;
	}
}

object BoardreaderRDD 
{
	
	def testIter(temp:String):String= {
			temp;
	}

	def create[T](
			sc: JavaSparkContext,			
			url: String,			
			numPartitions: Int,
			mapRow: JFunction[String, String]): JavaRDD[String] = 
  {
			val boardreaderRDD = new BoardreaderRDD[String](
					sc.sc,
					url,					
					numPartitions,
					(temp: String) => mapRow.call(temp))

					new JavaRDD[String](boardreaderRDD)
	}	
	def create(
			sc: JavaSparkContext,
			url: String,			
			numPartitions: Int): JavaRDD[String] = {

			val mapRow = new JFunction[String, String] {
				override def call(temp: String): String = {
						testIter(temp)
				}
			}

			create(sc, url, numPartitions, mapRow)
	}
}
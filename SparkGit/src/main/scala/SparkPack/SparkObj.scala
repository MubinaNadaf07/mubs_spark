package SparkPack

import org.apache.spark.SparkConf  
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.spark.sql.functions._
import scala.io.Source

object SparkObj {
 
   def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark= SparkSession.builder().getOrCreate()
					import spark.implicits._
					
					
					val flattendf=spark.read.format("json").option("multiline", "true").load("file:///D:/Mubina/data/arrayjson.json")
					
					flattendf.show()
					flattendf.printSchema()
  }
}
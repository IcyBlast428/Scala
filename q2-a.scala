import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.MurmurHash
import org.apache.spark.SparkContext._
import org.apache.spark.sql

val flightsDF = spark.read.parquet("/home/icyblast/Desktop/flight_2008_pq.parquet")

val flightsDFNoNA = flightsDF.filter("DepDelay != 'NA'")

val flightsDFStrInt = flightsDFNoNA.withColumn("DepDelay", $"DepDelay" cast "Int")

val flightsSelected = flightsDFStrInt.select($"FlightNum",$"DepDelay")

val flightsSorted = flightsSelected.sort($"DepDelay".desc)

flightsSorted.show(20)

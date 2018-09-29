
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.MurmurHash
import org.apache.spark.SparkContext._

sc.setLogLevel("OFF")

val df_1 = spark.read.option("header", "true").csv("/home/icyblast/Documents/FIT5202/Week08/flights_2008.csv.bz2")
val flightsFromTo = df_1.select($"Origin", $"Dest")
val airportCodes = df_1.select($"Origin", $"Dest").flatMap(x => Iterable(x(0).toString, x(1).toString))

val airportVertices: RDD[(VertexId, String)] = airportCodes.rdd.distinct().map(x => (MurmurHash.stringHash(x), x))

val defaultAirport = ("Missing")

val flightEdges = flightsFromTo.rdd.map(x => ((MurmurHash.stringHash(x(0).toString), MurmurHash.stringHash(x(1).toString)), 1)).reduceByKey(_+_).map(x => Edge(x._1._1, x._1._2, x._2))

val graph = Graph(airportVertices, flightEdges, defaultAirport)

graph.persist()

graph.numVertices
graph.numEdges

graph.triplets.sortBy(_.attr, ascending = false).map(triplet => "There were " + triplet.attr.toString + " flights from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(10)

graph.triplets.sortBy(_.attr, ascending = true).map(triplet => "There were " + triplet.attr.toString + " flights from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(10)

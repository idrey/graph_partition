import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD	
import java.util.Calendar

object lj {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("lj")
            .getOrCreate()
        val sc = spark.sparkContext
        val graph = GraphLoader.edgeListFile(sc, "/home/idrey/Spark/soc-LiveJournal1.txt")
        val vertexCount = graph.numVertices 
        val edges = graph.edges
        println("vertexCount", vertexCount)
        println("edgesCount", edges.count())
        spark.stop()
    }
}
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat
import java.util.Date

object par {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("par")
            .getOrCreate()
        val sc = spark.sparkContext
        val now = new Date()
        val graph = GraphLoader.edgeListFile(sc, "/home/idrey/Spark/graphpar2/wiki.txt") // 读入数据
        var node: Set[Long] = Set()
        

        val partition:Int = 8 // 图划分个数
        for (i <- 1 to partition) {
            var node: Set[Long] = Set()
            // 先划分边
            val subgraph = graph.subgraph(epred = edge => PartitionStrategy.RandomVertexCut.getPartition(edge.srcId, edge.dstId, i) == 1)
            subgraph.edges.collect.foreach(e => {
            node += e.srcId
            node += e.dstId
            })
            // 根据边的划分来划分点
            val subgraph2 = subgraph.subgraph(vpred = (id, _) => node.contains(id))
            println("Partition" + i.toString)
            println(subgraph2.vertices.count())
            println(subgraph2.edges.count())
        }

        //统计程序运行时间
        val now2: Date = new Date()
        val now3 = now2.getTime - now.getTime
        val dataFormat: SimpleDateFormat = new SimpleDateFormat("mm:ss:SSS")
        val date = dataFormat.format(now3)
        println(date)
        spark.stop()
    }
}
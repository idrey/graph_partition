import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object par {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("par")
            .getOrCreate()
        val sc = spark.sparkContext
        val graph = GraphLoader.edgeListFile(sc, "/home/idrey/Spark/graphpar2/wiki.txt")
        var node: Set[Long] = Set()
        // val subgraph = graph.partitionBy(PartitionStrategy.RandomVertexCut,2)


        val subgraph = graph.subgraph(epred = edge => PartitionStrategy.RandomVertexCut.getPartition(edge.srcId, edge.dstId, 2) == 1)
        println("vertices", subgraph.vertices.count())
        println("edges", subgraph.edges.count())
        subgraph.edges.collect.foreach(e => {
            node += e.srcId
            node += e.dstId
            })
        // println("node", node)
        val subgraph2 = subgraph.subgraph(vpred = (id, _) => node.contains(id))
        println("vertices2", subgraph2.vertices.count())
        println("edges2", subgraph2.edges.count())
        
                // var random_pi_index:Int = 0
                // var random_pi = vertexPartition.apply(i).apply(random_pi_index)
                // var random_u_index:Int = 0
                // var random_u = neighborsMap.apply(random_pi).apply(random_u_index)
                // var flag:Boolean = false
                // breakable{
                // while(!vertexBuffer.contains(random_u)) {
                //     random_pi_index += 1
                //     random_u_index += 1
                //     if (random_pi_index == vertexPartition.apply(i).size && random_u_index == neighborsMap.apply(random_pi).size) {
                //         flag = true
                //         break
                //     }
                //     random_pi = vertexPartition.apply(i).apply(random_pi_index)
                //     random_u = neighborsMap.apply(random_pi).apply(random_u_index)
                // }
                // }
                // if (flag == false) {
                //     val index_in_vBuffer = vertexBuffer.indexOf(random_u)
                //     vertexBuffer.remove(index_in_vBuffer)
                //     vertexPartition.apply(i) += random_u
                //     edgePartition += ((random_pi, random_u) -> i)
                //     partitionVertex += (random_u -> i)
                // }
        spark.stop()
    }
}
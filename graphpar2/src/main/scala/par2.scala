import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object par2 {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("par")
            .getOrCreate()
        val sc = spark.sparkContext
        val graph = GraphLoader.edgeListFile(sc, "wiki.txt")
        // val graph = ori_graph.connectedComponents()
        var vertexPartition: Map[Int, ArrayBuffer[VertexId]] = Map()
        var vertexBuffer = ArrayBuffer[VertexId]()
        var neighborsMap: Map[VertexId, ArrayBuffer[VertexId]] = Map()
        var edgePartition: Map[(VertexId, VertexId), Int] = Map()
        var partitionVertex: Map[VertexId, Int] = Map()
        var is_Error:Boolean = false
        graph.vertices.collect().foreach(id => vertexBuffer += id._1)
        graph.edges.collect.foreach( e => {
            if(neighborsMap.get(e.srcId) == None) {
                var tmp_array = ArrayBuffer[VertexId](e.dstId)
                neighborsMap += (e.srcId -> tmp_array)
            }
            else {
                neighborsMap.apply(e.srcId) += e.dstId
            }
        })
        // println(neighborsMap)
        val partition:Int = 2
        var partitionIndex = ArrayBuffer[Int]()
        for (i <- 1 to partition) {
            val idx = Random.nextInt(vertexBuffer.size)
            val pid = vertexBuffer.apply(idx)
            if (vertexPartition.get(i) == None) {
                var tmp_array = ArrayBuffer[VertexId](pid)
                vertexPartition += (i -> tmp_array)
            }
            else {
                vertexPartition.apply(i) += pid
            }
            partitionVertex += (pid -> i)
            vertexBuffer.remove(idx)
        }


        
        for (i <- 0 to partition) {
            partitionIndex += 0
        }
        breakable {
        while (vertexBuffer.size > 0) {
            val pre_size = vertexBuffer.size
            println(pre_size)
            for ( i <- 1 to partition) {
                var random_pi_index:Int = 0
                val begin:Int = partitionIndex.apply(i)
                for (random_pi_index <- begin until vertexPartition.apply(i).size) {
                    var random_pi = vertexPartition.apply(i).apply(random_pi_index)
                    var random_u_index:Int = 0
                    var random_u:VertexId = 0;
                    var flag:Boolean = false
                    // println(vertexPartition)
                    // println(i)
                    // println(vertexBuffer.size)
                    // println(random_pi)
                    // println(random_u)
                    if (neighborsMap.get(random_pi) != None) {
                        random_u = neighborsMap.apply(random_pi).apply(random_u_index)
                        breakable {
                            while(!vertexBuffer.contains(random_u)) {
                                random_u_index += 1
                                if (random_u_index == neighborsMap.apply(random_pi).size) {
                                    flag = true
                                    partitionIndex(i) = partitionIndex.apply(i).max(random_pi_index + 1)
                                    break
                                }
                                random_u = neighborsMap.apply(random_pi).apply(random_u_index)
                            }
                        }
                    }
                    else flag = true
                    if(flag == false) {
                        val index_in_vBuffer = vertexBuffer.indexOf(random_u)
                        vertexBuffer.remove(index_in_vBuffer)
                        vertexPartition.apply(i) += random_u
                        // edgePartition += ((random_pi, random_u) -> i)
                        partitionVertex += (random_u -> i)
                    }
                }
            }
            val post_size = vertexBuffer.size
            // println(post_size)
            if (pre_size == post_size) {
                val minnum = vertexBuffer.size.min(partition)
                for (j <- 1 to minnum) {
                    val idx = Random.nextInt(vertexBuffer.size)
                    val pid = vertexBuffer.apply(idx)
                    vertexPartition.apply(j) += pid
                    partitionVertex += (pid -> j)
                    vertexBuffer.remove(idx)
                }
                // println("Error occurs")
                // is_Error = true
                // break
            }
        }
        }

        if (is_Error == false) {
                graph.edges.collect.foreach(e =>{
                if (partitionVertex.apply(e.srcId) != partitionVertex.apply(e.dstId)) {
                    if (Random.nextInt(2) == 0) {
                        edgePartition += ((e.srcId, e.dstId) -> partitionVertex.apply(e.srcId))
                    }
                    else {
                        edgePartition += ((e.srcId, e.dstId) -> partitionVertex.apply(e.dstId))
                    }
                }
                else {
                    edgePartition += ((e.srcId, e.dstId) -> partitionVertex.apply(e.srcId))
                }
            })
            println(vertexPartition.apply(1).size)
            println(vertexPartition.apply(2).size)
            println(edgePartition.size)
        }
        spark.stop()
    }
}
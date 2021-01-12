import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import java.text.SimpleDateFormat
import java.util.Date

object par2 {
    def main(args: Array[String]): Unit = {
        
        val spark = SparkSession
            .builder
            .appName("par")
            .getOrCreate()
        val sc = spark.sparkContext
        val now = new Date()
        val graph = GraphLoader.edgeListFile(sc, "wiki.txt")
        // val graph = ori_graph.connectedComponents()
        var vertexPartition: Map[Int, ArrayBuffer[VertexId]] = Map() //顶点划分映射，格式[划分ID -> 对应顶点集合]
        var vertexBuffer = ArrayBuffer[VertexId]()  // 储存未划分的顶点
        var neighborsMap: Map[VertexId, ArrayBuffer[VertexId]] = Map() // 邻居映射，格式[顶点ID -> 对应邻居集合]
        var edgePartition: Map[(VertexId, VertexId), Int] = Map() //边划分映射，格式[(源点，汇点) -> 划分ID]
        var partitionVertex: Map[VertexId, Int] = Map() //顶点划分映射，格式[顶点ID -> 映射ID]
        
        //初始化vertexPartition和neighborsMap
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
        // 图划分个数
        val partition:Int = 20
        // 
        var partitionIndex = ArrayBuffer[Int]()
        // 为每个划分随机选择一个点
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

            // 打印未访问的点数
            val pre_size = vertexBuffer.size
            println(pre_size)
            //针对每个划分，使其尽可能地向外拓展一个点
            for ( i <- 1 to partition) {
                var random_pi_index:Int = 0
                val begin:Int = partitionIndex.apply(i)
                for (random_pi_index <- begin until vertexPartition.apply(i).size) {
                    var random_pi = vertexPartition.apply(i).apply(random_pi_index) //从划分中选择一个点
                    var random_u_index:Int = 0
                    var random_u:VertexId = 0;
                    var flag:Boolean = false
                    
                    //从选出的点的邻居中选出点向外拓展
                    if (neighborsMap.get(random_pi) != None) {
                        random_u = neighborsMap.apply(random_pi).apply(random_u_index)
                        breakable {
                            while(!vertexBuffer.contains(random_u)) {
                                random_u_index += 1
                                if (random_u_index == neighborsMap.apply(random_pi).size) {
                                    //邻居都已经搜索过了
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
            //从邻居拓展未成功则随机选一个点拓展
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

        //根据点的划分确定边的划分
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

        //统计程序运行时间
        val now2: Date = new Date()
        val now3 = now2.getTime - now.getTime
        val dataFormat: SimpleDateFormat = new SimpleDateFormat("mm:ss:SSS")
        val date = dataFormat.format(now3)
        println(date)
        spark.stop()
    }
}
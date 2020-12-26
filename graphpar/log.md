# Graph Partition

- Spark子图划分算法
> PartitionStrategy.RandomVertexCut.getPartition(edge.srcId, edge.dstId, numParts)
> numParts代表的是要划分的子图个数，返回边的所属子图ID
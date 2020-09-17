import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {
  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(config)

    var graph = sc.textFile(args(0)).map( x => {val a = x.split(",")
      var adj_node = List[Long]()
      for(i <- 1 to (a.length-1))
      {adj_node = (adj_node :+ (a(i).toLong)).toList}
      (a(0).toLong,a(0).toLong,adj_node)
    })

    var graph1 = graph.map(group => (group._1,group))

    for (i <- 1 to 5) {
      graph = graph.flatMap(node => {
        node._3.map(adjacent => {
          (adjacent, node._2)
        }).+:((node._1, node._2))
      }).reduceByKey((a, b) => (if (a >= b) b else a))
        .join(graph1)
        .map(node => (node._1, node._2._1, node._2._2._3))
    }
    graph.map(node => (node._2,1))
    .reduceByKey((a,b) => a+b)
    .collect().foreach(println)
    }
}
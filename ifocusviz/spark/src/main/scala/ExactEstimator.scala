import java.nio.file.{Paths, Files}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
  * Usage: ExactEstimator [location of delays file] 
  */
object ExactEstimator {
  def main(args: Array[String]) {
    if (args.length == 0 /* || !Files.exists(Paths.get(args(0)))*/) {
      println("Usage: ExactEstimator [location of delays file]")
      System.exit(1)
    }

    val delaysFile = args(0)
    val sparkConf = new SparkConf().setAppName("Exact Estimator")
    val sc = new SparkContext(sparkConf)

    val delaysData = sc.textFile(delaysFile)
    println("Number of total entries: %s".format(delaysData.count()))

    val groupedData = delaysData.map(line => {
      line.split(",") match {
        case Array(state, delay) => (state, delay.toInt)
      }
    }).combineByKey(
      (_, 1),
      (comb: Tuple2[Int, Int], value: Int) =>
          (comb._1 + value, comb._2 + 1),
      (comb1: Tuple2[Int, Int], comb2: Tuple2[Int, Int]) =>
          (comb1._1 + comb2._1, comb1._2 + comb2._2)
    ).map(_ match {
      case (state, (sum, count)) => (state, sum.toDouble / count)
    })

    println("State | Average Delay (Mins)")
    groupedData.collect().foreach(println)
  }
}

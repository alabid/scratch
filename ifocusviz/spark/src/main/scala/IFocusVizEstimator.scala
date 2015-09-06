import java.nio.file.{Paths, Files}
import Math.{log => mlog, _}

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import collection.mutable.{Map => MutMap}

/**
  * Usage: IFocusVizEstimator [location of delays file] 
  */
object IFocusVizEstimator {

  var totalSampled: Int = 0

  /**
    *  Returns a sample of constant size.
    */
  def constantSample(group: RDD[Int]) : Array[Int] = {
    val x = group.takeSample(false, 20)
    totalSampled += x.size
    x
  }

  /**
    * Aggregation operator: sum, avg, ...
    */
  def agg(arr: Array[Int]) : Double = {
    arr.sum.toDouble / arr.size
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if (args.length == 0 /* || !Files.exists(Paths.get(args(0)))*/) {
      println("Usage: IFocusVizEstimator [location of delays file]")
      System.exit(1)
    }

    val delaysFile = args(0)
    val sparkConf = new SparkConf().setAppName("IFocusViz Estimator")
    val sc = new SparkContext(sparkConf)

    val delaysData = sc.textFile(delaysFile)
    println("Number of total entries: %s".format(delaysData.count()))

    val groupedData = delaysData.map(line => {
      line.split(",") match {
        case Array(state, delay) => (state, delay.toInt)
      }
    }).groupByKey().collect().map(
      _ match {
        case (state, elems) => (state, sc.parallelize(elems.toList).cache())
      }
    ).toMap

    val startTime = System.nanoTime
    for ((state, approxAggValue) <- ifocusViz(groupedData, 0.05)) {
      println("state: %s; approx average delay:%.2f; E(v)=%d".format(
        state,
        approxAggValue,
        encoding(approxAggValue)
      ))
    }
    println("IFocusVizEstimator ran for %s".format((System.nanoTime-startTime)/1e9))
  }

  /**
    * Encoding function for this estimator.
    */
  def encoding(v: Double): Int = {
    floor(v/4.0).toInt
  }

  /**
    * Bivariate perceptual function for this estimator.
    */
  def perceptual(e1: Double, e2:Double): Int = {
    // universal perceptual function
    0
  }

  def overlap(approxs: MutMap[String, Double], key: String, eps: Double): Boolean = {
    val ival1 = (approxs(key) - eps, approxs(key) + eps)

    for ((otherKey, otherVal) <- approxs if key != otherKey) {
      val ival2 = (otherVal - eps, otherVal + eps)
      if (!(ival2._2 < ival1._1 || ival2._1 > ival1._2)) {
        val ei1 = encoding(ival1._1)
        val ei2 = encoding(ival1._2)
        val ej1 = encoding(ival2._1)
        val ej2 = encoding(ival2._2)
        if (ej2 - ei1 > perceptual(ej2, ei1) ||
          ei2 - ej1 > perceptual(ei2, ej1)) {
          return true
        }
      }
    }

    false
  }

  def ifocusViz(groupMap: Map[String, RDD[Int]], delta: Double): MutMap[String, Double] = {
    val approxs = MutMap[String, Double]()
    var done = groupMap.keys.toSet
    // number of groups
    val k = done.size
    var m = 1

    var groupCounts = MutMap[String, Long]()
    for (key <- done) {
      groupCounts(key) = groupMap(key).count()
    }

    // Take 1 sample from each group S_1, ..., S_k to provide initial
    // estimates v_1, ..., v_k
    for (key <- groupMap.keys) {
      approxs(key) = agg(constantSample(groupMap(key)))
    }

    while (done.size > 0) {
      m += 1

      for (key <- done) {
        val x = constantSample(groupMap(key))
        approxs(key) = (m-1.)/m * approxs(key) + agg(x)/m
      }

      val maxGroupCount = groupCounts.values.reduce(max(_, _))

      val eps = sqrt(
        (1 - (m/2. - 1) / maxGroupCount) *
          ( (2 * mlog(mlog(m)) + mlog(pow(PI, 2) * k / 3 * delta) ) / (2. * m) )
      )

      for (key <- done) {
        if (!overlap(approxs, key, eps)) {
          done -= key
          groupCounts -= key
        }
      }
    }

    println("Iterations: %d".format(m))
    println("Total sampled: %d".format(totalSampled))
    approxs
  }
}

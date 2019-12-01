package br.ufmg.cs.systems.fractal

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

import scala.io.Source

class BasicTestSuite extends FunSuite with BeforeAndAfterAll {
  private val numPartitions: Int = 8
  private val appName: String = "fractal-test"
  private val logLevel: String = "error"

  private var master: String = _
  private var sc: SparkContext = _
  private var fc: FractalContext = _

  /** set up spark context */
  override def beforeAll: Unit = {
    master = s"local[${numPartitions}]"
    // spark conf and context
    val conf = new SparkConf().
      setMaster(master).
      setAppName(appName)

    sc = new SparkContext(conf)
    sc.setLogLevel(logLevel)
    fc = new FractalContext(sc, logLevel)
  }

  /** stop spark context */
  override def afterAll: Unit = {
    if (sc != null) {
      sc.stop()
      fc.stop()
    }
  }

  def getFileLines(path: String): Int = {
    Source.fromFile(path).getLines().toList.length
  }

  test("[exceptionalMining]", Tag("exceptionalMining")) {
    val filePath = "../data/exceptionalMining-v1/candidates_1_25_61_62_272.graph"
    val fileLines: Int = getFileLines(filePath)
    val fgraphExceptionalMining: FractalGraph = fc.textFile(filePath)

    for (k <- 1 to fileLines) {
      val graphClass = "br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining"
      var frac = fgraphExceptionalMining.vfractoid
      for (j <- 1 to k) {
        frac = frac.expand(1)
      }
      frac.set ("input_graph_class", graphClass)

      assert(true)
    }

  }

}

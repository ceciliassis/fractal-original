package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, VertexInducedSubgraph}
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

  private val SIGMA = 1
  private val DELTA = 0.05

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

  def wracc(v: VertexInducedSubgraph, c: Computation[VertexInducedSubgraph]): Boolean = {
    val vertices = v.getVertices

    if (vertices.getSize < SIGMA) {
      return false
    }

    true
  }

  test("[exceptionalMining]", Tag("exceptionalMining")) {
    val graphClass = "br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining"
    val filePath = "../data/exceptionalMining-v1/candidates_1_25_61_62_272.graph"
    val fileLines: Int = getFileLines(filePath)
    val fgraphExceptionalMining: FractalGraph = fc.textFile(filePath)

//    var subgraphs = List()

    for (k <- 1 to fileLines) {
      var frac = fgraphExceptionalMining.vfractoid.set("input_graph_class", graphClass)
      for (j <- 1 to k) {
        frac = frac.expand(1)
      }
      frac = frac.filter(wracc)

      assert(frac.subgraphs != null)
    }
  }

}

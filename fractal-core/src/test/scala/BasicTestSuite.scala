package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

import scala.collection.mutable.ListBuffer
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

//  def gain(vProps, )

  test("[exceptionalMining]", Tag("exceptionalMining")) {
    val graphClass = "br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining"
    var filePath = "../data/exceptionalMining-v1/candidates_1_25_61_62_272.graph"
    filePath = "../data/exceptionalMining-v1/candidates_4_34_249_271.graph"
    val fileLines: Int = getFileLines(filePath)
    val fgraphExceptionalMining: FractalGraph = fc.textFile(filePath)


    val wracc = (vis: VertexInducedSubgraph, cvis: Computation[VertexInducedSubgraph]) => {
      var v = vis.vertex(0)
      var vProps: IntArrayList = v.getProperty

      val posAttsLen: Int = vProps.get(0)
      val negAttsLen: Int = vProps.get(posAttsLen + 1)

      var posAtts = ListBuffer[Int]()
      var negAtts = ListBuffer[Int]()

      var start = 1
      var end = posAttsLen
      for (i <- start to end){
        posAtts += vProps.get(i)
      }

      start = end + 2
      end = start + negAttsLen - 1
      for (i <- start to end){
        negAtts += vProps.get(i)
      }

      var posAttsTot = 0
      var negAttsTot = 0
      var attsTot = 0

      end += 2

      for (i <- 0 to vis.getNumVertices - 1) {
        v = vis.vertex(i)
        vProps = v.getProperty

        posAtts.foreach((idx) => posAttsTot += vProps.get(end + idx))
        negAtts.foreach((idx) => negAttsTot += vProps.get(end + idx))
        attsTot += vProps.getLast
      }
      false
    }

    for (k <- 1 to fileLines) {
      var frac = fgraphExceptionalMining.vfractoid.set("input_graph_class", graphClass)

      for (j <- 1 to k) {
        frac = frac.expand(1)
      }

      frac = frac.filter(wracc)
//      frac = frac.filter((e,c) => false)

      assert(frac.subgraphs != null)
    }
  }

}

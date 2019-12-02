package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining
import br.ufmg.cs.systems.fractal.graph.Vertex
import br.ufmg.cs.systems.fractal.subgraph.VertexInducedSubgraph
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object ExceptionalMiningApp extends Logging {

  def getFileLines(path: String): Int = {
    Source.fromFile(path).getLines().toList.length
  }

  def main(args: Array[String]): Unit = {
    // environment setup
    val numPartitions: Int = 8
    val master = s"local[${numPartitions}]"
    val conf = new SparkConf().setMaster(master).setAppName("ExceptionalMiningApp")
    val sc = new SparkContext(conf)
    val fc = new FractalContext(sc)

    //  ENERGETICS
    val SIGMA = 1
    val DELTA = 0.05

    //  [ENERGETICS] Graph init
    val exceptionalGraphClass = "br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining"
    var graphPath = "data/exceptionalMining-v1/NYCFoursquareGraph.graph"

    val loadExceptionalMainGraph = {
      val graph = fc.textFile(graphPath).vfractoid
        .set("input_graph_class", exceptionalGraphClass)
        .expand(1)
      graph.compute()
      graph.config.getMainGraph.asInstanceOf[ExceptionalMining]
    }

    val graph: ExceptionalMining = loadExceptionalMainGraph
    val gVertsLen = graph.getNumberVertices()
    val gVerts = graph.getVertices

    val vIds = {
      val ids = new IntArrayList()
      for (i <- 0 to graph.getNumberVertices - 1) {
        ids.add(graph.getVertex(i).getVertexId)
      }
      ids
    }

    //  [ENERGETICS] Calculation functions
    //    [onpaper] sum(K) or sum(V)

    val sumK = (vs: IntArrayList) => {
      var vAttTot = 0.0
      vs.toIntArray.foreach(v => gVerts(v).getProperty.toIntArray.foreach(vAttTot += _))
      vAttTot
    }

    //    [onpaper] sum(L,K) or sum(L,V)
    val sumLK = (ls: IntArrayList, vs: IntArrayList) => {
      var attsTot = 0.0
      vs.toIntArray.foreach {
        v => ls.toIntArray.foreach(l => attsTot += gVerts(v).getProperty.get(l))
      }
      attsTot
    }

    val graphIds: IntArrayList = vIds
    val graphAttsTot: Double = sumK(graphIds)

    //    [onpaper] gain(L, K)
    val gain = (vis: VertexInducedSubgraph) => {
      //    Get props
      val props: IntArrayList = vis.vertex(0).getProperty.asInstanceOf[IntArrayList]

      val posAttsLen: Int = props.get(0)
      val negAttsLen: Int = props.get(posAttsLen + 1)

      val posAtts = new IntArrayList
      val negAtts = new IntArrayList

      var startIdx = 1
      var endIdx = posAttsLen
      for (i <- startIdx to endIdx) {
        posAtts.add(props.get(i))
      }

      startIdx = endIdx + 2
      endIdx = startIdx + negAttsLen - 1
      for (i <- startIdx to endIdx) {
        negAtts.add(props.get(i))
      }

      //    Calc gain
      val kIds = new IntArrayList
      for (v <- vis.getVertices.toIntArray) {
        kIds.add(vis.vertex(v).getVertexLabel)
      }

      val kAttsTot: Double = sumK(kIds)

      val posAttsGain: Double = (sumLK(posAtts, kIds) / kAttsTot) - (sumLK(posAtts, graphIds) / graphAttsTot)
      val negAttsGain: Double = (sumLK(negAtts, kIds) / kAttsTot) - (sumLK(negAtts, graphIds) / graphAttsTot)

      posAttsGain - negAttsGain
    }

    //    var vGraphAttsTot = new IntArrayList

    val wracc = (vis: VertexInducedSubgraph, cvis: Computation[VertexInducedSubgraph]) => {
      gain(vis) > 0
    }

    //  -------------------------------------------

    graphPath = "data/exceptionalMining-v1/candidates_1_25_61_62_272.graph"

    val fileLines: Int = getFileLines(graphPath)
    val fgraph = fc.textFile(graphPath)
    var frac: Fractoid[VertexInducedSubgraph] = fgraph.vfractoid.set("input_graph_class", exceptionalGraphClass)
    frac = frac.expand(1)
    frac = frac.filter(wracc)

    for (k <- 1 to fileLines) {
      frac = fgraph.vfractoid.set("input_graph_class", exceptionalGraphClass)
      for (_ <- 1 to k) {
        frac = frac.expand(1)
      }
      frac = frac.filter(wracc)
    }

    // environment cleaning
    fc.stop()
    sc.stop()
  }
}

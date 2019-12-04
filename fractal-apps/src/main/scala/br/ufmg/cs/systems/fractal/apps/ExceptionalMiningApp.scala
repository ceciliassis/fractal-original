package br.ufmg.cs.systems.fractal.apps

import java.io.File

import br.ufmg.cs.systems.fractal.{FractalGraph, _}
import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining
import br.ufmg.cs.systems.fractal.subgraph.{ResultSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
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
    val graphPath = "data/exceptionalMining-v1/main.graph"

    val loadExceptionalMainGraph: ExceptionalMining = {
      val graph = fc.textFile(graphPath).vfractoid
        .set("input_graph_class", exceptionalGraphClass)
        .expand(1)
      graph.compute()
      graph.config.getMainGraph.asInstanceOf[ExceptionalMining]
    }

    val graph: ExceptionalMining = loadExceptionalMainGraph
    val gVertsLen = graph.getNumberVertices()
    val gVerts = graph.getVertices

    val vIds: IntArrayList = {
      val ids = new IntArrayList()
      for (i <- 0 to gVertsLen - 1) {
        ids.add(gVerts(i).getVertexId)
      }
      ids
    }

    //  [ENERGETICS] Calculation functions
    //    [onpaper] sum(K) or sum(V)
    val sumK: IntArrayList => Double = vs => {
      var vAttTot = 0.0
      vs.toIntArray.foreach(v => gVerts(v).getProperty.toIntArray.foreach(vAttTot += _))
      vAttTot
    }

    //    [onpaper] sum(L,K) or sum(L,V)
    val sumLK: (IntArrayList, IntArrayList) => Double = (ls, vs) => {
      var attsTot = 0.0
      vs.toIntArray.foreach {
        v => ls.toIntArray.foreach(l => attsTot += gVerts(v).getProperty.get(l))
      }
      attsTot
    }

    val graphIds: IntArrayList = vIds
    val graphAttsTot: Double = sumK(graphIds)

    val gain: (IntArrayList, IntArrayList, Double) => Double = (kIds, atts, kAttsTot) => {
      (sumLK(atts, kIds) / kAttsTot) - (sumLK(atts, graphIds) / graphAttsTot)
    }

    val valid: (IntArrayList, IntArrayList, IntArrayList, IntArrayList) => Boolean = (kIds, kProps, posAtts, negAtts) => {
      if (gain(kIds, posAtts, kProps.getLast) < 0.0) {
        false
      } else if (gain(kIds, negAtts, kProps.getLast) > 0.0) {
        false
      } else {
        true
      }
    }

    val props: VertexInducedSubgraph => List[IntArrayList] = vis => {
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

      List(posAtts, negAtts)
    }

    //    [onpaper] A(S, K)
    val aMeasure: (VertexInducedSubgraph, IntArrayList, IntArrayList) => Double = (vis, posAtts, negAtts) => {
      //    Calc gain
      val kIds = new IntArrayList
      for (v <- vis.getVertices.toIntArray) {
        kIds.add(vis.vertex(v).getVertexLabel)
      }

      val kAttsTot: Double = sumK(kIds)
      gain(kIds, posAtts, kAttsTot) - gain(kIds, negAtts, kAttsTot)
    }

    val wracc = (vis: VertexInducedSubgraph, cvis: Computation[VertexInducedSubgraph]) => {
      if (vis.getNumVertices > SIGMA) {
        val atts = props(vis)
        val posAtts = atts(0)
        val negAtts = atts(1)

        var i = 0
        val kIds = new IntArrayList
        var v = vis.vertex(0)
        kIds.add(v.getVertexLabel)

        while (valid(kIds, v.getProperty, posAtts, negAtts) && i < vis.getNumVertices) {
          kIds.clear()
          i += 1

          kIds.add(v.getVertexLabel)
          v = vis.vertex(i)
        }

        aMeasure(vis, posAtts, negAtts) * (vis.getNumVertices / gVertsLen) > DELTA
      } else {
        false
      }

    }

    val getFiles = (dirPath: String) => {
      new File(dirPath).listFiles().filter(_.getName.endsWith(".graph"))
    }

    //  RUN
    val startTime = System.currentTimeMillis

    val dirPath = "data/exceptionalMining-v1/candidates"
    val files = getFiles(dirPath)

    var subgraphs = new ListBuffer[RDD[ResultSubgraph[_]]]

    var fileLines = 0
    var fGraph: FractalGraph = null
    var filePath = ""

    val expanded: (Int, FractalGraph) => Fractoid[VertexInducedSubgraph] = (k, fGraph) => {
      var frac = fGraph.vfractoid.set("input_graph_class", exceptionalGraphClass)
      for (_ <- 1 to k) {
        frac = frac.expand(1)
      }
      frac
    }

    files.foreach {
      file =>
        filePath = file.getPath
        fGraph = fc.textFile(filePath)
        fileLines = getFileLines(filePath)
        for (k <- 1 to fileLines) {
          subgraphs += expanded(k, fGraph).filter(wracc).subgraphs
        }
    }


    subgraphs = subgraphs.filter(rdd => !rdd.isEmpty())

    val stopTime = System.currentTimeMillis
    val elapsedTime = stopTime - startTime
    println(s"Elapsed time(s): ${elapsedTime/1000.0}")
//
//    for (s <- subgraphs) {
//      s.coalesce(1).saveAsTextFile(s"data/rdds/${s.name}")
//    }

    // ENV CLEANING
    fc.stop()
    sc.stop()
  }
}

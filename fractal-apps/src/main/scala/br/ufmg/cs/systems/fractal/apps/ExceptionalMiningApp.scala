package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining
import br.ufmg.cs.systems.fractal.subgraph.{ResultSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source


object BKPExceptionalMiningApp extends Logging {
  def main(args: Array[String]): Unit = {
    // environment setup
    val conf = new SparkConf().setAppName("ExceptionalMiningApp")
    val sc = new SparkContext(conf)

    val hdfsFileSystem = FileSystem.get(sc.hadoopConfiguration)

    val hdfsCore = "hdfs://compute1:9000"
    val userFolder = s"${hdfsCore}/user/ceciliassis"

    val fc = new FractalContext(sc, tmpDir=s"${userFolder}/tmp/fractal")

    //  ENERGETICS
    val SIGMA = 1
    val DELTA = 0.05

    //  [ENERGETICS] Graph init
    val fractalDatasets = s"${userFolder}/fractal/data/exceptionalMining-v1/candidates"
    val graphClass = "br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining"
    val graphPath = s"${fractalDatasets}/maingraph/main.graph"

    val mainGraph = fc.textFile(graphPath, graphClass).vfractoid.expand(1)
    mainGraph.compute

    val exceptionalMainGraph = mainGraph.config.getMainGraph.asInstanceOf[ExceptionalMining]

    val gVertsLen = exceptionalMainGraph.getNumberVertices
    val gVerts = exceptionalMainGraph.getVertices

    // val vIds: IntArrayList = {
    //   val ids = new IntArrayList()
    //   for (i <- 0 to gVertsLen - 1) {
    //     ids.add(gVerts(i).getVertexId)
    //   }
    //   ids
    // }

    // //  [ENERGETICS] Calculation functions
    // //  [onpaper] sum(K) or sum(V)
    // val sumK: IntArrayList => Double = vs => {
    //   var vAttTot = 0.0
    //   vs.toIntArray.foreach(v => gVerts(v).getProperty.toIntArray.foreach(vAttTot += _))
    //   vAttTot
    // }

    // //  [onpaper] sum(L,K) or sum(L,V)
    // val sumLK: (IntArrayList, IntArrayList) => Double = (ls, vs) => {
    //   var attsTot = 0.0
    //   vs.toIntArray.foreach {
    //     v => ls.toIntArray.foreach(l => attsTot += gVerts(v).getProperty.get(l))
    //   }
    //   attsTot
    // }

    // val graphIds: IntArrayList = vIds
    // val graphAttsTot: Double = sumK(graphIds)

    // //  [onpaper] gain(L,K)
    // val gain: (IntArrayList, IntArrayList, Double) => Double = (kIds, atts, kAttsTot) => {
    //   (sumLK(atts, kIds) / kAttsTot) - (sumLK(atts, graphIds) / graphAttsTot)
    // }

    // val props: VertexInducedSubgraph => List[IntArrayList] = vis => {
    //   val props: IntArrayList = vis.vertex(0).getProperty.asInstanceOf[IntArrayList]

    //   val posAttsLen: Int = props.get(0)
    //   val negAttsLen: Int = props.get(posAttsLen + 1)

    //   val posAtts = new IntArrayList
    //   val negAtts = new IntArrayList

    //   var startIdx = 1
    //   var endIdx = posAttsLen
    //   for (i <- startIdx to endIdx) {
    //     try {
    //       posAtts.add(props.get(i))
    //     } catch {
    //       case x: ArrayIndexOutOfBoundsException => {
    //         val path = vis.getConfig.getMainGraph.asInstanceOf[ExceptionalMining].getName
    //         println(s"${path} : posAtts (${startIdx}; ${endIdx}) : ArrayIndexOutOfBoundsException")
    //         x.printStackTrace()
    //       }
    //     }
    //   }

    //   startIdx = endIdx + 2
    //   endIdx = startIdx + negAttsLen - 1
    //   for (i <- startIdx to endIdx) {
    //     try {
    //       negAtts.add(props.get(i))
    //     } catch {
    //       case x: ArrayIndexOutOfBoundsException => {
    //         val path = vis.getConfig.getMainGraph.asInstanceOf[ExceptionalMining].getName
    //         println(s"${path} : negAtts (${startIdx}; ${endIdx}) : ArrayIndexOutOfBoundsException")
    //         x.printStackTrace()
    //       }
    //     }
    //   }

    //   List(posAtts, negAtts)
    // }

    // //  [onpaper] A(S, K)
    // val aMeasure: (VertexInducedSubgraph, IntArrayList, IntArrayList) => Double = (vis, posAtts, negAtts) => {
    //   //    Calc gain
    //   val kIds = new IntArrayList
    //   for (v <- vis.getVertices.toIntArray) {
    //     kIds.add(vis.vertex(v).getVertexLabel)
    //   }

    //   val kAttsTot: Double = sumK(kIds)
    //   gain(kIds, posAtts, kAttsTot) - gain(kIds, negAtts, kAttsTot)
    // }

    // //  [onpaper] WRAcc(S,K)
    // val wracc = (vis: VertexInducedSubgraph, cvis: Computation[VertexInducedSubgraph]) => {
    //   //    [onpaper] |K| ≥ σ
    //   if (vis.getNumVertices > SIGMA) {
    //     val atts = props(vis)
    //     val posAtts = atts(0)
    //     val negAtts = atts(1)
    //     val wraccRes = aMeasure(vis, posAtts, negAtts) * (vis.getNumVertices / gVertsLen)
    //     //      [onpaper] WRAcc(S,K) ≥ δ
    //     wraccRes > DELTA
    //   } else {
    //     false
    //   }
    // }

    // //  RUN
    // val candidatesFiles = hdfsFileSystem.globStatus(new Path(s"${fractalDatasets}/*.graph"))
    // var subgraphs = new ListBuffer[RDD[ResultSubgraph[_]]]

    // val startTime = System.currentTimeMillis

    // val expanded: (Int, FractalGraph) => Fractoid[VertexInducedSubgraph] = (k, fGraph) => {
    //   var frac = fGraph.vfractoid.set("input_graph_class", graphClass)
    //   for (_ <- 1 to k) {
    //     frac = frac.expand(1)
    //   }
    //   frac
    // }

    // var filePath: Path = null
    // var fileLines = 0
    // var fileGraph: FractalGraph = null
    // //
    // candidatesFiles.foreach {
    //   file =>
    //     filePath = file.getPath
    //     fileGraph = fc.textFile(filePath.toString)
    //     fileLines = Source.fromInputStream(hdfsFileSystem.open(filePath)).getLines.length
    //     for (k <- 1 to fileLines) {
    //       subgraphs += expanded(k, fileGraph).subgraphs // NOTE: explode erro quando tenta computar algo
    //       //              subgraphs += expanded(k, fileGraph).filter(wracc).subgraphs
    //     }
    // }

    // val stopTime = System.currentTimeMillis
    // val elapsedTime = stopTime - startTime
    // println(s"Elapsed time(s): ${elapsedTime / 1000.0}")

    // ENV CLEANING
    fc.stop()
    sc.stop()
  }
}

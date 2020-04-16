package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining
import br.ufmg.cs.systems.fractal.subgraph.{ResultSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.graph.Vertex
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source


object ExceptionalMiningApp extends Logging {
  def main(args: Array[String]): Unit = {
    // environment setup

    val masterUrl = "local[*]"
    val conf = new SparkConf().setMaster(masterUrl).setAppName("ExceptionalMiningApp")
    //    val conf = new SparkConf().setAppName("ExceptionalMiningApp")
    val sc = new SparkContext(conf)

    //    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://localhost:9000")

    val hdfsFileSystem = FileSystem.get(sc.hadoopConfiguration)

    val hdfsCore = "hdfs://compute1:9000"
    //    val hdfsCore = "hdfs://localhost:9000"
    val userFolder = s"${hdfsCore}/user/ceciliassis"
    val tmpFolder = s"${userFolder}/tmp/fractal"

    val fc = new FractalContext(sc)
    //    val fc = new FractalContext(sc, tmpDir = tmpFolder)

    //  ENERGETICS
    val SIGMA = 1
    val DELTA = 0.05

    //  [ENERGETICS] Graph init
    //    val fractalDatasets = s"${userFolder}/fractal/candidates"
    val fractalDatasets = "/home/ceciliaassis/jobs/ufmg/fractal-cecilia-bkp/data/exceptionalMining-v1/candidates"
    val graphClass = "br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining"

    val graph: ExceptionalMining = {
      val graphPath = s"${fractalDatasets}/maingraph/main.graph"
      val tmpGraph = fc.textFile(graphPath).vfractoid.set("input_graph_class", graphClass).expand(1)
      tmpGraph.compute()
      tmpGraph.config.getMainGraph.asInstanceOf[ExceptionalMining]
    }

    //
    //    val gVertsLen = graph.getNumberVertices
    //    val gVerts = graph.getVertices
    //
    //     val vIds: IntArrayList = {
    //       val ids = new IntArrayList()
    //       for (i <- 0 to gVertsLen - 1) {
    //         ids.add(gVerts(i).getVertexId)
    //       }
    //       ids
    //     }
    //
    //    // //  [ENERGETICS] Calculation functions
    //    // //  [onpaper] sum(K) or sum(V)
    //     val sumK: IntArrayList => Double = vs => {
    //       var vAttTot = 0.0
    //       vs.toIntArray.foreach(v => gVerts(v).getProperty.toIntArray.foreach(vAttTot += _))
    //       vAttTot
    //     }
    //
    //    // //  [onpaper] sum(L,K) or sum(L,V)
    //     val sumLK: (IntArrayList, IntArrayList) => Double = (ls, vs) => {
    //       var attsTot = 0.0
    //       vs.toIntArray.foreach {
    //         v => ls.toIntArray.foreach(l => attsTot += gVerts(v).getProperty.get(l))
    //       }
    //       attsTot
    //     }
    //
    //     val graphIds: IntArrayList = vIds
    //     val graphAttsTotal: Double = sumK(graphIds)
    //
    //    // //  [onpaper] gain(L,K)
    //     val gain: (IntArrayList, IntArrayList, Double) => Double = (kIds, atts, kAttsTot) => {
    //       (sumLK(atts, kIds) / kAttsTot) - (sumLK(atts, graphIds) / graphAttsTotal)
    //     }
    //
    //     val props: VertexInducedSubgraph => List[IntArrayList] = vis => {
    //       val propsList: IntArrayList = vis.vertex(0).getProperty.asInstanceOf[IntArrayList]
    //
    //       val posAttsLen: Int = propsList.get(0)
    //       val negAttsLen: Int = propsList.get(posAttsLen + 1)
    //
    //       val posAtts = new IntArrayList
    //       val negAtts = new IntArrayList
    //
    //       var startIdx = 1
    //       var endIdx = posAttsLen
    //       for (i <- startIdx to endIdx) {
    //         try {
    //           posAtts.add(propsList.get(i))
    //         } catch {
    //           case x: ArrayIndexOutOfBoundsException => {
    //             val path = vis.getConfig.getMainGraph.asInstanceOf[ExceptionalMining].getName
    //             println(s"${path} : posAtts (${startIdx}; ${endIdx}) : ArrayIndexOutOfBoundsException")
    //             x.printStackTrace()
    //           }
    //         }
    //       }
    //
    //       startIdx = endIdx + 2
    //       endIdx = startIdx + negAttsLen - 1
    //       for (i <- startIdx to endIdx) {
    //         try {
    //           negAtts.add(propsList.get(i))
    //         } catch {
    //           case x: ArrayIndexOutOfBoundsException => {
    //             val path = vis.getConfig.getMainGraph.asInstanceOf[ExceptionalMining].getName
    //             println(s"${path} : negAtts (${startIdx}; ${endIdx}) : ArrayIndexOutOfBoundsException")
    //             x.printStackTrace()
    //           }
    //         }
    //       }
    //
    //       List(posAtts, negAtts)
    //     }
    //
    //    // //  [onpaper] A(S, K)
    //     val aMeasure: (VertexInducedSubgraph, IntArrayList, IntArrayList) => Double = (vis, posAtts, negAtts) => {
    //       //    Calc gain
    //       val kIds = new IntArrayList
    //       for (v <- vis.getVertices.toIntArray) {
    //         kIds.add(vis.vertex(v).getVertexLabel)
    //       }
    //
    //       val kAttsTot: Double = sumK(kIds)
    //       gain(kIds, posAtts, kAttsTot) - gain(kIds, negAtts, kAttsTot)
    //     }
    //
    //    // //  [onpaper] WRAcc(S,K)
    //     val wracc = (vis: VertexInducedSubgraph, cvis: Computation[VertexInducedSubgraph]) => {
    //       //    [onpaper] |K| ≥ σ
    //       if (vis.getNumVertices > SIGMA) {
    //         val atts = props(vis)
    //         val posAtts = atts(0)
    //         val negAtts = atts(1)
    //         val wraccRes = aMeasure(vis, posAtts, negAtts) * (vis.getNumVertices / gVertsLen)
    //         //      [onpaper] WRAcc(S,K) ≥ δ
    //         wraccRes > DELTA
    //       } else {
    //         false
    //       }
    //     }

    // //  RUN
    val candidatesFiles = hdfsFileSystem.globStatus(new Path(s"${fractalDatasets}/*.graph"))
    var subgraphs = new ListBuffer[Array[ResultSubgraph[_]]]
    var fileGraph: Fractoid[VertexInducedSubgraph] = null
    val graphPath = s"${fractalDatasets}/maingraph/main.graph"

    val startTime = System.currentTimeMillis

    val mainGraph = fc.textFile(graphPath, graphClass = graphClass).vfractoidAndExpand

    candidatesFiles.foreach {
      file =>
        val idsMap = mutable.Map[Int, Int]()
        for (line <- Source.fromInputStream(hdfsFileSystem.open(file.getPath)).getLines) {
          line.split(' ').foreach(id => idsMap.update(id.toInt, 1))
        }

        fileGraph = mainGraph.vfilter[IntArrayList](v => idsMap.contains(v.getVertexLabel))
        for (k <- 0 to idsMap.size - 1) {
          subgraphs += fileGraph.explore(k).subgraphs.collect // NOTE: explode erro quando tenta computar algo
          //              subgraphs += expanded(k, fileGraph).filter(wracc).subgraphs
        }
    }

    val stopTime = System.currentTimeMillis
    val elapsedTime = stopTime - startTime
    println(s"Elapsed time(s): ${elapsedTime / 1000.0}")

    // ENV CLEANING
    fc.stop()
    sc.stop()
  }
}

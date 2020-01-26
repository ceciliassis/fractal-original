package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining
import br.ufmg.cs.systems.fractal.subgraph.{ResultSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.{FractalGraph, _}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source


object ExceptionalMiningApp extends Logging {
  def main(args: Array[String]): Unit = {
    // environment setup
    //    val conf = new SparkConf().setMaster(s"local[*]").setAppName("ExceptionalMiningApp")
    val conf = new SparkConf().setAppName("ExceptionalMiningApp")
    val sc = new SparkContext(conf)
    val fc = new FractalContext(sc)

    val hdfsFileSystem = FileSystem.get(sc.hadoopConfiguration)

    val hdfsCore = "hdfs://compute1:9000" // NOTE: endereço hdfs

    val userFolder = "user/ceciliassis"

    //  ENERGETICS
    val SIGMA = 1
    val DELTA = 0.05

    //  [ENERGETICS] Graph init
    val fractalDatasets = s"${hdfsCore}/${userFolder}/fractal/data/exceptionalMining-v1/candidates"
    val graphClass = "br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining"

    val graph: ExceptionalMining = {
      val graphPath = s"${fractalDatasets}/maingraph/main.graph"
      val graph = fc.textFile(graphPath, graphClass).vfractoid.expand(1)
      val subgraphs = graph.subgraphs // NOTE: explode erro quando tenta computar algo
      graph.config.getMainGraph.asInstanceOf[ExceptionalMining]
    }

    val gVertsLen = graph.getNumberVertices
    val gVerts = graph.getVertices

    // ENV CLEANING
    fc.stop()
    sc.stop()
  }
}

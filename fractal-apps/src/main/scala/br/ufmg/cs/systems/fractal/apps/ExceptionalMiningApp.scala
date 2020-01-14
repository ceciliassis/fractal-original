package br.ufmg.cs.systems.fractal.apps

import java.io.File

import br.ufmg.cs.systems.fractal.{FractalGraph, _}
import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining
import br.ufmg.cs.systems.fractal.subgraph.{ResultSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import org.apache.commons.httpclient.URI
import org.apache.hadoop.conf.Configuration
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
    val hdfsFileSystem = FileSystem.get(new Configuration())


    //  ENERGETICS
    val SIGMA = 1
    val DELTA = 0.05

    //  [ENERGETICS] Graph init
    val fractalDatasets = "hdfs://compute1:9000/user/ceciliassis/fractal/data/exceptionalMining-v1"
    val graphClass = "br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining"




    // ENV CLEANING
    fc.stop()
    sc.stop()
  }
}

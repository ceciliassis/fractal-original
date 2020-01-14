package br.ufmg.cs.systems.fractal.apps

import java.io.File

import br.ufmg.cs.systems.fractal.{FractalGraph, _}
import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining
import br.ufmg.cs.systems.fractal.subgraph.{ResultSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
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
    //    val conf = new SparkConf().setMaster(s"local[*]").setAppName("ExceptionalMiningApp")
    val conf = new SparkConf().setAppName("ExceptionalMiningApp")
    val sc = new SparkContext(conf)
    val fc = new FractalContext(sc)

    val hdfsFileSystem = FileSystem.get(new Configuration())
    val hdfsCore = "hdfs://compute1:9000"

    val userFolder = "user/ceciliassis"

    //  ENERGETICS
    val SIGMA = 1
    val DELTA = 0.05

    //  [ENERGETICS] Graph init
    val fractalDatasets = s"${hdfsCore}/${userFolder}/fractal/data/exceptionalMining-v1"

    //  RUN
    val candidatesFiles = hdfsFileSystem.globStatus(new Path(s"${fractalDatasets}/candidates/*.graph"))

    candidatesFiles.foreach {
      file =>
        print(file)
    }

    // ENV CLEANING
    fc.stop()
    sc.stop()
  }
}

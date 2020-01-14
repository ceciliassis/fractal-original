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
  def main(args: Array[String]): Unit = {
    // environment setup
    //    val conf = new SparkConf().setMaster(s"local[*]").setAppName("ExceptionalMiningApp")
    val conf = new SparkConf().setAppName("ExceptionalMiningApp")
    val sc = new SparkContext(conf)
    val fc = new FractalContext(sc)

    //  ENERGETICS
    val SIGMA = 1
    val DELTA = 0.05

    //  [ENERGETICS] Graph init
    val fractalDatasets = "/user/ceciliassis/fractal/data/exceptionalMining-v1"
    val graphClass = "br.ufmg.cs.systems.fractal.gmlib.exceptionalmining.ExceptionalMining"


    val getFiles: String => Array[File] = (dirPath: String) => {
      new File(dirPath).listFiles().filter(_.getName.endsWith(".graph"))
    }

    val getFileLines: String => Int = filePath => {
      Source.fromFile(filePath).getLines().toList.length
    }

    //  RUN
    val files = getFiles(s"${fractalDatasets}/candidates")


    var fileLines = 0
    var fGraph: FractalGraph = null
    var filePath = ""


    files.foreach {
      file =>
        filePath = file.getPath
        fGraph = fc.textFile(filePath)
        fileLines = getFileLines(filePath)
        print(fileLines)
    }


    // ENV CLEANING
    fc.stop()
    sc.stop()
  }
}

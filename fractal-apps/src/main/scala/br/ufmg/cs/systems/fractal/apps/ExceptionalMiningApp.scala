package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}


object ExceptionalMiningApp extends Logging {
  def main(args: Array[String]): Unit = {
    // environment setup
    val conf = new SparkConf().setAppName("ExceptionalMiningApp")
    val sc = new SparkContext(conf)
    //    val fc = new FractalContext(sc)

    val hdfsFileSystem = FileSystem.get(sc.hadoopConfiguration)

    val hdfsCore = "hdfs://compute1:9000" // NOTE: endereço hdfs

    val userFolder = "user/ceciliassis"

    //  ENERGETICS
    val SIGMA = 1
    val DELTA = 0.05

    //  [ENERGETICS] Graph init
    val fractalDatasets = s"${hdfsCore}/${userFolder}/fractal/data/exceptionalMining-v1/candidates"

    val graphPath = s"${fractalDatasets}/maingraph/main.graph"

    val file = sc.textFile(graphPath)

    println(s"------------ file path ${graphPath} | file lines ${file.count()}")

    sc.stop()
  }
}

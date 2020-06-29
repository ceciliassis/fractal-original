package br.ufmg.cs.systems.fractal.mpmg

import java.io.{BufferedWriter, File, FileWriter}

import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.Logging
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.util.{Logging, PairWritable}
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder
import org.apache.hadoop.io.IntWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class HiveApp(val configPath: String) extends Logging {
  var sparkConfigs: ujson.Value = _
  var databaseConfigs: ujson.Value = _
  var algorithmConfigs: ujson.Value = _

  def initConfigs: Unit = {
    val config = ujson.read(scala.reflect.io.File(configPath).slurp)
    sparkConfigs = config("spark")
    databaseConfigs = config("database")
    algorithmConfigs = config("algorithm")
  }

  initConfigs

  def updateSparkSession: SparkSession = {
    var conf = new SparkConf()
    sparkConfigs.arr.foreach(setting => {
      conf = conf.set(setting("name").str, setting("value").str)
    })

    SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
  }

  /**
   * Read from hive/database write in disk
   */
  def readWriteInput(outputPath: String): Unit = {
    val sparkSession = updateSparkSession
    val hive = HiveWarehouseBuilder.session(sparkSession).build()

    databaseConfigs("temporary_tables").arr.foreach(table => {
      logInfo(s"\tLoading temporary table ${table("name").str} with query ${table("value").str}")
      hive.execute(table("value").str).createOrReplaceTempView(table("name").str)
    })

    logInfo(s"\tLoading edges with query: ${databaseConfigs("query").str}")
    val edges = hive.execute(databaseConfigs("query").str)

    //    todo: write using spark
    logInfo(s"\tWriting data to CSV at: ${outputPath}")
    val outputBuffer = new BufferedWriter(new FileWriter(new File(outputPath)))
    edges.collect.foreach(edge => {
      outputBuffer.write(s"${edge.get(0)} ${edge.get(1)}\n")
    })

    //    edges.write.csv(outputPath)

    outputBuffer.close()
    //    hive.close()
  }

  /**
   * Read from disk write in hive/database
   */
  def readWriteOutput(outputPath: String) {

  }

  //  todo: read from spark csv folder
}

trait MPMGApp extends Logging{
  def writeResults(outputPath: String): Unit
}

class CliquesApp (
                  val fractalGraph: FractalGraph,
                  algs: FractalAlgorithms,
                  explorationSteps: Int) extends FractalSparkApp with MPMGApp {
  var app: Fractoid[VertexInducedSubgraph] = _

  def execute: Unit = {
    val cliquesRes = algs.cliques(fractalGraph, (explorationSteps + 1)).
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      cliquesRes.compute()
    }

    logInfo(s"CliquesOptApp" +
      s" explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${cliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
    )

    app = cliquesRes
  }

  def writeResults(outputPath: String): Unit = {
    val outputBuffer = new BufferedWriter(new FileWriter(new File(outputPath)))
    outputBuffer.write("Identificador da clique,Identificador do vértice participante\n")

    var i = 1
    app.mappedSubgraphs.collect.foreach(subgraph => {
      for (vertex: String <- subgraph.mappedWords) {
        outputBuffer.write(s"${i},${vertex}\n")
      }
      i += 1 // todo: validate if is don't collide
    })

    outputBuffer.close()
  }
}

class ShortestPathsApp (
                        val fractalGraph: FractalGraph,
                        algs: FractalAlgorithms,
                        explorationSteps: Int) extends FractalSparkApp with MPMGApp {
  var app: Fractoid[EdgeInducedSubgraph] = _
  
  def execute: Unit = {
    val (pathsf, elapsed) = FractalSparkRunner.time {
      algs.spaths(fractalGraph, explorationSteps)
    }
    logInfo(s"ShortestPathsApp" +
      s" explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${pathsf.numValidSubgraphs()} elapsed=${elapsed}"
    )

    app = pathsf
  }

  override def writeResults(outputPath: String): Unit = {
    val outputBuffer = new BufferedWriter(new FileWriter(new File(outputPath)))
    outputBuffer.write("Identificador do caminho,Identificador do vértice participante, Vértice origem, Vértice destino\n")

    var i = 1
    app.aggregationMap[PairWritable[IntWritable, IntWritable], IntArrayList]("sps").foreach{ case (pair, path) =>
      { 
        //val map = c.getConfig().getMainGraph[MainGraph[_, _]]();
	val it = path.iterator
	while(it.hasNext() ) {
	//	val originalId = map.getVertex(vertex).getVertexOriginalId
        	outputBuffer.write(s"${i},${it.next()}\n")
	}
        i += 1 // todo: validate if is don't collide
     }
    }
    outputBuffer.close()
  }
}

object MPMGSparkRunner {
  def time[R](block: => R): (R, Long) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    (result, t1 - t0)
  }

  def main(args: Array[String]) {
    //args
    val hiveApp = new HiveApp(args(0))

    //Create Session
    val ss = hiveApp.updateSparkSession
    val fc = new FractalContext(ss.sparkContext)

    if (!ss.sparkContext.isLocal) {
      // TODO: this is ugly but have to make sure all spark executors are up by
      // the time we start executing fractal applications
      Thread.sleep(10000)
    }

    val outputPath = hiveApp.algorithmConfigs("output_path").str

    //query the input graph if is the case and write it in graphPath.
    val graphPath = s"${outputPath}.edges"
    hiveApp.readWriteInput(graphPath)

    //running fractal application
    val fractalGraph = fc.textFile(graphPath, "br.ufmg.cs.systems.fractal.graph.EdgeListGraph")
    val algs = new FractalAlgorithms

    val app = hiveApp.algorithmConfigs("app").str.toLowerCase match {
      case "cliques" =>
        new CliquesApp(fractalGraph, algs, hiveApp.algorithmConfigs("steps").num.toInt)
      case "spaths" =>
        new ShortestPathsApp(fractalGraph, algs, hiveApp.algorithmConfigs("steps").num.toInt)
      case appName =>
        throw new RuntimeException(s"Unknown app: ${appName}")
    }

    app.execute

    //write output results
    app.writeResults(outputPath)

    fc.stop()
    ss.stop()
  }
}

package br.ufmg.cs.systems.fractal

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.{Connection, DriverManager}

import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.{SparkConf, SparkContext}


trait FractalSparkApp extends Logging {
  def fractalGraph: FractalGraph

  def execute: Fractoid[_]
}

class VSubgraphsApp(val fractalGraph: FractalGraph,
                    commStrategy: String,
                    numPartitions: Int,
                    explorationSteps: Int) extends FractalSparkApp {
  def execute = {
    val vsubgraphsRes = fractalGraph.vfractoidAndExpand.
      set("comm_strategy", commStrategy).
      set("num_partitions", numPartitions).
      explore(explorationSteps)

    vsubgraphsRes.compute()

    vsubgraphsRes
  }
}

class MotifsApp(val fractalGraph: FractalGraph,
                commStrategy: String,
                numPartitions: Int,
                explorationSteps: Int) extends FractalSparkApp {
  def execute = {
    val motifsRes = fractalGraph.motifs.
      set("comm_strategy", commStrategy).
      set("num_partitions", numPartitions).
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      motifsRes.compute()
    }

    logInfo(s"MotifsApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${motifsRes.numValidSubgraphs()} elapsed=${elapsed}"
    )

    motifsRes
  }
}

class CliquesOptApp(val fractalGraph: FractalGraph,
                    commStrategy: String,
                    numPartitions: Int,
                    explorationSteps: Int) extends FractalSparkApp {
  def execute = {
    val cliquesRes = fractalGraph.cliquesKClist(explorationSteps + 1).
      set("comm_strategy", commStrategy).
      set("num_partitions", numPartitions).
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      cliquesRes.compute()
    }

    logInfo(s"CliquesOptApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${cliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
    )

    cliquesRes
  }
}

class CliquesApp(val fractalGraph: FractalGraph,
                 commStrategy: String,
                 numPartitions: Int,
                 explorationSteps: Int) extends FractalSparkApp {
  def execute = {
    val cliquesRes = fractalGraph.cliques.
      set("comm_strategy", commStrategy).
      set("num_partitions", numPartitions).
      set("fractal.optimizations", "br.ufmg.cs.systems.fractal.optimization.CliqueOptimization").
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      cliquesRes.compute()
    }

    logInfo(s"CliquesApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${cliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
    )

    cliquesRes
  }
}

class MaximalCliquesApp(val fractalGraph: FractalGraph,
                        commStrategy: String,
                        numPartitions: Int,
                        explorationSteps: Int) extends FractalSparkApp {
  def execute = {
    val maximalcliquesRes = fractalGraph.maximalcliques.
      set("comm_strategy", commStrategy).
      set("num_partitions", numPartitions).
      explore(explorationSteps)

    val (counting, elapsed) = FractalSparkRunner.time {
      maximalcliquesRes.compute()
    }

    logInfo(s"MaximalCliquesApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${maximalcliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
    )

    maximalcliquesRes
  }
}

class QuasiCliquesApp(val fractalGraph: FractalGraph,
                      commStrategy: String,
                      numPartitions: Int,
                      explorationSteps: Int,
                      minDensity: Double) extends FractalSparkApp {
  def execute = {
    val quasiCliquesRes = fractalGraph.quasiCliques(explorationSteps, minDensity).
      set("comm_strategy", commStrategy).
      set("num_partitions", numPartitions)

    val (counting, elapsed) = FractalSparkRunner.time {
      quasiCliquesRes.compute()
    }

    logInfo(s"QuasiCliquesApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${quasiCliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
    )

    quasiCliquesRes
  }
}

class FSMApp(val fractalGraph: FractalGraph,
             commStrategy: String,
             numPartitions: Int,
             explorationSteps: Int,
             support: Int) extends FractalSparkApp {
  def execute = {
    fractalGraph.set("comm_strategy", commStrategy)
    fractalGraph.set("num_partitions", numPartitions)

    val (fsm, elapsed) = FractalSparkRunner.time {
      fractalGraph.fsm(support, explorationSteps)
    }

    logInfo(s"FSMApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${fsm.numValidSubgraphs()} elapsed=${elapsed}"
    )

    fsm
  }
}

class KeywordSearchApp(val fractalGraph: FractalGraph,
                       commStrategy: String,
                       numPartitions: Int,
                       explorationSteps: Int,
                       queryWords: Array[String]) extends FractalSparkApp {
  def execute = {
    val (kws, elapsed) = FractalSparkRunner.time {
      fractalGraph.keywordSearch(numPartitions, queryWords)
    }

    logInfo(s"KeywordSearchApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${kws.numValidSubgraphs()} elapsed=${elapsed}"
    )

    kws
  }
}

class GQueryingApp(val fractalGraph: FractalGraph,
                   commStrategy: String,
                   numPartitions: Int,
                   explorationSteps: Int,
                   subgraphPath: String) extends FractalSparkApp {
  def execute = {

    val subgraph = new FractalGraph(
      subgraphPath, fractalGraph.fractalContext, "warn")

    val gquerying = fractalGraph.gquerying(subgraph).
      set("comm_strategy", commStrategy).
      set("num_partitions", numPartitions).
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      gquerying.compute()
    }

    println(s"GQueryingApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} subgraph=${subgraph}" +
      s" counting=${gquerying.numValidSubgraphs()} elapsed=${elapsed}"
    )

    gquerying
  }
}

class PathsApp(val fractalGraph: FractalGraph,
               commStrategy: String,
               numPartitions: Int,
               explorationSteps: Int) extends FractalSparkApp {
  def execute = {
    val pathsRes = fractalGraph.paths.
      set("comm_strategy", commStrategy).
      set("num_partitions", numPartitions).
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      pathsRes.compute()
    }

    logInfo(s"PathsApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${pathsRes.numValidSubgraphs()} elapsed=${elapsed}"
    )

    pathsRes
  }
}

object FractalSparkRunner extends Logging {
  def time[R](block: => R): (R, Long) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    (result, t1 - t0)
  }

  def main(args: Array[String]) {
    // args
    var i = 0
    val databaseConfigFile = args(i)
    i += 1
    val graphClass = args(i) match {
      case "al" =>
        "br.ufmg.cs.systems.fractal.graph.BasicMainGraph"
      case "el" =>
        "br.ufmg.cs.systems.fractal.graph.EdgeListGraph"
      case "al-kws" =>
        "br.ufmg.cs.systems.fractal.gmlib.keywordsearch.KeywordSearchGraph"
      case other =>
        throw new RuntimeException(s"Input graph format '${other}' is invalid")
    }
    i += 1
    val algorithm = args(i)
    i += 1
    val commStrategy = args(i)
    i += 1
    val numPartitions = args(i).toInt
    i += 1
    val explorationSteps = args(i).toInt
    i += 1
    val logLevel = args(i)

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val fc = new FractalContext(sc, logLevel)

    if (!sc.isLocal) {
      // TODO: this is ugly but have to make sure all spark executors are up by
      //  the time we start executing fractal applications
      Thread.sleep(10000)
    }

    //    TODO: ler json
    val databaseConfig = ujson.read(scala.reflect.io.File(databaseConfigFile).slurp)

    val url = databaseConfig("url").str
    val username = databaseConfig("username").str
    val password = databaseConfig("password").str
    val query = databaseConfig("query").str

    //        TODO: abrir conexão com o banco
    logInfo("Reading data from " + url)

    //    TODO: ler linhas do banco
    var connection: Connection = null
    val graphPath = s"${fc.tmpPath}/graph.edges"
    var outputBuffer: BufferedWriter = null

    try {
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(query)

      //      TODO: escrever linhas em arquivo no formato: ID ID
      val file = new File(graphPath)
      new File(fc.tmpPath).mkdirs()

      outputBuffer = new BufferedWriter(new FileWriter(file))
      while (resultSet.next) {
        outputBuffer.write(s"${resultSet.getString(1)} ${resultSet.getString(2)}\n")
      }
    } finally {
      outputBuffer.close()
      connection.close()
    }

    val fractalGraph = fc.textFile(graphPath, graphClass = graphClass)

    //    TODO: rodar  app
    val app = algorithm.toLowerCase match {
      case "vsubgraphs" =>
        new VSubgraphsApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "motifs" =>
        new MotifsApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "cliques" =>
        new CliquesApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "cliquesopt" =>
        new CliquesOptApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "maximalcliques" =>
        new MaximalCliquesApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "quasicliques" =>
        i += 1
        val minDensity = args(i).toDouble
        new QuasiCliquesApp(fractalGraph, commStrategy, numPartitions,
          explorationSteps, minDensity)
      case "fsm" =>
        i += 1
        val support = args(i).toInt
        new FSMApp(fractalGraph, commStrategy, numPartitions,
          explorationSteps, support)
      case "kws" =>
        i += 1
        val queryWords = args.slice(i, args.length)
        new KeywordSearchApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps, queryWords)
      case "gquerying" =>
        i += 1
        val subgraphPath = args(i)
        new GQueryingApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps, subgraphPath)
      case "paths" =>
        new PathsApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case appName =>
        throw new RuntimeException(s"Unknown app: ${appName}")
    }

    i += 1
    while (i < args.length) {
      println(s"Found config=${args(i)}")
      val kv = args(i).split(":")
      if (kv.length == 2) {
        fractalGraph.set(kv(0), kv(1))
      }
      i += 1
    }

    val appRes = app.execute

    val outputPath = s"${sys.env("HOME")}/fractal-outputs/${fc.tmpPath.split('/')(2)}.csv"
    outputBuffer = new BufferedWriter(new FileWriter(new File(outputPath)))

    //      TODO: without the collect, a not serializable
    //       error explodes due to ArrayBuffer mappedWords
    appRes.mappedSubgraphs.collect.foreach(subgraph => {
      outputBuffer.write(s"${subgraph.mappedWords.mkString(",")}\n")
    })

    outputBuffer.close()

    fc.stop()
    sc.stop()
  }
}

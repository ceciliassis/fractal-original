package br.ufmg.cs.systems.fractal

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

class BasicTestSuite extends FunSuite with BeforeAndAfterAll {
  private val numPartitions: Int = 2
  private val appName: String = "fractal-test"
  private val logLevel: String = "error"

  private var master: String = _
  private var sampleGraphPath: String = _
  private var sc: SparkContext = _
  private var fc: FractalContext = _
  private var fgraph: FractalGraph = _

  /** set up spark context */
  override def beforeAll: Unit = {
    master = s"local[${numPartitions}]"
    // spark conf and context
    val conf = new SparkConf().
      setMaster(master).
      setAppName(appName)

    sc = new SparkContext(conf)
    sc.setLogLevel(logLevel)
    fc = new FractalContext(sc, logLevel)

    sampleGraphPath = "data/cube.graph"
    fgraph = fc.textFile (sampleGraphPath)

  }

  /** stop spark context */
  override def afterAll: Unit = {
    if (sc != null) {
      sc.stop()
      fc.stop()
    }
  }

  test ("[cube,motifs]", Tag("cube.motifs")) {
    // Test output for motifs for subgraph with size 0 to 3

    // Expected output
    val numSubgraph = List(8, 12, 24)

    for (k <- 0 to (numSubgraph.size - 1)) {
      val motifsRes = fgraph.motifs.
        set ("num_partitions", numPartitions).
        explore(k)
      val subgraphs = motifsRes.subgraphs

      assert(subgraphs.count() == numSubgraph(k))
    }

  }

  test ("[cube,cliques]", Tag("cube.cliques")) {
    // Test output for clique for Subgraphs with size 1 to 3
    // Expected output
    val numSubgraph = List(8, 12, 0)

    for (k <- 0 to (numSubgraph.size - 1)) {
      val cliqueRes = fgraph.cliques.
        set ("num_partitions", numPartitions).
        explore(k)

      val Subgraphs = cliqueRes.subgraphs
      assert(Subgraphs.count == numSubgraph(k))
    }
  }


  test ("[cube,fsm]", Tag("cube.fsm")) {
    import br.ufmg.cs.systems.fractal.gmlib.fsm.DomainSupport
    import br.ufmg.cs.systems.fractal.pattern.Pattern

    // Critical test
    // Test output for fsm with support 2 for Subgraphs with size 2 to 3
    val support = 2

    // Expected output
    val numFreqPatterns = List(3, 3+4, 3+4+7)

    for (k <- 0 to (numFreqPatterns.size - 1)) {
      val fsmRes = fgraph.fsm(support, k).
        set ("num_partitions", numPartitions)

      val freqPatterns = fsmRes.
        aggregationMap [Pattern,DomainSupport] ("frequent_patterns")

      assert(freqPatterns.size == numFreqPatterns(k))
    }
  }

  test ("[cube,gquerying]", Tag("cube.gquerying")) {
    // Expected output
    val numSubgraph = List(8, 12, 0)
    
    val subgraph = new FractalGraph("data/triangle.graph", fgraph.fractalContext)

    val cliqueRes = fgraph.gquerying(subgraph).
      set ("num_partitions", numPartitions).
      explore(2)

    val subgraphs = cliqueRes.subgraphs
    assert(subgraphs.count == 0)
  }

  test ("[cube,vfilter]", Tag("cube.vfilter")) {
    val numSubgraph = List(3)
    for (k <- 0 to (numSubgraph.size - 1)) {
      val frac = fgraph.vfractoid.
        vfilter [String] (v => v.getVertexLabel() == 1).
        set ("num_partitions", numPartitions)
      val subgraphs = frac.subgraphs
      assert(subgraphs.count == numSubgraph(k))
    }
  }

  test ("[cube,efilter]", Tag("cube.efilter")) {
    val numSubgraph = List(2)
    for (k <- 0 to (numSubgraph.size - 1)) {
      val frac = fgraph.vfractoid.
        efilter [String] (e => e.getSourceId() == 1).
        expand.
        set ("num_partitions", numPartitions)
      val subgraphs = frac.subgraphs
      assert(subgraphs.count == numSubgraph(k))
    }
  }
}

package io.arabesque.conf

import io.arabesque.aggregation._
import io.arabesque.computation._
import io.arabesque.conf.Configuration._
import io.arabesque.embedding._
import io.arabesque.graph.{BasicMainGraph, MainGraph}
import io.arabesque.optimization.OptimizationSet
import io.arabesque.optimization.OptimizationSetDescriptor
import io.arabesque.pattern.Pattern
import io.arabesque.utils.{Logging, SerializableConfiguration}
import io.arabesque.utils.collection.AtomicBitSetArray

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.io.{ObjectInputStream, ObjectOutputStream}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkConf
import org.apache.spark.util.SizeEstimator

import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.io.Writable

import scala.collection.mutable.Map

/**
 * Configurations are passed along in this mapping
 */
case class SparkConfiguration[E <: Embedding](confs: Map[String,Any])
    extends Configuration[E] with Logging {

  // from scratch computation must have incremental aggregations
  fixAssignments
  if (SparkConfiguration.COMMS_FROM_SCRATCH.contains(getCommStrategy())) {
    logInfo (s"Switching aggregations to incremental")
    set("incremental_aggregation", true)
  }

  // master hostname
  if (!confs.contains(CONF_MASTER_HOSTNAME)) try {
    confs.update(CONF_MASTER_HOSTNAME,
      java.net.InetAddress.getLocalHost().getHostAddress())
  } catch {
    case e: java.net.UnknownHostException =>
      throw e
  }

  def this() {
    this (Map.empty)
  }

  /**
   * Sets a configuration (mutable)
   */
  def set(key: String, value: Any): SparkConfiguration[E] = {
    confs.update (key, value)
    fixAssignments
    this
  }

  /**
   * Sets a configuration (mutable) if this configuration has not been set yet
   */
  def setIfUnset(key: String, value: Any)
    : SparkConfiguration[E] = confs.get(key) match {
    case Some(_) =>
      this
    case None =>
      set (key, value)
  }

  /**
   * Sets a configuration (immutable)
   */
  def withNewConfig(key: String, value: Any): SparkConfiguration[E] = {
    withNewConfig(Map(key -> value))
  }
  
  /**
   * Sets a few configurations (immutable)
   */
  def withNewConfig(configMap: Map[String,Any]): SparkConfiguration[E] = {
    val newConfig = this.copy [E] (confs = confs ++ configMap)
    newConfig.fixAssignments
    newConfig.setMainGraph (getMainGraph())
    newConfig
  }

  /**
   * Unsets a configuration (immutable)
   */
  def withoutConfig(key: String): SparkConfiguration[E] = {
    withoutConfig(Set(key))
  }
  
  /**
   * Unsets a few configurations (immutable)
   */
  def withoutConfig(keys: Set[String]): SparkConfiguration[E] = {
    val newConfig = this.copy [E] (confs = confs -- keys)
    newConfig.fixAssignments
    newConfig.setMainGraph (getMainGraph())
    newConfig
  }

  /**
   * Sets a new computation in this configuration (immutable)
   */
  def withNewComputation (computation: Computation[E])
    : SparkConfiguration[E] = {
    withNewConfig (SparkConfiguration.COMPUTATION_CONTAINER, computation)
  }
  
  /**
   * Sets a new master computation in this configuration (immutable)
   */
  def withNewMasterComputation (masterComputation: MasterComputation)
    : SparkConfiguration[E] = {
    withNewConfig (SparkConfiguration.MASTER_COMPUTATION_CONTAINER,
      masterComputation)
  }

  /**
   * Sets a default hadoop configuration for this arabesque configuration. That
   * way both can be shipped together to the workers.
   * This function mutates the object.
   */
  def setHadoopConfig(conf: HadoopConfiguration): SparkConfiguration[E] = {
    val serHadoopConf = new SerializableConfiguration(conf)
    // we store the hadoop configuration as a common configuration
    this.confs.update (SparkConfiguration.HADOOP_CONF, serHadoopConf)
    this
  }

  /**
   * Returns a hadoop configuration assigned to this configuration, or throw an
   * exception otherwise.
   */
  def hadoopConf
    : HadoopConfiguration = confs.get(SparkConfiguration.HADOOP_CONF) match {
    case Some(serHadoopConf: SerializableConfiguration) =>
      serHadoopConf.value

    case Some(value) =>
      logError (s"The hadoop configuration type is invalid: ${value}")
      throw new RuntimeException(s"Invalid hadoop configuration type")

    case None =>
      logError ("The hadoop configuration type is not set")
      throw new RuntimeException(s"Hadoop configuration is not set")
  }

  /**
   * Translates Arabesque configuration into SparkConf.
   * ATENTION: This is highly spark-dependent
   */
  def sparkConf = {
    if (!isInitialized) {
      initialize()
    }
    val sparkMaster = getString ("spark_master", "local[*]")
    val conf = new SparkConf().
      setAppName ("Arabesque Master Execution Engine").
      setMaster (sparkMaster)
        
    conf.set ("spark.executor.memory", getString("worker_memory", "1g"))
    conf.set ("spark.driver.memory", getString("worker_memory", "1g"))

    sparkMaster match {
      case "yarn-client" | "yarn-cluster" | "yarn" =>
        conf.set ("spark.executor.instances",
          getInteger("num_workers", 1).toString)
        conf.set ("spark.executor.cores",
          getInteger("num_compute_threads", 1).toString)
        conf.set ("spark.driver.cores",
          getInteger("num_compute_threads", 1).toString)

      case standaloneUrl : String if standaloneUrl startsWith "spark://" =>
        conf.set ("spark.cores.max",
          (getInteger("num_workers", 1) * 
            getInteger("num_compute_threads", 1)).toString)

      case _ =>
    }
    logInfo (s"Spark configurations:\n${conf.getAll.mkString("\n")}")
    conf
  }

  /**
   * This function accounts for the computation instance that can be set in
   * this configuration. If this is the case we just return the computation,
   * otherwise we create an extended one by calling the method from super.
   */
  override def createComputation[E <: Embedding](): Computation[E] = {
    confs.get(SparkConfiguration.COMPUTATION_CONTAINER) match {
      case Some(cc: ComputationContainer[_]) =>
        // cc.shallowCopy().asInstanceOf[Computation[E]]
        val bytes = SparkConfiguration.serialize(cc)
        SparkConfiguration.deserialize[Computation[E]](bytes)

      case Some(c) =>
        throw new RuntimeException (s"Invalid computation type: ${c}")

      case None =>
        super.createComputation[E]()
    }
  }

  /**
   * Returns the computation container associated with this configuration, if
   * available. A computation container holds a custom computation that is
   * shipped to execution in the workers.
   */
  def computationContainer[E <: Embedding]: ComputationContainer[E] = {
    confs.get(SparkConfiguration.COMPUTATION_CONTAINER) match {
      case Some(cc: ComputationContainer[_]) =>
        cc.asInstanceOf[ComputationContainer[E]]
      case Some(cc) =>
        throw new RuntimeException (s"Computation ${cc} is not a container")
      case None =>
        throw new RuntimeException (s"No computation is set")
    }
  }

  /**
   * Auxiliary function that returns an optional computation container
   */
  def computationContainerOpt[E <: Embedding]
    : Option[ComputationContainer[E]] = {
    confs.get(SparkConfiguration.COMPUTATION_CONTAINER).
      asInstanceOf[Option[ComputationContainer[E]]]
  }

  /**
   * Clears this configuration's container, if it exists
   */
  def clearComputationContainer: Boolean = {
    confs.get(SparkConfiguration.COMPUTATION_CONTAINER) match {
      case Some(cc: ComputationContainer[_]) =>
        set (SparkConfiguration.COMPUTATION_CONTAINER, cc.clear())
        true
      case _ =>
        false
    }
  }

  /**
   * This function accounts for the master computation instance that can be set
   * in this configuration. If this is the case we just return the computation,
   * otherwise we create an extended one by calling the method from super.
   */
  override def createMasterComputation(): MasterComputation = {
    confs.get(SparkConfiguration.MASTER_COMPUTATION_CONTAINER) match {
      case Some(cc: MasterComputationContainer) =>
        cc.shallowCopy().asInstanceOf[MasterComputation]
      case Some(c) =>
        throw new RuntimeException (s"Invalid master computation type: ${c}")
      case None =>
        super.createMasterComputation()
    }
  }

  /**
   * Returns the master computation container associated with this
   * configuration, if available. A master computation container holds a custom
   * computation that is shipped to execution in the workers.
   */
  def masterComputationContainer: MasterComputationContainer = {
    confs.get(SparkConfiguration.MASTER_COMPUTATION_CONTAINER) match {
      case Some(cc: MasterComputationContainer) =>
        cc
      case _ =>
        new MasterComputationContainer()
    }
  }

  /**
   * We assume the number of requested executor cores as an alternative number
   * of partitions. However, by the time we call this function, the config
   * *num_partitions* should be already set by the user, or by the execution
   * master engine which has SparkContext.defaultParallelism as default
   */
  def numPartitions: Int = getInteger("num_partitions",
    getInteger("num_workers", 1) *
      getInteger("num_compute_threads", Runtime.getRuntime.availableProcessors))

  /**
   * Given the total number of partitions in the cluster, this function returns
   * roughly the number of partitions per worker. We assume an uniform division
   * among workers.
   */
  def numPartitionsPerWorker: Int = numPartitions / getInteger("num_workers", 1)

  /**
   * Update assign internal names to user defined properties
   */
  private def fixAssignments = {
    def updateIfExists(key: String, config: String) = confs.remove (key) match {
      case Some(value) => confs.update (config, value)
      case None =>
    }

    // log level
    updateIfExists ("log_level", CONF_LOG_LEVEL)

    // info period
    updateIfExists ("info_period", INFO_PERIOD)
    
    // computation classes
    updateIfExists ("master_computation", CONF_MASTER_COMPUTATION_CLASS)
    updateIfExists ("computation", CONF_COMPUTATION_CLASS)

    // communication strategy
    updateIfExists ("comm_strategy", CONF_COMM_STRATEGY)

    // odag flush method
    updateIfExists ("flush_method", CONF_ODAG_FLUSH_METHOD)
    updateIfExists ("num_odag_parts", CONF_EZIP_AGGREGATORS)

    // gtag
    updateIfExists ("gtag_batch_low", CONF_GTAG_BATCH_SIZE_LOW)
    updateIfExists ("gtag_batch_high", CONF_GTAG_BATCH_SIZE_HIGH)

    // work stealing
    updateIfExists ("ws_internal", CONF_WS_INTERNAL)
    updateIfExists ("ws_external", CONF_WS_EXTERNAL)

    // input
    updateIfExists ("input_graph_class", CONF_MAINGRAPH_CLASS)
    updateIfExists ("input_graph_path", CONF_MAINGRAPH_PATH)
    updateIfExists ("input_graph_local", CONF_MAINGRAPH_LOCAL)
    updateIfExists ("edge_labelled", CONF_MAINGRAPH_EDGE_LABELLED)

    // embedding
    updateIfExists ("keep_maximal", CONF_EMBEDDING_KEEP_MAXIMAL)
 
    // output
    updateIfExists ("output_active", CONF_OUTPUT_ACTIVE)
    updateIfExists ("output_path", CONF_OUTPUT_PATH)
    updateIfExists ("output_format", CONF_OUTPUT_FORMAT)

    // aggregation
    updateIfExists ("incremental_aggregation", CONF_INCREMENTAL_AGGREGATION)
   
    // max number of odags in case of odag communication strategy
    updateIfExists ("max_odags", CONF_COMM_STRATEGY_ODAGMP_MAX)

  }

  var tagApplied = false

  def initializeWithTag(
      vtag: AtomicBitSetArray, etag: AtomicBitSetArray): Unit = synchronized {
    initialize()
    if (!tagApplied) {
      val start = System.currentTimeMillis
      val ret = getMainGraph[BasicMainGraph[_,_]].applyTag(vtag, etag)
      System.gc()
      val elapsed = System.currentTimeMillis - start
      logInfo (s"GraphTagging took ${elapsed} ms. Return: ${ret}")
      tagApplied = true
    }
  }

  def initializeWithTag(tag: AtomicBitSetArray): Unit = synchronized {
    initialize()
    if (!tagApplied) {
      val start = System.currentTimeMillis
      val ec = createComputation.getEmbeddingClass()
      val ret = if (ec == classOf[VertexInducedEmbedding]) {
        getMainGraph[BasicMainGraph[_,_]].applyTagVertexes(tag)
      } else if (ec == classOf[EdgeInducedEmbedding]) {
        getMainGraph[BasicMainGraph[_,_]].applyTagEdges(tag)
      } else {
        throw new RuntimeException(s"Unknown embedding type: ${ec}")
      }
      val elapsed = System.currentTimeMillis - start
      logInfo (s"GraphTagging took ${elapsed} ms. Return: ${ret}")
      tagApplied = true
    }
  }

  def initializeWithTag(): Unit = synchronized {
    initialize()
    if (!tagApplied) {
      val start = System.currentTimeMillis
      val ret = getMainGraph[BasicMainGraph[_,_]].applyTag()
      val elapsed = System.currentTimeMillis - start
      logInfo (s"GraphTagging took ${elapsed} ms. Return: ${ret}")
      tagApplied = true
    }
  }

  def uninitialize(): Unit = {
    tagApplied = false
  }

  /**
   * Garantees that arabesque configuration is properly set
   *
   * TODO: generalize the initialization in the superclass Configuration
   */
  override def initialize(isMaster: Boolean = false): Unit = synchronized {
    if (Configuration.isUnset(id)) {
      initializeInstance(!isMaster)
    } else if (!isInitialized) {
      initializeInstance(!isMaster)
    }
    
    if (getMainGraph == null || !isMainGraphRead()) {
      logInfo(s"Creating graph configId=${id} mainGraphId=${mainGraphId}")
      setMainGraph(createGraph())
    }

    if (!isMaster && !isMainGraphRead()) {
      logInfo(s"Reading graph configId=${id} mainGraphId=${mainGraphId}")
      setGraph()
      val optimizationSet = getOptimizationSet()
      logInfo (s"Active optimizations (applyAfterGraphLoad): ${optimizationSet}")
      optimizationSet.applyAfterGraphLoad()
    }

    Configuration.add(this)
  }

  /**
   * Called whether no arabesque configuration is set in the running jvm
   */
  private def initializeInstance(shouldSetGraph: Boolean = true): Unit = {

    fixAssignments
    
    // periodic information about execution
    infoPeriod = getLong(INFO_PERIOD, INFO_PERIOD_DEFAULT)

    // common configs
    setMainGraphClass (
      getClass (CONF_MAINGRAPH_CLASS, CONF_MAINGRAPH_CLASS_DEFAULT).
      asInstanceOf[Class[_ <: MainGraph[_,_]]]
    )

    setIsGraphEdgeLabelled (getBoolean (CONF_MAINGRAPH_EDGE_LABELLED,
      CONF_MAINGRAPH_EDGE_LABELLED_DEFAULT))

    setMasterComputationClass (
      getClass (CONF_MASTER_COMPUTATION_CLASS,
        CONF_MASTER_COMPUTATION_CLASS_DEFAULT).
      asInstanceOf[Class[_ <: MasterComputation]]
    )
    
    setComputationClass (
      getClass (CONF_COMPUTATION_CLASS, CONF_COMPUTATION_CLASS_DEFAULT).
      asInstanceOf[Class[_ <: Computation[E]]]
    )

    setPatternClass (
      getClass (CONF_PATTERN_CLASS, CONF_PATTERN_CLASS_DEFAULT).
      asInstanceOf[Class[_ <: Pattern]]
    )

    // optimizations
    setOptimizationSetDescriptorClass (
      getClass (CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS,
        CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS_DEFAULT).
      asInstanceOf[Class[_ <: OptimizationSetDescriptor]]
    )

    val optimizationSet = getOptimizationSet()
    logInfo (s"Active optimizations (applyStartup): ${optimizationSet}")
    optimizationSet.applyStartup()

    setAggregationsMetadata (new java.util.HashMap())

    setOutputPath (getString(CONF_OUTPUT_PATH, CONF_OUTPUT_PATH_DEFAULT))
    
    initialized = true
  }

  private def setGraph(): Boolean = {
    var graphRead = false

    // in case of the mainGraph is empty (no vertices and no edges), we try to
    // read it
    getMainGraph[BasicMainGraph[_,_]].synchronized {
      if (!isMainGraphRead) {
        logInfo ("MainGraph is empty, gonna try reading it")
        readMainGraph()
        graphRead = true
      }
    }

    graphRead
  }

  /** */
  @transient lazy val aggStorageFactory = new AggregationStorageFactory(this)

  @transient lazy val finalAggStorages = Map.empty[String,
    (AtomicInteger, AggregationStorage[_ <: Writable, _ <: Writable])]

  def getOrCreateFinalAggStorage(name: String) = synchronized {
    finalAggStorages.get(name) match {
      case Some((_, finalAggStorage)) =>
        finalAggStorage

      case None =>
        val finalAggStorage = aggStorageFactory.createAggregationStorage(name)
        finalAggStorages.update (name,
          (new AtomicInteger(taskCounter()), finalAggStorage))
        finalAggStorage
    }
  }

  def maybeReclaimFinalAggStorage(step: Int, name: String) = synchronized {
    finalAggStorages.get(name) match {
      case Some((barrier, finalAggStorage)) if barrier.get == 1 =>
        barrier.decrementAndGet
        finalAggStorages.remove(name)
        
        logInfo(s"FinalLocalAggregationStorage name=${name}" +
          s" step=${step}" +
          s" taskCounter=${taskCounter()}" +
          s" barrier=${barrier}" +
          s" finalAggStorage=${finalAggStorage}")

        finalAggStorage.synchronized {
          finalAggStorage.notifyAll()
        }

        (finalAggStorage, barrier)

      case Some((barrier, finalAggStorage)) =>
        assert (barrier.get > 1,
          s"taskCounter=${taskCounter()} barrier=${barrier}")
        barrier.decrementAndGet
        (finalAggStorage, barrier)

      case None =>
        throw new RuntimeException(s"Trying to reclaim without registering")
    }
  }

  /** */

  def getValue(key: String, defaultValue: Any): Any = confs.get(key) match {
    case Some(value) => value
    case None => defaultValue
  }

  override def getInteger(key: String, defaultValue: Integer) =
    getValue(key, defaultValue).asInstanceOf[Int]
  
  override def getLong(key: String, defaultValue: java.lang.Long) =
    getValue(key, defaultValue).asInstanceOf[Long]

  override def getString(key: String, defaultValue: String) =
    getValue(key, defaultValue).asInstanceOf[String]
  
  override def getBoolean(key: String, defaultValue: java.lang.Boolean) = {
    val value = getValue(key, defaultValue)
    if (value.isInstanceOf[java.lang.Boolean]) {
      value.asInstanceOf[java.lang.Boolean]
    } else if (value.isInstanceOf[String]) {
      new java.lang.Boolean(value.asInstanceOf[String])
    } else {
      throw new RuntimeException(s"Invalid boolean for (${key}, ${value})")
    }
  }
}

object SparkConfiguration extends Logging {
  /** odag flush methods */

  // good for regular distributions
  val FLUSH_BY_PATTERN = "flush_by_pattern"
  // good for irregular distributions but small embedding domains
  val FLUSH_BY_ENTRIES = "flush_by_entries"
  // good for irregular distributions, period
  val FLUSH_BY_PARTS = "flush_by_parts"

  /** communication strategies */

  // pack embeddings with single-pattern odags
  val COMM_ODAG_SP = "odag_sp"
  // pack embeddings with multi-pattern odags
  val COMM_ODAG_MP = "odag_mp"
  // pack embeddings with compressed caches (e.g., LZ4)
  val COMM_EMBEDDING = "embedding"
  // re-enumerates from scratch every superstep
  val COMM_FROM_SCRATCH = "scratch"
  // re-enumerates from scratch every superstep (includes graph tags)
  val COMM_GTAG = "gtag"
  // re-enumerates from scratch every superstep (hierarchical)
  val COMM_GTAG_HIER = "gtag_hier"
  // re-enumerates from scratch every step (includes boolean tags)
  val COMM_BTAG = "btag"
  // re-enumerates from scratch every step (includes boolean tags for vertices
  // and edges)
  val COMM_VETAG = "vetag"
  // re-enumerates from scratch every step (includes word edges tags)
  val COMM_VIEWTAG = "viewtag"
  // re-enumerates from scratch (used for characterization purposes)
  val COMM_CHARAC = "charac"

  val COMMS_FROM_SCRATCH = Set(COMM_FROM_SCRATCH, COMM_GTAG,
    COMM_GTAG_HIER, COMM_BTAG, COMM_VETAG, COMM_VIEWTAG, COMM_CHARAC)

  // gtag
  val GTAG_BATCH_LOW = "gtag_batch_low"
  val GTAG_BATCH_HIGH = "gtag_batch_high"

  // hadoop conf
  val HADOOP_CONF = "hadoop_conf"

  // computation container
  val COMPUTATION_CONTAINER = "computation_container"
  val MASTER_COMPUTATION_CONTAINER = "master_computation_container"

  // output format
  val OUTPUT_PLAIN_TEXT = "plain_text"
  val OUTPUT_SEQUENCE_FILE = "sequence_file"

  // auxiliary functions
  def serialize[T](obj: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close
    baos.toByteArray
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    ois.readObject().asInstanceOf[T]
  }
}

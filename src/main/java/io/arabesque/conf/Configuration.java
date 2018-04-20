package io.arabesque.conf;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.aggregation.AggregationStorageMetadata;
import io.arabesque.aggregation.EndAggregationFunction;
import io.arabesque.aggregation.reductions.ReductionFunction;
import io.arabesque.computation.Computation;
import io.arabesque.computation.ExecutionEngine;
import io.arabesque.computation.MasterComputation;
import io.arabesque.computation.WorkerContext;
import io.arabesque.computation.comm.CommunicationStrategy;
import io.arabesque.computation.comm.CommunicationStrategyFactory;
import io.arabesque.embedding.Embedding;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.embedding.EdgeInducedEmbedding;
import io.arabesque.graph.MainGraph;
import io.arabesque.optimization.OptimizationSet;
import io.arabesque.optimization.OptimizationSetDescriptor;
import io.arabesque.pattern.Pattern;
import io.arabesque.pattern.VICPattern;
import io.arabesque.utils.pool.Pool;
import io.arabesque.utils.pool.PoolRegistry;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class Configuration<O extends Embedding> implements Serializable {
    
    // we keep a local (per JVM) pool of configurations potentially
    // representing several active arabesque applications
    private static AtomicInteger nextConfId = new AtomicInteger(0);

    protected final int id = newConfId();

    protected AtomicInteger taskCounter = new AtomicInteger(0);

    private static final Logger LOG = Logger.getLogger(Configuration.class);
    public static final int KB = 1024;
    public static final int MB = 1024 * KB;
    public static final int GB = 1024 * MB;

    public static final int SECONDS = 1000;
    public static final int MINUTES = 60 * SECONDS;
    public static final int HOURS = 60 * MINUTES;

    public static final int K = 1000;
    public static final int M = 1000 * K;
    public static final int B = 1000 * M;

    public static final String CONF_LOG_LEVEL = "arabesque.log.level";
    public static final String CONF_LOG_LEVEL_DEFAULT = "info";
    public static final String CONF_MAINGRAPH_CLASS = "arabesque.graph.class";
    public static final String CONF_MAINGRAPH_CLASS_DEFAULT = "io.arabesque.graph.BasicMainGraph";
    public static final String CONF_MAINGRAPH_PATH = "arabesque.graph.location";
    public static final String CONF_MAINGRAPH_PATH_DEFAULT = "main.graph";
    public static final String CONF_MAINGRAPH_LOCAL = "arabesque.graph.local";
    public static final boolean CONF_MAINGRAPH_LOCAL_DEFAULT = false;
    public static final String CONF_MAINGRAPH_EDGE_LABELLED = "arabesque.graph.edge_labelled";
    public static final boolean CONF_MAINGRAPH_EDGE_LABELLED_DEFAULT = false;
    public static final String CONF_MAINGRAPH_MULTIGRAPH = "arabesque.graph.multigraph";
    public static final boolean CONF_MAINGRAPH_MULTIGRAPH_DEFAULT = false;

    public static final String CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS = "arabesque.optimizations.descriptor";
    public static final String CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS_DEFAULT = "io.arabesque.optimization.ConfigBasedOptimizationSetDescriptor";

    private static final String CONF_COMPRESSED_CACHES = "arabesque.caches.compress";
    private static final boolean CONF_COMPRESSED_CACHES_DEFAULT = false;
    private static final String CONF_CACHE_THRESHOLD_SIZE = "arabesque.cache.threshold";
    private static final int CONF_CACHE_THRESHOLD_SIZE_DEFAULT = 1 * MB;

    public static final String CONF_OUTPUT_ACTIVE = "arabesque.output.active";
    public static final boolean CONF_OUTPUT_ACTIVE_DEFAULT = true;
    public static final String CONF_OUTPUT_FORMAT = "arabesque.output.format";
    public static final String CONF_OUTPUT_FORMAT_DEFAULT = "plain_text";

    public static final String INFO_PERIOD = "arabesque.info.period";
    public static final long INFO_PERIOD_DEFAULT = 60000;

    public static final String CONF_COMPUTATION_CLASS = "arabesque.computation.class";
    public static final String CONF_COMPUTATION_CLASS_DEFAULT = "io.arabesque.computation.ComputationContainer";

    public static final String CONF_MASTER_COMPUTATION_CLASS = "arabesque.master_computation.class";
    public static final String CONF_MASTER_COMPUTATION_CLASS_DEFAULT = "io.arabesque.computation.MasterComputation";

    public static final String CONF_COMM_STRATEGY = "arabesque.comm.strategy";
    public static final String CONF_COMM_STRATEGY_DEFAULT = "scratch";

    public static final String CONF_COMM_STRATEGY_ODAGMP_MAX = "arabesque.comm.strategy.odagmp.max";
    public static final int CONF_COMM_STRATEGY_ODAGMP_MAX_DEFAULT = 100;

    public static final String CONF_COMM_STRATEGY_FACTORY_CLASS = "arabesque.comm.factory.class";
    public static final String CONF_COMM_STRATEGY_FACTORY_CLASS_DEFAULT = "io.arabesque.computation.comm.ODAGCommunicationStrategyFactory";

    public static final String CONF_PATTERN_CLASS = "arabesque.pattern.class";
    public static final String CONF_PATTERN_CLASS_DEFAULT = "io.arabesque.pattern.JBlissPattern";

    public static final String CONF_EMBEDDING_KEEP_MAXIMAL = "arabesque.embedding.keep.maximal";
    public static final boolean CONF_EMBEDDING_KEEP_MAXIMAL_DEFAULT = false;

    // TODO: maybe we should the name of this configuration in the future, use
    // odag instead of ezip ?
    public static final String CONF_EZIP_AGGREGATORS = "arabesque.odag.aggregators";
    public static final int CONF_EZIP_AGGREGATORS_DEFAULT = -1;

    public static final String CONF_ODAG_FLUSH_METHOD = "arabesque.odag.flush.method";
    public static final String CONF_ODAG_FLUSH_METHOD_DEFAULT = "flush_by_parts";

    private static final String CONF_2LEVELAGG_ENABLED = "arabesque.2levelagg.enabled";
    private static final boolean CONF_2LEVELAGG_ENABLED_DEFAULT = true;
    private static final String CONF_FORCE_GC = "arabesque.forcegc";
    private static final boolean CONF_FORCE_GC_DEFAULT = false;

    public static final String CONF_OUTPUT_PATH = "arabesque.output.path";
    public static final String CONF_OUTPUT_PATH_DEFAULT = "Output";

    public static final String CONF_DEFAULT_AGGREGATOR_SPLITS = "arabesque.aggregators.default_splits";
    public static final int CONF_DEFAULT_AGGREGATOR_SPLITS_DEFAULT = 1;

    public static final String CONF_INCREMENTAL_AGGREGATION = "arabesque.aggregation.incremental";
    public static final boolean CONF_INCREMENTAL_AGGREGATION_DEFAULT = false;

    public static final String CONF_AGGREGATION_STORAGE_CLASS = "arabesque.aggregation.storage.class";
    public static final String CONF_AGGREGATION_STORAGE_CLASS_DEFAULT = "io.arabesque.aggregation.AggregationStorage";

    public static final String CONF_MASTER_HOSTNAME = "arabesque.master.hostname";
    public static final String CONF_MASTER_HOSTNAME_DEFAULT = "localhost";

    // gtag
    public static final String CONF_GTAG_BATCH_SIZE_LOW = "arabesque.gtag.batch.size.low";
    public static final int CONF_GTAG_BATCH_SIZE_LOW_DEFAULT = 1;

    public static final String CONF_GTAG_BATCH_SIZE_HIGH = "arabesque.gtag.batch.size.high";
    public static final int CONF_GTAG_BATCH_SIZE_HIGH_DEFAULT = 10;
    
    public static final String CONF_WS_INTERNAL = "arabesque.ws.mode.internal";
    public static final boolean CONF_WS_MODE_INTERNAL_DEFAULT = true;
    public static final String CONF_WS_EXTERNAL = "arabesque.ws.mode.external";
    public static final boolean CONF_WS_MODE_EXTERNAL_DEFAULT = true;

    private String masterHostname;
    protected static Configuration instance = null;
    protected static ConcurrentHashMap<Integer,Configuration> activeConfigs =
       new ConcurrentHashMap<Integer,Configuration>();
    private ImmutableClassesGiraphConfiguration giraphConfiguration;

    private boolean useCompressedCaches;
    private int cacheThresholdSize;
    protected long infoPeriod;
    private int odagNumAggregators;
    private boolean is2LevelAggregationEnabled;
    private boolean forceGC;
    private CommunicationStrategyFactory communicationStrategyFactory;

    private Class<? extends MainGraph> mainGraphClass;
    private Class<? extends OptimizationSetDescriptor>
       optimizationSetDescriptorClass;
    private Class<? extends Pattern> patternClass;
    private Class<? extends Computation> computationClass;
    private Class<? extends AggregationStorage> aggregationStorageClass;
    private Class<? extends MasterComputation> masterComputationClass;
    private Class<? extends Embedding> embeddingClass;

    private String outputPath;
    private int defaultAggregatorSplits;

    private transient Map<String, AggregationStorageMetadata>
       aggregationsMetadata;
    protected transient MainGraph mainGraph;
    protected int mainGraphId = -1;
    private boolean isGraphEdgeLabelled;
    protected transient boolean initialized = false;
    private boolean isGraphMulti;

    private static int newConfId() {
       return nextConfId.getAndIncrement();
    }

    public int getId() {
       return id;
    }

    public static boolean isUnset() {
       return instance == null;
    }

    public static <C extends Configuration> C get() {
        if (instance == null) {
           LOG.error ("instance is null");
            throw new RuntimeException("Oh-oh, Null configuration");
        }

        if (instance instanceof SparkConfiguration) {
           LOG.error ("static getter not allowed in Spark mode");
            throw new RuntimeException(
                  "Static getter is not allowed in Spark mode");
        }

        if (!instance.isInitialized()) {
           instance.initialize();
        }

        return (C) instance;
    }

    public static void unset() {
       instance = null;
    }

    public synchronized static void setIfUnset(Configuration configuration) {
        if (isUnset()) {
            set(configuration);
        }
    }

    public static void set(Configuration configuration) {
        instance = configuration;

        // Whenever we set configuration, reset all known pools Since they might
        // have initialized things based on a previous configuration NOTE: This
        // is essential for the unit tests
        for (Pool pool : PoolRegistry.instance().getPools()) {
            pool.reset();
        }
    }

    public static void add(Configuration configuration) {
       activeConfigs.put(configuration.getId(), configuration);
    }

    public static void remove(int id) {
       activeConfigs.remove(id);
    }

    public static Configuration get(int id) {
       return activeConfigs.get(id);
    }

    public static boolean isUnset(int id) {
       return !activeConfigs.containsKey(id);
    }

    public int taskCheckIn() {
       return taskCounter.incrementAndGet();
    }
    
    public boolean taskCheckIn(int expect, int ntasks) {
       return taskCounter.compareAndSet(expect, ntasks);
    }

    public int taskCheckOut() {
       return taskCounter.decrementAndGet();
    }

    public int taskCounter() {
       return taskCounter.get();
    }

    public Configuration(
          ImmutableClassesGiraphConfiguration giraphConfiguration) {
        this.giraphConfiguration = giraphConfiguration;
    }

    public Configuration() {}

    public void initialize() {
       initialize(false);
    }
    
    public void initialize(boolean isMaster) {
        if (initialized) {
            return;
        }

        LOG.info("Initializing Configuration...");

        useCompressedCaches = getBoolean(CONF_COMPRESSED_CACHES,
              CONF_COMPRESSED_CACHES_DEFAULT);

        cacheThresholdSize = getInteger(CONF_CACHE_THRESHOLD_SIZE,
              CONF_CACHE_THRESHOLD_SIZE_DEFAULT);

        infoPeriod = getLong(INFO_PERIOD, INFO_PERIOD_DEFAULT);

        Class<? extends CommunicationStrategyFactory> 
           communicationStrategyFactoryClass =
                (Class<? extends CommunicationStrategyFactory>) getClass(
                      CONF_COMM_STRATEGY_FACTORY_CLASS,
                      CONF_COMM_STRATEGY_FACTORY_CLASS_DEFAULT);

        communicationStrategyFactory = ReflectionUtils.newInstance(
              communicationStrategyFactoryClass);

        odagNumAggregators = getInteger(CONF_EZIP_AGGREGATORS,
              CONF_EZIP_AGGREGATORS_DEFAULT);

        is2LevelAggregationEnabled = getBoolean(CONF_2LEVELAGG_ENABLED,
              CONF_2LEVELAGG_ENABLED_DEFAULT);

        forceGC = getBoolean(CONF_FORCE_GC, CONF_FORCE_GC_DEFAULT);

        mainGraphClass = (Class<? extends MainGraph>) getClass(
              CONF_MAINGRAPH_CLASS, CONF_MAINGRAPH_CLASS_DEFAULT);

        isGraphEdgeLabelled = getBoolean(CONF_MAINGRAPH_EDGE_LABELLED,
              CONF_MAINGRAPH_EDGE_LABELLED_DEFAULT);

        isGraphMulti = getBoolean(CONF_MAINGRAPH_MULTIGRAPH,
              CONF_MAINGRAPH_MULTIGRAPH_DEFAULT);

        optimizationSetDescriptorClass =
           (Class<? extends OptimizationSetDescriptor>) getClass(
                 CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS,
                 CONF_OPTIMIZATIONSETDESCRIPTOR_CLASS_DEFAULT);

        patternClass = (Class<? extends Pattern>) getClass(CONF_PATTERN_CLASS,
              CONF_PATTERN_CLASS_DEFAULT);

        // create (empty) graph
        setMainGraph(createGraph());

        // TODO: Make this more flexible
        if (isGraphEdgeLabelled || isGraphMulti) {
            patternClass = VICPattern.class;
        }

        computationClass = (Class<? extends Computation>)
           getClass(CONF_COMPUTATION_CLASS, CONF_COMPUTATION_CLASS_DEFAULT);
        masterComputationClass = (Class<? extends MasterComputation>)
           getClass(CONF_MASTER_COMPUTATION_CLASS,
                 CONF_MASTER_COMPUTATION_CLASS_DEFAULT);

        aggregationsMetadata = new HashMap<>();

        outputPath = getString(CONF_OUTPUT_PATH, CONF_OUTPUT_PATH_DEFAULT + "_"
              + computationClass.getName());

        defaultAggregatorSplits = getInteger(CONF_DEFAULT_AGGREGATOR_SPLITS,
              CONF_DEFAULT_AGGREGATOR_SPLITS_DEFAULT);

        Computation<O> computation = createComputation();
        computation.initAggregations(this);

        OptimizationSetDescriptor optimizationSetDescriptor =
           ReflectionUtils.newInstance(optimizationSetDescriptorClass);
        OptimizationSet optimizationSet =
           optimizationSetDescriptor.describe(this);

        LOG.info("Active optimizations: " + optimizationSet);

        optimizationSet.applyStartup();

        if (!isMainGraphRead()) {
            // Load graph immediately (try to make it so that everyone loads the
            // graph at the same time) This prevents imbalances if aggregators
            // use the main graph (which means that master node would load first
            // on superstep -1) then all the others would load on (superstep 0).
            // mainGraph = createGraph();
            readMainGraph();
        }

        optimizationSet.applyAfterGraphLoad();
        initialized = true;
        LOG.info("Configuration initialized");
    }

    public boolean isInitialized() {
       return initialized;
    }

    public ImmutableClassesGiraphConfiguration getUnderlyingConfiguration() {
        return giraphConfiguration;
    }

    public String getString(String key, String defaultValue) {
        return giraphConfiguration.get(key, defaultValue);
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        return giraphConfiguration.getBoolean(key, defaultValue);
    }

    public Integer getInteger(String key, Integer defaultValue) {
        return giraphConfiguration.getInt(key, defaultValue);
    }

    public Long getLong(String key, Long defaultValue) {
        return giraphConfiguration.getLong(key, defaultValue);
    }

    public Float getFloat(String key, Float defaultValue) {
        return giraphConfiguration.getFloat(key, defaultValue);
    }

    public Class<?> getClass(String key, String defaultValue) {
        try {
            return Class.forName(getString(key, defaultValue));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public String[] getStrings(String key, String... defaultValues) {
        return giraphConfiguration.getStrings(key, defaultValues);
    }

    public Class<?>[] getClasses(String key, Class<?>... defaultValues) {
       String classNamesStr = getString(key, null);
       if (classNamesStr == null) {
          return defaultValues;
       }

       String[] classNames = classNamesStr.split(",");
       if (classNames.length == 0) {
          return defaultValues;
       } else {
          try {
             Class<?>[] classes = new Class<?>[classNames.length];
             for (int i = 0; i < classes.length; ++i) {
                classes[i] = Class.forName(classNames[i]);
             }
             return classes;
          } catch (ClassNotFoundException e) {
             throw new RuntimeException(e);
          }
       }
       //return giraphConfiguration.getClasses(key, defaultValues);
    }

    public Class<? extends Pattern> getPatternClass() {
        return patternClass;
    }

    public void setPatternClass(Class<? extends Pattern> patternClass) {
       this.patternClass = patternClass;
    }

    public Pattern createPattern() {
       Pattern pattern = ReflectionUtils.newInstance(getPatternClass());
       pattern.init(this);
       return pattern;
    }

    public Class<? extends MainGraph> getMainGraphClass() {
       return mainGraphClass;
    }

    public void setMainGraphClass(Class<? extends MainGraph> graphClass) {
       mainGraphClass = graphClass;
    }

    public boolean isUseCompressedCaches() {
        return useCompressedCaches;
    }

    public int getCacheThresholdSize() {
        return cacheThresholdSize;
    }

    public String getLogLevel() {
       return getString (CONF_LOG_LEVEL, CONF_LOG_LEVEL_DEFAULT);
    }

    public String getMainGraphPath() {
        return getString(CONF_MAINGRAPH_PATH, CONF_MAINGRAPH_PATH_DEFAULT);
    }
    
    public String getMainGraphPropertiesPath() {
        return getMainGraphPath() + ".prop";
    }

    public long getInfoPeriod() {
        return infoPeriod;
    }

    public <E extends Embedding> E createEmbedding() {
        E embedding = (E) ReflectionUtils.newInstance(embeddingClass);
        embedding.init(this);
        return embedding;
    }

    public Class<? extends Embedding> getEmbeddingClass() {
        return embeddingClass;
    }

    public void setEmbeddingClass(Class<? extends Embedding> embeddingClass) {
        this.embeddingClass = embeddingClass;
    }

    public <G extends MainGraph> G getMainGraph() {
        return (G) mainGraph;
    }

    public <G extends MainGraph> void setMainGraph(G mainGraph) {
        if (mainGraph != null) {
            this.mainGraphId = mainGraph.getId();
            this.mainGraph = mainGraph;
        }
    }

    public void setMainGraphId(int mainGraphId) {
       this.mainGraphId = mainGraphId;
    }

    public int getMainGraphId() {
       return mainGraphId;
    }

    protected boolean isMainGraphRead() {
       return mainGraph.getNumberVertices() > 0 ||
          mainGraph.getNumberEdges() > 0;
    }

    public MainGraph createGraph() {
        for (Map.Entry<Integer,Configuration> entry: activeConfigs.entrySet()) {
            if (entry.getValue().mainGraphId == mainGraphId &&
                  entry.getValue().isMainGraphRead()) {
                MainGraph graph = entry.getValue().getMainGraph();
                if (graph != null) {
                   return graph;
                }
            }
        }

        try {
            Constructor<? extends MainGraph> constructor;
            constructor = mainGraphClass.getConstructor(String.class,
                  boolean.class, boolean.class);
            MainGraph graph = constructor.newInstance(getMainGraphPath(),
                  isGraphEdgeLabelled, isGraphMulti);
            if (this.mainGraphId > -1) {
               graph.setId(mainGraphId);
            }
            return graph;
        } catch (NoSuchMethodException | IllegalAccessException |
              InstantiationException | InvocationTargetException e) {
            throw new RuntimeException("Could not create main graph", e);
        }
    }

    protected void readMainGraph() {
        boolean useLocalGraph = getBoolean(CONF_MAINGRAPH_LOCAL,
              CONF_MAINGRAPH_LOCAL_DEFAULT);
        
        // maybe read properties
        try {
            Method initProperties = mainGraphClass.getMethod(
                  "initProperties", Object.class);

            if (useLocalGraph) {
                initProperties.invoke(mainGraph,
                      Paths.get(getMainGraphPropertiesPath()));
            } else {
                initProperties.invoke(mainGraph,
                      new Path(getMainGraphPropertiesPath()));
            }
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException("Could not read graph properties", e);
        } catch (InvocationTargetException e) {
           if (e.getTargetException() instanceof IOException) {
              LOG.warn("Graph properties file not found: " +
                    getMainGraphPropertiesPath());
           } else {
              throw new RuntimeException("Could not read graph properties", e);
           }
        }

        try {
            Method init = mainGraphClass.getMethod("init", Object.class);

            if (useLocalGraph) {
                init.invoke(mainGraph, Paths.get(getMainGraphPath()));
            } else {
                init.invoke(mainGraph, new Path(getMainGraphPath()));
            }
        } catch (NoSuchMethodException | IllegalAccessException |
              InvocationTargetException e) {
            throw new RuntimeException("Could not read main graph", e);
        }
    }

    public boolean isOutputActive() {
        return getBoolean(CONF_OUTPUT_ACTIVE, CONF_OUTPUT_ACTIVE_DEFAULT);
    }

    public int getODAGNumAggregators() {
        return odagNumAggregators;
    }

    public String getOdagFlushMethod() {
       return getString(CONF_ODAG_FLUSH_METHOD, CONF_ODAG_FLUSH_METHOD_DEFAULT);
    }

    public int getMaxEnumerationsPerMicroStep() {
        return 10000000;
    }

    public boolean is2LevelAggregationEnabled() {
        return is2LevelAggregationEnabled;
    }

    public boolean isForceGC() {
        return forceGC;
    }

    public <M extends Writable> M createCommunicationStrategyMessage() {
        return communicationStrategyFactory.createMessage();
    }

    public CommunicationStrategy<O> createCommunicationStrategy(
          Configuration<O> configuration,
          ExecutionEngine<O> executionEngine, WorkerContext workerContext) {
        CommunicationStrategy<O> commStrategy =
           communicationStrategyFactory.createCommunicationStrategy();

        commStrategy.setConfiguration(configuration);
        commStrategy.setExecutionEngine(executionEngine);
        commStrategy.setWorkerContext(workerContext);

        return commStrategy;
    }

    public Set<String> getRegisteredAggregations() {
        return Collections.unmodifiableSet(aggregationsMetadata.keySet());
    }

    public Map<String, AggregationStorageMetadata> getAggregationsMetadata() {
        return Collections.unmodifiableMap(aggregationsMetadata);
    }

    public void setAggregationsMetadata(
          Map<String,AggregationStorageMetadata> aggregationsMetadata) {
       this.aggregationsMetadata = aggregationsMetadata;
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name,
          Class<? extends AggregationStorage> aggStorageClass,
          Class<K> keyClass, Class<V> valueClass,
          boolean persistent, ReductionFunction<V> reductionFunction,
          EndAggregationFunction<K, V> endAggregationFunction, int numSplits,
          boolean isIncremental) {
       if (aggregationsMetadata.containsKey(name)) {
          return;
       }

       AggregationStorageMetadata<K, V> aggregationMetadata =
          new AggregationStorageMetadata<>(aggStorageClass,
                keyClass, valueClass, persistent, reductionFunction,
                endAggregationFunction, numSplits, isIncremental);

       aggregationsMetadata.put(name, aggregationMetadata);
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name,
          Class<? extends AggregationStorage> aggStorageClass,
          Class<K> keyClass, Class<V> valueClass,
          boolean persistent, ReductionFunction<V> reductionFunction,
          EndAggregationFunction<K, V> endAggregationFunction, int numSplits) {
       registerAggregation(name, aggStorageClass, keyClass, valueClass,
             persistent, reductionFunction, endAggregationFunction, numSplits,
             isAggregationIncremental());
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name,
          Class<K> keyClass, Class<V> valueClass,
          boolean persistent, ReductionFunction<V> reductionFunction) {
    	registerAggregation(name,
              getAggregationStorageClass(), keyClass, valueClass, persistent,
              reductionFunction, null, defaultAggregatorSplits,
              isAggregationIncremental());
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name,
          Class<? extends AggregationStorage> aggStorageClass,
          Class<K> keyClass, Class<V> valueClass,
          boolean persistent, ReductionFunction<V> reductionFunction) {
    	registerAggregation(name, aggStorageClass, keyClass, valueClass,
              persistent, reductionFunction, null, defaultAggregatorSplits,
              isAggregationIncremental());
    }
    
    public <K extends Writable, V extends Writable>
    void registerAggregation(String name,
          Class<K> keyClass, Class<V> valueClass,
          boolean persistent,
          ReductionFunction<V> reductionFunction,
          EndAggregationFunction<K, V> endAggregationFunction) {
    	registerAggregation(name,
              getAggregationStorageClass(), keyClass, valueClass, persistent,
              reductionFunction, endAggregationFunction,
              defaultAggregatorSplits, isAggregationIncremental());
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name,
          Class<? extends AggregationStorage> aggStorageClass,
          Class<K> keyClass, Class<V> valueClass,
          boolean persistent,
          ReductionFunction<V> reductionFunction,
          EndAggregationFunction<K, V> endAggregationFunction,
          boolean isIncremental) {
    	registerAggregation(name, aggStorageClass, keyClass, valueClass,
              persistent, reductionFunction, endAggregationFunction,
              defaultAggregatorSplits, isIncremental);
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name,
          Class<? extends AggregationStorage> aggStorageClass,
          Class<K> keyClass, Class<V> valueClass,
          boolean persistent,
          ReductionFunction<V> reductionFunction,
          EndAggregationFunction<K, V> endAggregationFunction) {
    	registerAggregation(name, aggStorageClass, keyClass, valueClass,
              persistent, reductionFunction, endAggregationFunction,
              defaultAggregatorSplits, isAggregationIncremental());
    }

    public <K extends Writable, V extends Writable>
    AggregationStorageMetadata<K, V> getAggregationMetadata(String name) {
        AggregationStorageMetadata<K,V> metadata =
           (AggregationStorageMetadata<K, V>) aggregationsMetadata.get(name);
        return metadata;
    }

    public String getAggregationSplitName(String name, int splitId) {
        return name + "_" + splitId;
    }

    public <O extends Embedding> Computation<O> createComputation() {
        return ReflectionUtils.newInstance(computationClass);
    }
    
    public <K extends Writable, V extends Writable>
    AggregationStorage<K,V> createAggregationStorage(String name) {
        return ReflectionUtils.newInstance (
              getAggregationMetadata(name).getAggregationStorageClass());
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getOutputFormat() {
       return getString(CONF_OUTPUT_FORMAT, CONF_OUTPUT_FORMAT_DEFAULT);
    }

    public MasterComputation createMasterComputation() {
        return ReflectionUtils.newInstance(masterComputationClass);
    }

    public void setIsGraphEdgeLabelled(boolean isGraphEdgeLabelled) {
       this.isGraphEdgeLabelled = isGraphEdgeLabelled;
    }

    public boolean isGraphEdgeLabelled() {
        return isGraphEdgeLabelled;
    }

    public boolean isGraphMulti() {
        return isGraphMulti;
    }

    public Class<? extends Computation> getComputationClass() {
        return computationClass;
    }
    
    public Class<? extends AggregationStorage> getAggregationStorageClass() {
        return (Class<? extends AggregationStorage>) getClass(
              CONF_AGGREGATION_STORAGE_CLASS,
              CONF_AGGREGATION_STORAGE_CLASS_DEFAULT);
    }

    public void setMasterComputationClass(
          Class<? extends MasterComputation> masterComputationClass) {
       this.masterComputationClass = masterComputationClass;
    }

    public void setComputationClass(
          Class<? extends Computation> computationClass) {
       this.computationClass = computationClass;
    }

    public void setOptimizationSetDescriptorClass(
          Class<? extends OptimizationSetDescriptor> optimizationSetDescriptorClass) {
       this.optimizationSetDescriptorClass = optimizationSetDescriptorClass;
    }

    public OptimizationSet getOptimizationSet() {
       OptimizationSetDescriptor optimizationSetDescriptor =
          ReflectionUtils.newInstance(optimizationSetDescriptorClass);
       OptimizationSet optimizationSet =
          optimizationSetDescriptor.describe(this);
       return optimizationSet;
    }

    public boolean isAggregationIncremental() {
       return getBoolean (CONF_INCREMENTAL_AGGREGATION,
             CONF_INCREMENTAL_AGGREGATION_DEFAULT);
    }

    public boolean shouldKeepMaximal() {
       return getBoolean (CONF_EMBEDDING_KEEP_MAXIMAL,
             CONF_EMBEDDING_KEEP_MAXIMAL_DEFAULT);
    }

    public int getMaxOdags() {
       return getInteger (CONF_COMM_STRATEGY_ODAGMP_MAX,
             CONF_COMM_STRATEGY_ODAGMP_MAX_DEFAULT);
    }

    public String getCommStrategy() {
       return getString (CONF_COMM_STRATEGY, CONF_COMM_STRATEGY_DEFAULT);
    }

    public String getMasterHostname() {
       return getString(CONF_MASTER_HOSTNAME, CONF_MASTER_HOSTNAME_DEFAULT);
    }

    public int getGtagBatchSizeLow() {
       return getInteger(CONF_GTAG_BATCH_SIZE_LOW,
             CONF_GTAG_BATCH_SIZE_LOW_DEFAULT);
    }
    
    public int getGtagBatchSizeHigh() {
       return getInteger(CONF_GTAG_BATCH_SIZE_HIGH,
             CONF_GTAG_BATCH_SIZE_HIGH_DEFAULT);
    }

    public boolean internalWsEnabled() {
       return getBoolean(CONF_WS_INTERNAL, CONF_WS_MODE_INTERNAL_DEFAULT);
    }
    
    public boolean externalWsEnabled() {
       return getBoolean(CONF_WS_EXTERNAL, CONF_WS_MODE_EXTERNAL_DEFAULT);
    }

    public boolean wsEnabled() {
       return internalWsEnabled() || externalWsEnabled();
    }

    public int getNumWords() {
       Class<? extends Embedding> embeddingClass = getEmbeddingClass();
       if (embeddingClass == EdgeInducedEmbedding.class) {
          return getMainGraph().getNumberEdges();
       } else if (embeddingClass == VertexInducedEmbedding.class) {
          return getMainGraph().getNumberVertices();
       } else {
          throw new RuntimeException(
                "Unknown embedding type " + embeddingClass);
       }
    }

    public int getNumVertices() {
       return getMainGraph().getNumberVertices();
    }
    
    public int getNumEdges() {
       return getMainGraph().getNumberEdges();
    }
}


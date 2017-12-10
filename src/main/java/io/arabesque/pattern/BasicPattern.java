package io.arabesque.pattern;

import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.Edge;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.Vertex;
import io.arabesque.pattern.pool.PatternEdgePool;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.IntCollectionAddConsumer;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMapFactory;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;

// TODO: use murmur hash to do pattern hashing
//import com.google.common.hash.Hashing;

public abstract class BasicPattern implements Pattern {
    private static final Logger LOG = Logger.getLogger(BasicPattern.class);

    protected int configurationId;

    protected HashIntIntMapFactory positionMapFactory = HashIntIntMaps.getDefaultFactory().withDefaultValue(-1);

    // Basic structure {{
    private IntArrayList vertices;
    private PatternEdgeArrayList edges;
    // K = vertex id, V = vertex position
    private IntIntMap vertexPositions;
    // }}

    // Incremental building {{
    private IntArrayList previousWords; // TODO: is it previous or current ?
    private int numVerticesAddedFromPrevious;
    private int numAddedEdgesFromPrevious;
    // }}

    // Isomorphisms {{
    private VertexPositionEquivalences vertexPositionEquivalences;
    private IntIntMap canonicalLabelling;
    // }}

    // Others {{
    protected transient MainGraph mainGraph;
    private PatternEdgePool patternEdgePool;

    protected volatile boolean dirtyVertexPositionEquivalences;
    protected volatile boolean dirtyCanonicalLabelling;

    protected IntCollectionAddConsumer intAddConsumer = new IntCollectionAddConsumer();
    // }}

    public BasicPattern() {
        vertices = new IntArrayList();
        vertexPositions = positionMapFactory.newMutableMap();
        previousWords = new IntArrayList();
        reset();
    }

    public BasicPattern(BasicPattern basicPattern) {
        this();

        intAddConsumer.setCollection(vertices);
        basicPattern.vertices.forEach(intAddConsumer);

        edges = createPatternEdgeArrayList(basicPattern.
              getConfig().isGraphEdgeLabelled());

        patternEdgePool = PatternEdgePool.instance(
              basicPattern.getConfig().isGraphEdgeLabelled());

        edges.ensureCapacity(basicPattern.edges.size());

        for (PatternEdge otherEdge : basicPattern.edges) {
            edges.add(createPatternEdge(otherEdge));
        }

        vertexPositions.putAll(basicPattern.vertexPositions);
    }

    @Override
    public void init(Configuration config) {
        configurationId = config.getId();

        if (edges == null) {
           edges = createPatternEdgeArrayList(getConfig().isGraphEdgeLabelled());
        }

        if (patternEdgePool == null) {
           patternEdgePool = PatternEdgePool.
              instance(getConfig().isGraphEdgeLabelled());
        }

        // mainGraph = getConfig().getMainGraph();
    }

    @Override
    public void reset() {
        if (vertices != null) {
           vertices.clear();
        }

        if (patternEdgePool != null) {
           patternEdgePool.reclaimObjects(edges);
        }

        if (edges != null) {
           edges.clear();
        }

        if (vertexPositions != null) {
           vertexPositions.clear();
        }

        setDirty();

        resetIncremental();
    }

    protected Configuration getConfig() {
       return Configuration.get(configurationId);
    }

    private void resetIncremental() {
        numVerticesAddedFromPrevious = 0;
        numAddedEdgesFromPrevious = 0;
        if (previousWords != null) {
           previousWords.clear();
        }
    }

    @Override
    public void setEmbedding(Embedding embedding) {
        try {
            if (canDoIncremental(embedding)) {
                setEmbeddingIncremental(embedding);
            } else {
                setEmbeddingFromScratch(embedding);
            }
        } catch (RuntimeException e) {
            LOG.error("Embedding: " + embedding + " " + e);
            throw e;
        }
    }

    /**
     * Reset everything and do everything from scratch.
     *
     * @param embedding
     */
    private void setEmbeddingFromScratch(Embedding embedding) {
        reset();

        int numEdgesInEmbedding = embedding.getNumEdges();
        int numVerticesInEmbedding = embedding.getNumVertices();

        if (numEdgesInEmbedding == 0 && numVerticesInEmbedding == 0) {
            return;
        }

        ensureCanStoreNewVertices(numVerticesInEmbedding);
        ensureCanStoreNewEdges(numEdgesInEmbedding);

        IntArrayList embeddingVertices = embedding.getVertices();

        for (int i = 0; i < numVerticesInEmbedding; ++i) {
            addVertex(embeddingVertices.getUnchecked(i));
        }

        numVerticesAddedFromPrevious = embedding.getNumVerticesAddedWithExpansion();

        IntArrayList embeddingEdges = embedding.getEdges();

        for (int i = 0; i < numEdgesInEmbedding; ++i) {
            addEdge(embeddingEdges.getUnchecked(i));
        }

        numAddedEdgesFromPrevious = embedding.getNumEdgesAddedWithExpansion();

        updateUsedEmbeddingFromScratch(embedding);
    }

    private void updateUsedEmbeddingFromScratch(Embedding embedding) {
        previousWords.clear();

        int embeddingNumWords = embedding.getNumWords();

        previousWords.ensureCapacity(embeddingNumWords);

        IntArrayList words = embedding.getWords();

        for (int i = 0; i < embeddingNumWords; i++) {
            previousWords.add(words.getUnchecked(i));
        }
    }

    private void resetToPrevious() {
        removeLastNEdges(numAddedEdgesFromPrevious);
        removeLastNVertices(numVerticesAddedFromPrevious);
        setDirty();
    }

    private void removeLastNEdges(int n) {
        int targetI = edges.size() - n;

        for (int i = edges.size() - 1; i >= targetI; --i) {
            patternEdgePool.reclaimObject(edges.remove(i));
        }
    }

    private void removeLastNVertices(int n) {
        int targetI = vertices.size() - n;

        for (int i = vertices.size() - 1; i >= targetI; --i) {
            try {
                vertexPositions.remove(vertices.getUnchecked(i));
            } catch (IllegalArgumentException e) {
                System.err.println(e.toString());
                System.err.println("i=" + i);
                System.err.println("targetI=" + targetI);
                throw e;
            }
        }

        vertices.removeLast(n);
    }

    /**
     * Only the last word has changed, so skipped processing for the previous vertices.
     *
     * @param embedding
     */
    private void setEmbeddingIncremental(Embedding embedding) {
        resetToPrevious();

        numVerticesAddedFromPrevious = embedding.getNumVerticesAddedWithExpansion();
        numAddedEdgesFromPrevious = embedding.getNumEdgesAddedWithExpansion();

        ensureCanStoreNewVertices(numVerticesAddedFromPrevious);
        ensureCanStoreNewEdges(numAddedEdgesFromPrevious);

        IntArrayList embeddingVertices = embedding.getVertices();
        int numVerticesInEmbedding = embedding.getNumVertices();
        for (int i = (numVerticesInEmbedding - numVerticesAddedFromPrevious); i < numVerticesInEmbedding; ++i) {
            addVertex(embeddingVertices.getUnchecked(i));
        }

        IntArrayList embeddingEdges = embedding.getEdges();
        int numEdgesInEmbedding = embedding.getNumEdges();
        for (int i = (numEdgesInEmbedding - numAddedEdgesFromPrevious); i < numEdgesInEmbedding; ++i) {
            addEdge(embeddingEdges.getUnchecked(i));
        }

        updateUsedEmbeddingIncremental(embedding);
    }

    /**
     * By default only the last word changed.
     *
     * @param embedding
     */
    private void updateUsedEmbeddingIncremental(Embedding embedding) {
        previousWords.setUnchecked(previousWords.size() - 1, embedding.getWords().getUnchecked(previousWords.size() - 1));
    }

    private void ensureCanStoreNewEdges(int numAddedEdgesFromPrevious) {
        int newNumEdges = edges.size() + numAddedEdgesFromPrevious;

        edges.ensureCapacity(newNumEdges);
    }

    private void ensureCanStoreNewVertices(int numVerticesAddedFromPrevious) {
        int newNumVertices = vertices.size() + numVerticesAddedFromPrevious;

        vertices.ensureCapacity(newNumVertices);
        vertexPositions.ensureCapacity(newNumVertices);
    }

    /**
     * Can do incremental only if the last word is different.
     *
     * @param embedding
     * @return
     */
    private boolean canDoIncremental(Embedding embedding) {
        if (embedding.getNumWords() == 0 || previousWords.size() != embedding.getNumWords()) {
            return false;
        }

        // Maximum we want 1 change (which we know by default that it exists in the last position).
        // so we check
        final IntArrayList words = embedding.getWords();
        for (int i = previousWords.size() - 2; i >= 0; i--) {
            if (words.getUnchecked(i) != previousWords.getUnchecked(i)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int getNumberOfVertices() {
        return vertices.getSize();
    }

    @Override
    public boolean addEdge(int edgeId) {
        Edge edge = getMainGraph().getEdge(edgeId);

        return addEdge(edge);
    }

    public boolean addEdge(Edge edge) {
        int srcId = edge.getSourceId();
        int srcPos = addVertex(srcId);
        int dstPos = addVertex(edge.getDestinationId());

        PatternEdge patternEdge = createPatternEdge(edge, srcPos, dstPos, srcId);

        return addEdge(patternEdge);
    }

    public boolean addEdge(PatternEdge edge) {
        // TODO: Remove when we have directed edges
        if (edge.getSrcPos() > edge.getDestPos()) {
            edge.invert();
        }

        edges.add(edge);

        setDirty();

        return true;
    }

    public int addVertex(int vertexId) {
        int pos = vertexPositions.get(vertexId);

        if (pos == -1) {
            pos = vertices.size();
            vertices.add(vertexId);
            vertexPositions.put(vertexId, pos);
            setDirty();
        }

        return pos;
    }

    protected void setDirty() {
        dirtyCanonicalLabelling = true;
        dirtyVertexPositionEquivalences = true;
    }

    @Override
    public int getNumberOfEdges() {
        return edges.size();
    }

    @Override
    public IntArrayList getVertices() {
        return vertices;
    }

    @Override
    public PatternEdgeArrayList getEdges() {
        return edges;
    }

    @Override
    public VertexPositionEquivalences getVertexPositionEquivalences() {
        if (dirtyVertexPositionEquivalences) {
            synchronized (this) {
                if (dirtyVertexPositionEquivalences) {
                    if (vertexPositionEquivalences == null) {
                        vertexPositionEquivalences = new VertexPositionEquivalences();
                    }

                    vertexPositionEquivalences.setNumVertices(getNumberOfVertices());
                    vertexPositionEquivalences.clear();

                    fillVertexPositionEquivalences(vertexPositionEquivalences);

                    dirtyVertexPositionEquivalences = false;
                }
            }
        }

        return vertexPositionEquivalences;
    }

    protected abstract void fillVertexPositionEquivalences(VertexPositionEquivalences vertexPositionEquivalences);

    @Override
    public IntIntMap getCanonicalLabeling() {
        if (dirtyCanonicalLabelling) {
            synchronized (this) {
                if (dirtyCanonicalLabelling) {
                    if (canonicalLabelling == null) {
                        canonicalLabelling = HashIntIntMaps.newMutableMap(getNumberOfVertices());
                    }

                    canonicalLabelling.clear();

                    fillCanonicalLabelling(canonicalLabelling);

                    dirtyCanonicalLabelling = false;
                }
            }
        }

        return canonicalLabelling;
    }

    protected abstract void fillCanonicalLabelling(IntIntMap canonicalLabelling);

    @Override
    public boolean turnCanonical() {
        resetIncremental();

        IntIntMap canonicalLabelling = getCanonicalLabeling();

        IntIntCursor canonicalLabellingCursor = canonicalLabelling.cursor();

        boolean allEqual = true;

        while (canonicalLabellingCursor.moveNext()) {
            int oldPos = canonicalLabellingCursor.key();
            int newPos = canonicalLabellingCursor.value();

            if (oldPos != newPos) {
                allEqual = false;
            }
        }

        if (allEqual) {
            edges.sort();
            return false;
        }

        IntArrayList oldVertices = new IntArrayList(vertices);

        for (int i = 0; i < vertices.size(); ++i) {
            int newPos = canonicalLabelling.get(i);

            // If position didn't change, do nothing
            if (newPos == i) {
                continue;
            }

            int vertexId = oldVertices.getUnchecked(i);
            vertices.setUnchecked(newPos, vertexId);

            vertexPositions.put(vertexId, newPos);
        }

        for (int i = 0; i < edges.size(); ++i) {
            PatternEdge edge = edges.get(i);

            int srcPos = edge.getSrcPos();
            int dstPos = edge.getDestPos();

            int convertedSrcPos = canonicalLabelling.get(srcPos);
            int convertedDstPos = canonicalLabelling.get(dstPos);

            if (convertedSrcPos < convertedDstPos) {
                edge.setSrcPos(convertedSrcPos);
                edge.setDestPos(convertedDstPos);
            } else {
                // If we changed the position of source and destination due to
                // relabel, we also have to change the labels to match this
                // change.
                int tmp = edge.getSrcLabel();
                edge.setSrcPos(convertedDstPos);
                edge.setSrcLabel(edge.getDestLabel());
                edge.setDestPos(convertedSrcPos);
                edge.setDestLabel(tmp);
            }
        }

        edges.sort();

        return true;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(getConfig().getId());
        edges.write(dataOutput);
        vertices.write(dataOutput);
    }

    @Override
    public void writeExternal(ObjectOutput objOutput) throws IOException {
       write (objOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        reset();

        init(Configuration.get(dataInput.readInt()));

        edges.readFields(dataInput);
        vertices.readFields(dataInput);

        for (int i = 0; i < vertices.size(); ++i) {
            vertexPositions.put(vertices.getUnchecked(i), i);
        }
    }
    @Override
    public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {
       readFields(objInput);
    }

    protected PatternEdgeArrayList createPatternEdgeArrayList(boolean areEdgesLabelled) {
        return new PatternEdgeArrayList(areEdgesLabelled);
    }

    protected PatternEdge createPatternEdge(Edge edge, int srcPos, int dstPos, int srcId) {
        PatternEdge patternEdge = patternEdgePool.createObject();

        patternEdge.setFromEdge(getConfig().getMainGraph(),
              edge, srcPos, dstPos, srcId);

        return patternEdge;
    }

    protected PatternEdge createPatternEdge(PatternEdge otherEdge) {
        PatternEdge patternEdge = patternEdgePool.createObject();

        patternEdge.setFromOther(otherEdge);

        return patternEdge;
    }

    @Override
    public String toString() {
        return toOutputString();
    }

    @Override
    public String toOutputString() {
        if (getNumberOfEdges() > 0) {
            return StringUtils.join(edges, ",");
        }
        else if (getNumberOfVertices() == 1) {
            Vertex vertex = getMainGraph().getVertex(vertices.getUnchecked(0));

            return "0(" + vertex.getVertexLabel() + ")";
        }
        else {
            return "";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicPattern that = (BasicPattern) o;

        return edges.equals(that.edges);

    }

    public boolean equals(Object o, int upTo) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BasicPattern that = (BasicPattern) o;

        if (this.getNumberOfEdges() < upTo || that.getNumberOfEdges() < upTo)
           return false;
        
        PatternEdgeArrayList otherEdges = that.getEdges();
        for (int i = 0; i < upTo; i++) {
           if (!edges.getUnchecked(i).equals (otherEdges.getUnchecked(i)))
              return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
       // TODO
        return //edges.isEmpty() ? mainGraph.getVertex(vertices.getUnchecked(0)).getVertexLabel() :
           edges.hashCode();
    }

    public MainGraph getMainGraph() {
        return getConfig().getMainGraph();
    }
}

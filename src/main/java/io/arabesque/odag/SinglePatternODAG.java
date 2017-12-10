package io.arabesque.odag;

import io.arabesque.computation.Computation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.odag.domain.DomainStorage;
import io.arabesque.odag.domain.DomainStorageReadOnly;
import io.arabesque.odag.domain.StorageReader;
import io.arabesque.odag.domain.StorageStats;
import io.arabesque.pattern.Pattern;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.concurrent.ExecutorService;

public class SinglePatternODAG extends BasicODAG {
    private Pattern pattern;

    public SinglePatternODAG(Pattern pattern, int numberOfDomains) {
        this.pattern = pattern;
        serializeAsReadOnly = false;
        storage = new DomainStorage(numberOfDomains);
    }

    public SinglePatternODAG(boolean readOnly) {
        this();
        serializeAsReadOnly = false;
        storage = createDomainStorage(readOnly);
    }

    public SinglePatternODAG() {
    }

    @Override
    public SinglePatternODAG init(Configuration config) {
       // do nothing by now, maybe we will want a reference of 'Configuration'
       // in here as well (see MultiPatternODAG)
       return this;
    }

    @Override
    public void addEmbedding(Embedding embedding) {
        if (pattern == null) {
            throw new RuntimeException("Tried to add an embedding without letting embedding zip know about the pattern");
        }

        storage.addEmbedding(embedding);
    }

    @Override
    public void aggregate(BasicODAG embZip) {
        if (embZip == null) return;

        storage.aggregate(embZip.storage);
    }

    @Override
    public StorageReader getReader(Configuration<Embedding> configuration, Computation<Embedding> computation,
          int numPartitions, int numBlocks, int maxBlockSize) {
        return storage.getReader(pattern, configuration, computation, numPartitions, numBlocks, maxBlockSize);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        storage.write(out);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
       out.writeBoolean(serializeAsReadOnly);
       write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.clear();
        storage.readFields(in);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
       serializeAsReadOnly = in.readBoolean();
       storage = createDomainStorage(serializeAsReadOnly);
       readFields(in);
    }

    public void setPattern(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public Pattern getPattern() {
        return pattern;
    }

    @Override
    public String toString() {
       return "SinglePatternODAG(" +
          (pattern != null ? pattern.toString() : "") + ")" +
          storage.toString();
    }
}

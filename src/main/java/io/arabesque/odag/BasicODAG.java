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

public abstract class BasicODAG implements Writable, Externalizable {
    protected DomainStorage storage;
    protected boolean serializeAsReadOnly;

    protected DomainStorage createDomainStorage(boolean readOnly) {
        if (readOnly) return new DomainStorageReadOnly();
        else return new DomainStorage();
    }

    public abstract BasicODAG init(Configuration config);
    public abstract void addEmbedding(Embedding embedding);
    public abstract Pattern getPattern();
    public abstract StorageReader getReader(
          Configuration<Embedding> configuration,
          Computation<Embedding> computation,
          int numPartitions,
          int numBlocks,
          int maxBlockSize);
    public abstract void aggregate(BasicODAG embZip);

    public int getNumberOfDomains() {
        return storage.getNumberOfDomains();
    }

    public DomainStorage getStorage() {
        return storage;
    }

    public long getNumberOfEnumerations() {
        return storage.getNumberOfEnumerations();
    }

    public void finalizeConstruction(ExecutorService pool, int numParts) {
        storage.finalizeConstruction(pool, numParts);
    }

    public void clear() {
        storage.clear();
    }

    public StorageStats getStats() {
        return storage.getStats();
    }

    public boolean getSerializeasWriteOnly() {
        return serializeAsReadOnly;
    }

    public void setSerializeAsReadOnly (boolean serializeAsReadOnly) {
        this.serializeAsReadOnly = serializeAsReadOnly;
    }

    public void writeInParts(DataOutput[] outputs, boolean[] hasContent) throws IOException {
        storage.write(outputs, hasContent);
    }

}

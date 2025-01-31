/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.jvector;

import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.ReaderSupplierFactory;
import io.github.jbellis.jvector.graph.NodesIterator;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.search.VectorScorer;

import java.io.IOException;
import java.nio.file.Path;

public class JVectorFloatVectorValues extends FloatVectorValues {
    private final OnDiskGraphIndex onDiskGraphIndex;
    private final OnDiskGraphIndex.View view;
    private int docId = -1;
    private final NodesIterator nodesIterator;
    private final ReaderSupplier readerSupplier;

    public JVectorFloatVectorValues(Path jvecFilePath) throws IOException {
        this.readerSupplier = ReaderSupplierFactory.open(jvecFilePath);
        this.onDiskGraphIndex = OnDiskGraphIndex.load(readerSupplier);
        this.view = onDiskGraphIndex.getView();
        this.nodesIterator = onDiskGraphIndex.getNodes();
    }

    @Override
    public int dimension() {
        return onDiskGraphIndex.getDimension();
    }

    @Override
    public int size() {
        return onDiskGraphIndex.size();
    }

    @Override
    public float[] vectorValue() throws IOException {
        if (!onDiskGraphIndex.containsNode(docId)) {
            throw new RuntimeException("DocId " + docId + " not found in graph");
        }
        try {
            final VectorFloat<?> vector = view.getVector(docId);
            return (float[])vector.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public VectorScorer scorer(float[] query) throws IOException {
        return null;
    }

    @Override
    public int docID() {
        return docId;
    }

    @Override
    public int nextDoc() throws IOException {
        if (nodesIterator.hasNext()) {
            docId = nodesIterator.next();
        } else {
            docId = NO_MORE_DOCS;
        }

        return docId;
    }

    @Override
    public int advance(int target) throws IOException {
        return slowAdvance(target);
    }
}

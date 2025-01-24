/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.jvector;

import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;

import java.io.IOException;

public class JVectorFlatVectorsWriter extends FlatVectorsWriter {
    /**
     * Sole constructor
     * @param state segment write state
     * @param vectorsScorer vectors scorer
     */
    protected JVectorFlatVectorsWriter(SegmentWriteState state, FlatVectorsScorer vectorsScorer) {
        super(vectorsScorer);
    }

    @Override
    public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        return null;
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {

    }

    @Override
    public void finish() throws IOException {

    }

    @Override
    public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }
}

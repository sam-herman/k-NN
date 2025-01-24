/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.lucene;

import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class JVectorFlatVectorsFormat extends FlatVectorsFormat {
    static final String NAME = "JVectorFlatVectorsFormat";
    static final String META_CODEC_NAME = "JVectorFlatVectorsFormatMeta";
    static final String VECTOR_DATA_CODEC_NAME = "JVectorFlatVectorsFormatData";
    static final String META_EXTENSION = "jvemf";
    static final String VECTOR_DATA_EXTENSION = "jvec";

    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = VERSION_START;

    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
    private final FlatVectorsScorer vectorsScorer;

    /** Constructs a format */
    public JVectorFlatVectorsFormat(FlatVectorsScorer vectorsScorer) {
        super(NAME);
        this.vectorsScorer = vectorsScorer;
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99FlatVectorsWriter(state, vectorsScorer);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99FlatVectorsReader(state, vectorsScorer);
    }

    @Override
    public String toString() {
        return "JVectorFlatVectorsFormat(" + "vectorsScorer=" + vectorsScorer + ')';
    }
}

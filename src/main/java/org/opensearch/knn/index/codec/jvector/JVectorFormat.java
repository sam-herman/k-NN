/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.jvector;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class JVectorFormat extends KnnVectorsFormat {
    static final String META_CODEC_NAME = "JVectorVectorsFormatMeta";
    static final String VECTOR_INDEX_CODEC_NAME = "JVectorVectorsFormatIndex";
    static final String META_EXTENSION = "jmeta";
    static final String VECTOR_INDEX_EXTENSION = "jvecs";
    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = VERSION_START;
    public static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    private static final FlatVectorsFormat flatVectorsFormat =
            new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
    /**
     * Sole constructor
     *
     * @param name
     */
    protected JVectorFormat(String name) {
        super(name);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new JVectorWriter(state, flatVectorsFormat.fieldsWriter(state));
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new JVectorReader(state, flatVectorsFormat.fieldsReader(state));
    }
}

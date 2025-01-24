/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.lucene;

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

public class JVectorReader extends KnnVectorsReader {

    public JVectorReader(SegmentReadState state) {
        super();
    }

    @Override
    public void checkIntegrity() throws IOException {
        throw new UnsupportedOperationException("checkIntegrity not supported with JVectorFieldsWriter");
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return null;
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return null;
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {

    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }
}

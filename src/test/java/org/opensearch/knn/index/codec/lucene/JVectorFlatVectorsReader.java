/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.lucene;

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

public class JVectorFlatVectorsReader extends FlatVectorsReader {
    /**
     * Sole constructor
     * @param state segment read state
     * @param vectorsScorer vectors scorer
     */
    protected JVectorFlatVectorsReader(SegmentReadState state, FlatVectorsScorer vectorsScorer) {
        super(vectorsScorer);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
        return null;
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
        return null;
    }

    @Override
    public void checkIntegrity() throws IOException {

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
    public void close() throws IOException {

    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    private static final VectorTypeSupport VECTOR_TYPE_SUPPORT = VectorizationProvider.getInstance().getVectorTypeSupport();

    public static ArrayList<VectorFloat<?>> readFlatVectors(String filePath) throws IOException {
        var vectors = new ArrayList<VectorFloat<?>>();
        try (var dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filePath)))) {
            while (dis.available() > 0) {
                var dimension = Integer.reverseBytes(dis.readInt());
                assert dimension > 0 : dimension;
                var buffer = new byte[dimension * Float.BYTES];
                dis.readFully(buffer);
                var byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);

                var vector = new float[dimension];
                var floatBuffer = byteBuffer.asFloatBuffer();
                floatBuffer.get(vector);
                vectors.add(VECTOR_TYPE_SUPPORT.createFloatVector(vector));
            }
        }
        return vectors;
    }
}

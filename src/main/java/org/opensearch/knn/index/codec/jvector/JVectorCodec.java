/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.jvector;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;

public class JVectorCodec extends FilterCodec {

    public static final String CODEC_NAME = "JVectorCodec";

    public JVectorCodec() {
        super(CODEC_NAME, new Lucene912Codec());
    }

    /**
     * Sole constructor. When subclassing this codec, create a no-arg ctor and pass the delegate codec
     * and a unique name to this ctor.
     *
     * @param name
     * @param delegate
     */
    protected JVectorCodec(String name, Codec delegate) {
        super(name, delegate);
    }

    @Override
    public KnnVectorsFormat knnVectorsFormat() {
        return new JVectorFormat();
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.lucene;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;

public class JVectorCodec extends FilterCodec {

    public static final String CODEC_NAME = "JVectorCodec";

    public JVectorCodec() {
        super(CODEC_NAME, Codec.getDefault());
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
        return new JVectorFormat(CODEC_NAME);
    }
}

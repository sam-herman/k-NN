/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.knn.index.codec.jvector.JVectorCodec;

import java.io.IOException;

/**
 * Test used specifically for JVector
 */
public class KNNJVectorTests extends KNNTestCase {

    @Test
    public void testJVectorKnnIndex() throws IOException {
        IndexWriterConfig indexWriterConfig = LuceneTestCase.newIndexWriterConfig();
        indexWriterConfig.setCodec(new JVectorCodec());
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir, indexWriterConfig)) {
            // Note: even though a field was added, it doesn't participate in the formulation of the histogram
            // It's still there just to demonstrate that the histogram is formulated correctly and ignores other fields than the range field
            // specified
            final float[] target = new float[] { 1.0f, 1.0f };
            for (int i = 1; i < 11; i++) {
                final float[] source = new float[] { 1.0f, 0f / i };
                final Document doc = new Document();
                doc.add(new KnnFloatVectorField("test_field", source, VectorSimilarityFunction.EUCLIDEAN));
                w.addDocument(doc);
            }
            w.commit();

            try (IndexReader reader = w.getReader()) {
                final Query filterQuery = new MatchAllDocsQuery();
                final IndexSearcher searcher = newSearcher(reader);
                KnnFloatVectorQuery knnFloatVectorQuery = new KnnFloatVectorQuery("test_field", target, 3, filterQuery);
                TopDocs topDocs = searcher.search(knnFloatVectorQuery, 3);
                assertEquals(3, topDocs.totalHits.value);
            }
        }
    }
}

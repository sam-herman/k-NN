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
import org.opensearch.knn.index.codec.lucene.JVectorCodec;

import java.io.IOException;
import java.util.*;

import static org.mockito.Mockito.*;
import static org.opensearch.knn.common.KNNConstants.*;

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
            Document doc = new Document();
            for (int i = 0; i < 1; i++) {
                doc.add(new KnnFloatVectorField("test_field", target, VectorSimilarityFunction.EUCLIDEAN));
                w.addDocument(doc);
            }
            w.commit();

            try (IndexReader reader = w.getReader()) {
                final Query filterQuery = new MatchAllDocsQuery();
                final IndexSearcher searcher = newSearcher(reader);
                KnnFloatVectorQuery knnFloatVectorQuery = new KnnFloatVectorQuery("test_field", target, 10, filterQuery);
                TopDocs topDocs = searcher.search(knnFloatVectorQuery, 10);
                assertEquals(10, topDocs.totalHits.value);
            }
        }
    }
}

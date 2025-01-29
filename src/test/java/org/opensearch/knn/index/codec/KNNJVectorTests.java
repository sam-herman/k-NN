/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.knn.index.codec.jvector.JVectorCodec;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Test used specifically for JVector
 */
@Log4j2
public class KNNJVectorTests extends LuceneTestCase {

    @Test
    public void testJVectorKnnIndex() throws IOException {
        int k = 3; // The number of nearest neighbours to gather
        int totalNumberOfDocs = 10;
        IndexWriterConfig indexWriterConfig = LuceneTestCase.newIndexWriterConfig();
        // TODO: re-enable this after fixing the compound file augmentation for JVector
        indexWriterConfig.setUseCompoundFile(false);
        indexWriterConfig.setCodec(new JVectorCodec());
        final Path indexPath = createTempDir();
        log.info("Index path: {}", indexPath);
        try (Directory dir = newFSDirectory(indexPath);
             RandomIndexWriter w = new RandomIndexWriter(random(), dir, indexWriterConfig)) {
            // Note: even though a field was added, it doesn't participate in the formulation of the histogram
            // It's still there just to demonstrate that the histogram is formulated correctly and ignores other fields than the range field
            // specified
            final float[] target = new float[] { 0.0f, 0.0f };
            for (int i = 1; i < totalNumberOfDocs + 1; i++) {
                final float[] source = new float[] { 0.0f, 1.0f / i };
                final Document doc = new Document();
                doc.add(new KnnFloatVectorField("test_field", source, VectorSimilarityFunction.EUCLIDEAN));
                w.addDocument(doc);
            }
            log.info("Flushing docs to make them discoverable on the file system");
            w.commit();


            try (IndexReader reader = w.getReader()) {
                final Query filterQuery = new MatchAllDocsQuery();
                final IndexSearcher searcher = newSearcher(reader);
                KnnFloatVectorQuery knnFloatVectorQuery = new KnnFloatVectorQuery("test_field", target, k, filterQuery);
                TopDocs topDocs = searcher.search(knnFloatVectorQuery, k);
                assertEquals(k, topDocs.totalHits.value);
                assertEquals(9, topDocs.scoreDocs[0].doc);
                Assert.assertEquals(VectorSimilarityFunction.EUCLIDEAN.compare(target, new float[]{0.0f, 1.0f / 10.0f}), topDocs.scoreDocs[0].score, 0.01f);
                assertEquals(8, topDocs.scoreDocs[1].doc);
                Assert.assertEquals(VectorSimilarityFunction.EUCLIDEAN.compare(target, new float[]{0.0f, 1.0f / 9.0f}), topDocs.scoreDocs[0].score, 0.1f);
                assertEquals(7, topDocs.scoreDocs[2].doc);
                Assert.assertEquals(VectorSimilarityFunction.EUCLIDEAN.compare(target, new float[]{0.0f, 1.0f / 8.0f}), topDocs.scoreDocs[0].score, 0.1f);
            }

            // TODO: Wire the execution thread pool to {@link IndexGraphBuilder} to avoid the failure of the UT due to leaked thread pool warning.
            // Currently {@link IndexGraphBuilder} is using the default ForkJoinPool.commonPool() which is not being shutdown.
        }
    }
}

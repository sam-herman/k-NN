/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.opensearch.Version;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.VectorField;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.engine.KNNMethodConfigContext;
import org.opensearch.knn.index.engine.KNNMethodContext;
import org.opensearch.knn.index.engine.MethodComponentContext;
import org.opensearch.knn.index.mapper.CompressionLevel;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.knn.index.mapper.KNNVectorFieldType;
import org.opensearch.knn.index.mapper.Mode;
import org.opensearch.knn.index.memory.NativeMemoryLoadStrategy;
import org.opensearch.knn.index.query.BaseQueryFactory;
import org.opensearch.knn.index.query.KNNQuery;
import org.opensearch.knn.index.query.KNNQueryFactory;
import org.opensearch.knn.index.query.KNNWeight;
import org.opensearch.knn.indices.*;
import org.opensearch.knn.jni.JNIService;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.opensearch.Version.CURRENT;
import static org.opensearch.knn.common.KNNConstants.*;
import static org.opensearch.knn.index.KNNSettings.MODEL_CACHE_SIZE_LIMIT_SETTING;

/**
 * Test used specifically for JVector
 */
public class KNNJVectorTests extends KNNTestCase {

    @Test
    public void testJVectorKnnIndex() throws IOException {
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir);) {
            // Note: even though a field was added, it doesn't participate in the formulation of the histogram
            // It's still there just to demonstrate that the histogram is formulated correctly and ignores other fields than the range field specified
            try (IndexReader reader = w.getReader()) {
                final float[] target = new float[]{1.0f, 1.0f};
                final Query filterQuery = new MatchAllDocsQuery();
                final IndexSearcher searcher = newSearcher(reader);
                KNNQuery knnQuery = new KNNQuery("test_field", target, 10, "myIndex", null);
                TopDocs topDocs = searcher.search(knnQuery, 10);
                assertEquals(10, topDocs.totalHits.value);
            }
        }
    }
}

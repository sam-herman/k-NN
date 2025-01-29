/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.After;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.knn.KNNRestTestCase;
import org.opensearch.knn.KNNResult;
import org.opensearch.knn.TestUtils;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import static org.opensearch.knn.common.KNNConstants.*;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 1)
public class JVectorEngineIT extends KNNRestTestCase {

    private static final int DIMENSION = 3;
    private static final String DOC_ID = "doc1";
    private static final String DOC_ID_2 = "doc2";
    private static final String DOC_ID_3 = "doc3";
    private static final int EF_CONSTRUCTION = 128;
    private static final String COLOR_FIELD_NAME = "color";
    private static final String TASTE_FIELD_NAME = "taste";
    private static final int M = 16;

    private static final Float[][] TEST_INDEX_VECTORS = { { 1.0f, 1.0f, 1.0f }, { 2.0f, 2.0f, 2.0f }, { 3.0f, 3.0f, 3.0f } };
    private static final Float[][] TEST_COSINESIMIL_INDEX_VECTORS = { { 6.0f, 7.0f, 3.0f }, { 3.0f, 2.0f, 5.0f }, { 4.0f, 5.0f, 7.0f } };
    private static final Float[][] TEST_INNER_PRODUCT_INDEX_VECTORS = {
        { 1.0f, 1.0f, 1.0f },
        { 2.0f, 2.0f, 2.0f },
        { 3.0f, 3.0f, 3.0f },
        { -1.0f, -1.0f, -1.0f },
        { -2.0f, -2.0f, -2.0f },
        { -3.0f, -3.0f, -3.0f } };

    private static final float[][] TEST_QUERY_VECTORS = { { 1.0f, 1.0f, 1.0f }, { 2.0f, 2.0f, 2.0f }, { 3.0f, 3.0f, 3.0f } };

    private static final Map<KNNVectorSimilarityFunction, Function<Float, Float>> VECTOR_SIMILARITY_TO_SCORE = ImmutableMap.of(
        KNNVectorSimilarityFunction.EUCLIDEAN,
        (similarity) -> 1 / (1 + similarity),
        KNNVectorSimilarityFunction.DOT_PRODUCT,
        (similarity) -> (1 + similarity) / 2,
        KNNVectorSimilarityFunction.COSINE,
        (similarity) -> (1 + similarity) / 2,
        KNNVectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
        (similarity) -> similarity <= 0 ? 1 / (1 - similarity) : similarity + 1
    );
    private static final String DIMENSION_FIELD_NAME = "dimension";
    private static final String KNN_VECTOR_TYPE = "knn_vector";
    private static final String PROPERTIES_FIELD_NAME = "properties";
    private static final String TYPE_FIELD_NAME = "type";
    private static final String INTEGER_FIELD_NAME = "int_field";
    private static final String FILED_TYPE_INTEGER = "integer";
    private static final String NON_EXISTENT_INTEGER_FIELD_NAME = "nonexistent_int_field";

    @After
    public final void cleanUp() throws IOException {
        deleteKNNIndex(INDEX_NAME);
    }

    public void testQuery_l2() throws Exception {
        baseQueryTest(SpaceType.L2);
    }

    public void testQuery_cosine() throws Exception {
        baseQueryTest(SpaceType.COSINESIMIL);
    }

    public void testAddDoc() throws Exception {
        List<Integer> mValues = ImmutableList.of(16, 32, 64, 128);
        List<Integer> efConstructionValues = ImmutableList.of(16, 32, 64, 128);

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(PROPERTIES_FIELD_NAME)
            .startObject(FIELD_NAME)
            .field(TYPE_FIELD_NAME, KNN_VECTOR_TYPE)
            .field(DIMENSION_FIELD_NAME, DIMENSION)
            .startObject(KNNConstants.KNN_METHOD)
            .field(KNNConstants.NAME, DISK_ANN)
            .field(KNNConstants.METHOD_PARAMETER_SPACE_TYPE, SpaceType.L2.getValue())
            .field(KNNConstants.KNN_ENGINE, KNNEngine.JVECTOR.getName())
            .startObject(KNNConstants.PARAMETERS)
            .field(KNNConstants.METHOD_PARAMETER_M, mValues.get(random().nextInt(mValues.size())))
            .field(KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION, efConstructionValues.get(random().nextInt(efConstructionValues.size())))
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> mappingMap = xContentBuilderToMap(builder);
        String mapping = builder.toString();

        createKnnIndex(INDEX_NAME, mapping);
        assertEquals(new TreeMap<>(mappingMap), new TreeMap<>(getIndexMappingAsMap(INDEX_NAME)));

        Float[] vector = new Float[] { 2.0f, 4.5f, 6.5f };
        addKnnDoc(INDEX_NAME, DOC_ID, FIELD_NAME, vector);

        refreshIndex(INDEX_NAME);
        assertEquals(1, getDocCount(INDEX_NAME));
    }

    private void createKnnIndexMappingWithJVectorEngine(int dimension, SpaceType spaceType, VectorDataType vectorDataType) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(PROPERTIES_FIELD_NAME)
            .startObject(FIELD_NAME)
            .field(TYPE_FIELD_NAME, KNN_VECTOR_TYPE)
            .field(DIMENSION_FIELD_NAME, dimension)
            .field(VECTOR_DATA_TYPE_FIELD, vectorDataType)
            .startObject(KNNConstants.KNN_METHOD)
            .field(KNNConstants.NAME, DISK_ANN)
            .field(KNNConstants.METHOD_PARAMETER_SPACE_TYPE, spaceType.getValue())
            .field(KNNConstants.KNN_ENGINE, KNNEngine.JVECTOR.getName())
            .startObject(KNNConstants.PARAMETERS)
            .field(KNNConstants.METHOD_PARAMETER_M, M)
            .field(KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION, EF_CONSTRUCTION)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        String mapping = builder.toString();
        createKnnIndex(INDEX_NAME, mapping);
    }

    private void baseQueryTest(SpaceType spaceType) throws Exception {

        createKnnIndexMappingWithJVectorEngine(DIMENSION, spaceType, VectorDataType.FLOAT);
        for (int j = 0; j < TEST_INDEX_VECTORS.length; j++) {
            addKnnDoc(INDEX_NAME, Integer.toString(j + 1), FIELD_NAME, TEST_INDEX_VECTORS[j]);
        }

        validateQueries(spaceType, FIELD_NAME);
        validateQueries(spaceType, FIELD_NAME, Map.of("ef_search", 100));
    }

    private void validateQueries(SpaceType spaceType, String fieldName) throws Exception {
        validateQueries(spaceType, fieldName, null);
    }

    private void validateQueries(SpaceType spaceType, String fieldName, Map<String, ?> methodParameters) throws Exception {

        int k = JVectorEngineIT.TEST_INDEX_VECTORS.length;
        for (float[] queryVector : TEST_QUERY_VECTORS) {
            Response response = searchKNNIndex(INDEX_NAME, buildSearchQuery(fieldName, k, queryVector, methodParameters), k);
            String responseBody = EntityUtils.toString(response.getEntity());
            List<KNNResult> knnResults = parseSearchResponse(responseBody, fieldName);
            assertEquals(k, knnResults.size());

            List<Float> actualScores = parseSearchResponseScore(responseBody, fieldName);
            for (int j = 0; j < k; j++) {
                float[] primitiveArray = knnResults.get(j).getVector();
                float distance = TestUtils.computeDistFromSpaceType(spaceType, primitiveArray, queryVector);
                float rawScore = VECTOR_SIMILARITY_TO_SCORE.get(spaceType.getKnnVectorSimilarityFunction()).apply(distance);
                assertEquals(KNNEngine.LUCENE.score(rawScore, spaceType), actualScores.get(j), 0.0001);
            }
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.github.jbellis.jvector.graph.disk;

import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class OnDiskGraphIndexForLucene extends OnDiskGraphIndex {
    OnDiskGraphIndexForLucene(ReaderSupplier readerSupplier, Header header, long neighborsOffset) {
        super(readerSupplier, header, neighborsOffset);
    }

    /*
    public static void writeForLucene(GraphIndex graph, RandomAccessVectorValues vectors, IndexOutput out) throws IOException {
        writeForLucene(graph, vectors, OnDiskGraphIndexWriter.sequentialRenumbering(graph), out);
    }

    public static void writeForLucene(GraphIndex graph,
                                      RandomAccessVectorValues vectors,
                                      Map<Integer, Integer> oldToNewOrdinals,
                                      IndexOutput out)
            throws IOException
    {
        try (var writer = new OnDiskGraphIndexWriterForLucene.Builder(graph, out).withMap(oldToNewOrdinals)
                .with(new InlineVectors(vectors.dimension()))
                .build())
        {
            var suppliers = Feature.singleStateFactory(FeatureId.INLINE_VECTORS,
                    nodeId -> new InlineVectors.State(vectors.getVector(nodeId)));
            writer.write(suppliers);
        }
    }
     */
}

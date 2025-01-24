/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.jvector;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.*;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;


@Log4j2
public class JVectorWriter extends KnnVectorsWriter {
    private static final VectorTypeSupport VECTOR_TYPE_SUPPORT = VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final long SHALLOW_RAM_BYTES_USED =
            RamUsageEstimator.shallowSizeOfInstance(JVectorWriter.class);
    private final List<JVectorWriter.FieldWriter<?>> fields = new ArrayList<>();

    private final IndexOutput meta;
    private final IndexOutput vectorIndex;
    private final FlatVectorsWriter flatVectorWriter;
    private final String indexDataFileName;
    private boolean finished = false;

    public JVectorWriter(SegmentWriteState state, FlatVectorsWriter flatVectorWriter) throws IOException {
        this.flatVectorWriter = flatVectorWriter;

        String metaFileName =
                IndexFileNames.segmentFileName(
                        state.segmentInfo.name, state.segmentSuffix, JVectorFormat.META_EXTENSION);

        this.indexDataFileName =
                IndexFileNames.segmentFileName(
                        state.segmentInfo.name,
                        state.segmentSuffix,
                        JVectorFormat.VECTOR_INDEX_EXTENSION);


        boolean success = false;
        try {
            meta = state.directory.createOutput(metaFileName, state.context);
            vectorIndex = state.directory.createOutput(indexDataFileName, state.context);
            CodecUtil.writeIndexHeader(
                    meta,
                    JVectorFormat.META_CODEC_NAME,
                    JVectorFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix);
            CodecUtil.writeIndexHeader(
                    vectorIndex,
                    JVectorFormat.VECTOR_INDEX_CODEC_NAME,
                    JVectorFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix);

            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        JVectorWriter.FieldWriter<?> newField =
                JVectorWriter.FieldWriter.create(flatVectorWriter.getFlatVectorScorer(),
                        flatVectorWriter.addField(fieldInfo),
                        fieldInfo);
        fields.add(newField);
        return newField;
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        flatVectorWriter.flush(maxDoc, sortMap);
        for (JVectorWriter.FieldWriter<?> field : fields) {
            if (sortMap == null) {
                writeField(field);
            } else {
                throw new UnsupportedOperationException("Not implemented yet");
                //writeSortingField(field, sortMap);
            }
        }
    }

    private void writeField(JVectorWriter.FieldWriter<?> fieldData) throws IOException {
        // write graph
        long vectorIndexOffset = vectorIndex.getFilePointer();
        OnHeapGraphIndex graph = fieldData.getGraph();
        int[][] graphLevelNodeOffsets = writeGraph(graph, fieldData.randomAccessVectorValues);
        long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;

        /*
        writeMeta(
                fieldData.fieldInfo,
                vectorIndexOffset,
                vectorIndexLength,
                fieldData.getDocsWithFieldSet().cardinality(),
                graph,
                graphLevelNodeOffsets);

         */
    }


    /*
    private void writeMeta(
            FieldInfo field,
            long vectorIndexOffset,
            long vectorIndexLength,
            int count,
            OnHeapGraphIndex graph,
            int[][] graphLevelNodeOffsets)
            throws IOException {
        meta.writeInt(field.number);
        meta.writeInt(field.getVectorEncoding().ordinal());
        meta.writeInt(distFuncToOrd(field.getVectorSimilarityFunction()));
        meta.writeVLong(vectorIndexOffset);
        meta.writeVLong(vectorIndexLength);
        meta.writeVInt(field.getVectorDimension());
        meta.writeInt(count);
        // write graph nodes on each level
        if (graph == null) {
            meta.writeVInt(M);
            meta.writeVInt(0);
        } else {
            meta.writeVInt(graph.maxConn());
            meta.writeVInt(graph.numLevels());
            long valueCount = 0;
            for (int level = 0; level < graph.numLevels(); level++) {
                HnswGraph.NodesIterator nodesOnLevel = graph.getNodesOnLevel(level);
                valueCount += nodesOnLevel.size();
                if (level > 0) {
                    int[] nol = new int[nodesOnLevel.size()];
                    int numberConsumed = nodesOnLevel.consume(nol);
                    Arrays.sort(nol);
                    assert numberConsumed == nodesOnLevel.size();
                    meta.writeVInt(nol.length); // number of nodes on a level
                    for (int i = nodesOnLevel.size() - 1; i > 0; --i) {
                        nol[i] -= nol[i - 1];
                    }
                    for (int n : nol) {
                        assert n >= 0 : "delta encoding for nodes failed; expected nodes to be sorted";
                        meta.writeVInt(n);
                    }
                } else {
                    assert nodesOnLevel.size() == count : "Level 0 expects to have all nodes";
                }
            }
            long start = vectorIndex.getFilePointer();
            meta.writeLong(start);
            meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
            final DirectMonotonicWriter memoryOffsetsWriter =
                    DirectMonotonicWriter.getInstance(
                            meta, vectorIndex, valueCount, DIRECT_MONOTONIC_BLOCK_SHIFT);
            long cumulativeOffsetSum = 0;
            for (int[] levelOffsets : graphLevelNodeOffsets) {
                for (int v : levelOffsets) {
                    memoryOffsetsWriter.add(cumulativeOffsetSum);
                    cumulativeOffsetSum += v;
                }
            }
            memoryOffsetsWriter.finish();
            meta.writeLong(vectorIndex.getFilePointer() - start);
        }
    }
    */

    // TODO: implement this for proper return type
    private int[][] writeGraph(OnHeapGraphIndex graph, RandomAccessVectorValues ravv) throws IOException {
        // TODO: use the vector index inputStream instead of this!
        final Path jvecFilePath = Paths.get("/Users/sam.herman/projects/k-NN/build/tmp/", indexDataFileName);
        Files.deleteIfExists(jvecFilePath);
        Path indexPath = Files.createFile(jvecFilePath);
        log.info("Writing graph to {}", indexPath);
        OnDiskGraphIndex.write(graph, ravv, indexPath);

        return null;
    }

    static int distFuncToOrd(VectorSimilarityFunction func) {
        for (int i = 0; i < SIMILARITY_FUNCTIONS.size(); i++) {
            if (SIMILARITY_FUNCTIONS.get(i).equals(func)) {
                return (byte) i;
            }
        }
        throw new IllegalArgumentException("invalid distance function: " + func);
    }

    @Override
    public void finish() throws IOException {
        if (finished) {
            throw new IllegalStateException("already finished");
        }
        finished = true;
        flatVectorWriter.finish();

        if (meta != null) {
            // write end of fields marker
            meta.writeInt(-1);
            CodecUtil.writeFooter(meta);
        }
        if (vectorIndex != null) {
            CodecUtil.writeFooter(vectorIndex);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(meta, vectorIndex, flatVectorWriter);
    }

    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_RAM_BYTES_USED;
        for (JVectorWriter.FieldWriter<?> field : fields) {
            // the field tracks the delegate field usage
            total += field.ramBytesUsed();
        }
        return total;
    }

    static class FieldWriter<T> extends KnnFieldVectorsWriter<T> {
        private static final long SHALLOW_SIZE =
                RamUsageEstimator.shallowSizeOfInstance(JVectorWriter.FieldWriter.class);
        private final FieldInfo fieldInfo;
        private int lastDocID = -1;
        private final FlatFieldVectorsWriter<T> flatFieldVectorsWriter;
        private GraphIndexBuilder graphIndexBuilder;
        RandomAccessVectorValues randomAccessVectorValues;

        static FieldWriter<?> create(
                FlatVectorsScorer scorer,
                FlatFieldVectorsWriter<?> flatFieldVectorsWriter,
                FieldInfo fieldInfo)
                throws IOException {
            switch (fieldInfo.getVectorEncoding()) {
                case BYTE:
                    return new FieldWriter<>(
                        scorer,
                        (FlatFieldVectorsWriter<byte[]>) flatFieldVectorsWriter,
                        fieldInfo);
                case FLOAT32:
                    return new FieldWriter<>(
                        scorer,
                        (FlatFieldVectorsWriter<float[]>) flatFieldVectorsWriter,
                        fieldInfo);
                default:
                    throw new IllegalArgumentException("Unsupported vector encoding: " + fieldInfo.getVectorEncoding());
            }
        }

        FieldWriter(
                FlatVectorsScorer scorer,
                FlatFieldVectorsWriter<T> flatFieldVectorsWriter,
                FieldInfo fieldInfo) {
            this.fieldInfo = fieldInfo;
            this.flatFieldVectorsWriter = Objects.requireNonNull(flatFieldVectorsWriter);
        }

        @Override
        public void addValue(int docID, T vectorValue) throws IOException {
            if (docID == lastDocID) {
                throw new IllegalArgumentException(
                        "VectorValuesField \""
                                + fieldInfo.name
                                + "\" appears more than once in this document (only one value is allowed per field)");
            }
            flatFieldVectorsWriter.addValue(docID, vectorValue);
            /*
            var floats = (float[]) vectorValue;
            var vector = (VECTOR_TYPE_SUPPORT.createFloatVector(floats));
            graphIndexBuilder.addGraphNode(docID, vector);

             */
            lastDocID = docID;
        }

        @Override
        public T copyValue(T vectorValue) {
            return null;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE
                    + flatFieldVectorsWriter.ramBytesUsed()
                    + (Objects.isNull(graphIndexBuilder) ?  0 : graphIndexBuilder.getGraph().ramBytesUsed());
        }

        static io.github.jbellis.jvector.vector.VectorSimilarityFunction getVectorSimilarityFunction(FieldInfo fieldInfo) {
            switch (fieldInfo.getVectorSimilarityFunction()) {
                case EUCLIDEAN:
                    return io.github.jbellis.jvector.vector.VectorSimilarityFunction.EUCLIDEAN;
                case COSINE:
                    return io.github.jbellis.jvector.vector.VectorSimilarityFunction.COSINE;
                default:
                    throw new IllegalArgumentException("Unsupported similarity function: " + fieldInfo.getVectorSimilarityFunction());
            }
        }

        /**
         * This method will lazily initialize the graph. This is recommended to be called only during the flush stage
         * @return OnHeapGraphIndex OnHeapGraphIndex
         * @throws IOException IOException
         */
        public OnHeapGraphIndex getGraph() throws IOException {
            final BuildScoreProvider bsp;
            switch (fieldInfo.getVectorEncoding()) {
                case BYTE:
                        /*
                        * TODO: Does JVector support ByteVectorValues?
                        scorer.getRandomVectorScorerSupplier(
                                fieldInfo.getVectorSimilarityFunction(),

                                ByteVectorValues.fromBytes(
                                        (List<byte[]>) flatFieldVectorsWriter.getVectors(),
                                        fieldInfo.getVectorDimension()));
                        */
                    throw new UnsupportedOperationException("ByteVectorValues not supported yet");
                case FLOAT32:
                    // score provider using the raw, in-memory vectors
                    var vectors = new ArrayList<VectorFloat<?>>();
                    var floats = (List<float[]>) flatFieldVectorsWriter.getVectors();
                    var originalDimension = fieldInfo.getVectorDimension();
                    assert originalDimension == floats.get(0).length;
                    for (var f : floats) {
                        vectors.add(VECTOR_TYPE_SUPPORT.createFloatVector(f));
                    }
                    randomAccessVectorValues = new ListRandomAccessVectorValues(vectors, originalDimension);
                    bsp = BuildScoreProvider.randomAccessScoreProvider(randomAccessVectorValues, getVectorSimilarityFunction(fieldInfo));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported vector encoding: " + fieldInfo.getVectorEncoding());

            }
            this.graphIndexBuilder = new GraphIndexBuilder(bsp,
                    randomAccessVectorValues.dimension(),
                    16, // graph degree
                    100, // construction search depth
                    1.2f, // allow degree overflow during construction by this factor
                    1.2f);
            return graphIndexBuilder.build(randomAccessVectorValues);
        }
    }
}

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
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.*;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;


@Log4j2
public class JVectorWriter extends KnnVectorsWriter {
    private static final long SHALLOW_RAM_BYTES_USED =
            RamUsageEstimator.shallowSizeOfInstance(JVectorWriter.class);
    private final List<JVectorWriter.FieldWriter<?>> fields = new ArrayList<>();

    private final IndexOutput meta;
    private final IndexOutput vectorIndex;
    private final String indexDataFileName;
    private final String baseDataFileName;
    private final FlatVectorsWriter flatVectorWriter;
    private final Path directoryBasePath;
    private final SegmentWriteState segmentWriteState;
    private boolean finished = false;


    public JVectorWriter(SegmentWriteState segmentWriteState, FlatVectorsWriter flatVectorWriter) throws IOException {
        this.flatVectorWriter = flatVectorWriter;
        this.segmentWriteState = segmentWriteState;
        String metaFileName =
                IndexFileNames.segmentFileName(
                        segmentWriteState.segmentInfo.name, segmentWriteState.segmentSuffix, JVectorFormat.META_EXTENSION);

        this.indexDataFileName =
                IndexFileNames.segmentFileName(
                        segmentWriteState.segmentInfo.name,
                        segmentWriteState.segmentSuffix,
                        JVectorFormat.VECTOR_INDEX_EXTENSION);
        this.baseDataFileName = segmentWriteState.segmentInfo.name + "_" + segmentWriteState.segmentSuffix;

        Directory dir = segmentWriteState.directory;
        while (!(dir instanceof FSDirectory)) {
            if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                throw new IllegalArgumentException("directory must be FSDirectory or a wrapper around it");
            }
        }

        var fsDir = (FSDirectory)dir;
        this.directoryBasePath = fsDir.getDirectory();

        boolean success = false;
        try {
            meta = segmentWriteState.directory.createOutput(metaFileName, segmentWriteState.context);
            vectorIndex = segmentWriteState.directory.createOutput(indexDataFileName, segmentWriteState.context);
            CodecUtil.writeIndexHeader(
                    meta,
                    JVectorFormat.META_CODEC_NAME,
                    JVectorFormat.VERSION_CURRENT,
                    segmentWriteState.segmentInfo.getId(),
                    segmentWriteState.segmentSuffix);

            CodecUtil.writeIndexHeader(
                    vectorIndex,
                    JVectorFormat.VECTOR_INDEX_CODEC_NAME,
                    JVectorFormat.VERSION_CURRENT,
                    segmentWriteState.segmentInfo.getId(),
                    segmentWriteState.segmentSuffix);

            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        log.info("Adding field {} in segment {}", fieldInfo.name, segmentWriteState.segmentInfo.name);
        JVectorWriter.FieldWriter<?> newField =
                JVectorWriter.FieldWriter.create(flatVectorWriter.getFlatVectorScorer(),
                        flatVectorWriter.addField(fieldInfo),
                        fieldInfo,
                        segmentWriteState);
        fields.add(newField);
        return newField;
    }

    @Override
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        super.mergeOneField(fieldInfo, mergeState);
        log.info("Merging field {} in segment {}", fieldInfo.name, segmentWriteState.segmentInfo.name);
        //flatVectorWriter.mergeOneField(fieldInfo, mergeState);
        var scorerSupplier = flatVectorWriter.mergeOneFieldToIndex(fieldInfo, mergeState);
        var success = false;
        try {
            switch (fieldInfo.getVectorEncoding()) {
                case BYTE:
                    var byteWriter =
                            (JVectorWriter.FieldWriter<byte[]>) addField(fieldInfo);
                    ByteVectorValues mergedBytes =
                            MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState);
                    for (int doc = mergedBytes.nextDoc();
                         doc != DocIdSetIterator.NO_MORE_DOCS;
                         doc = mergedBytes.nextDoc()) {
                        byteWriter.addValue(doc, mergedBytes.vectorValue());
                    }
                    writeField(byteWriter);
                    break;
                case FLOAT32:
                    var floatVectorFieldWriter =
                            (JVectorWriter.FieldWriter<float[]>) addField(fieldInfo);
                    FloatVectorValues mergedFloats =
                            MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
                    for (int doc = mergedFloats.nextDoc();
                         doc != DocIdSetIterator.NO_MORE_DOCS;
                         doc = mergedFloats.nextDoc()) {
                        floatVectorFieldWriter.addValue(doc, mergedFloats.vectorValue());
                    }
                    writeField(floatVectorFieldWriter);
                    break;
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(scorerSupplier);
            } else {
                IOUtils.closeWhileHandlingException(scorerSupplier);
            }
        }
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        log.info("Flushing {} fields", fields.size());
        flatVectorWriter.flush(maxDoc, sortMap);
        log.info("flatVectorWriter flushed");

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
        int[][] graphLevelNodeOffsets = writeGraph(graph, fieldData);
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
    private int[][] writeGraph(OnHeapGraphIndex graph, FieldWriter<?> fieldData) throws IOException {
        // TODO: use the vector index inputStream instead of this!
        final Path jvecFilePath = JVectorFormat.getVectorIndexPath(directoryBasePath, baseDataFileName, fieldData.fieldInfo.name);
        /** This is an ugly hack to make sure Lucene actually knows about our input stream files, otherwise it will delete them */
        IndexOutput indexOutput = segmentWriteState.directory.createOutput(jvecFilePath.getFileName().toString(), segmentWriteState.context);
        indexOutput.close();
        Files.deleteIfExists(jvecFilePath);
        /** End of ugly hack */

        Path indexPath = Files.createFile(jvecFilePath);
        log.info("Writing graph to {}", indexPath);
        OnDiskGraphIndex.write(graph, fieldData.randomAccessVectorValues, indexPath);

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
        private static final VectorTypeSupport VECTOR_TYPE_SUPPORT = VectorizationProvider.getInstance().getVectorTypeSupport();
        private static final long SHALLOW_SIZE =
                RamUsageEstimator.shallowSizeOfInstance(JVectorWriter.FieldWriter.class);
        @Getter
        private final FieldInfo fieldInfo;
        private int lastDocID = -1;
        private final FlatFieldVectorsWriter<T> flatFieldVectorsWriter;
        private GraphIndexBuilder graphIndexBuilder;
        private final List<VectorFloat<?>> floatVectors = new ArrayList<>();
        private final String segmentName;
        RandomAccessVectorValues randomAccessVectorValues;



        static FieldWriter<?> create(
                FlatVectorsScorer scorer,
                FlatFieldVectorsWriter<?> flatFieldVectorsWriter,
                FieldInfo fieldInfo,
                SegmentWriteState segmentWriteState)
                throws IOException {
            switch (fieldInfo.getVectorEncoding()) {
                case BYTE:
                    return new FieldWriter<>(
                        scorer,
                        (FlatFieldVectorsWriter<byte[]>) flatFieldVectorsWriter,
                        fieldInfo,
                        segmentWriteState.segmentInfo.name);
                case FLOAT32:
                    return new FieldWriter<>(
                        scorer,
                        (FlatFieldVectorsWriter<float[]>) flatFieldVectorsWriter,
                        fieldInfo,
                        segmentWriteState.segmentInfo.name);
                default:
                    throw new IllegalArgumentException("Unsupported vector encoding: " + fieldInfo.getVectorEncoding());
            }
        }

        FieldWriter(
                FlatVectorsScorer scorer,
                FlatFieldVectorsWriter<T> flatFieldVectorsWriter,
                FieldInfo fieldInfo,
                String segmentName) {
            this.fieldInfo = fieldInfo;
            this.flatFieldVectorsWriter = Objects.requireNonNull(flatFieldVectorsWriter);
            this.segmentName = segmentName;
        }

        @Override
        public void addValue(int docID, T vectorValue) throws IOException {
            log.info("Adding value {} to field {} in segment {}", vectorValue, fieldInfo.name, segmentName);
            if (docID == lastDocID) {
                throw new IllegalArgumentException(
                        "VectorValuesField \""
                                + fieldInfo.name
                                + "\" appears more than once in this document (only one value is allowed per field)");
            }
            floatVectors.add(VECTOR_TYPE_SUPPORT.createFloatVector(vectorValue));
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
            throw new UnsupportedOperationException("copyValue not supported");
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
                    var originalDimension = fieldInfo.getVectorDimension();
                    randomAccessVectorValues = new ListRandomAccessVectorValues(floatVectors, originalDimension);
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

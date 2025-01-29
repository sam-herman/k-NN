/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.jvector;

import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.*;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.DirectMonotonicReader;

import io.github.jbellis.jvector.disk.ReaderSupplierFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.opensearch.knn.index.codec.jvector.JVectorWriter.FieldWriter.getVectorSimilarityFunction;

@Log4j2
public class JVectorReader extends KnnVectorsReader {
    private static final VectorTypeSupport VECTOR_TYPE_SUPPORT = VectorizationProvider.getInstance().getVectorTypeSupport();

    final FlatVectorsReader flatVectorsReader;
    private final FieldInfos fieldInfos;
    private final String indexDataFileName;
    private final String baseDataFileName;
    private final Path directoryBasePath;
    //private final IntObjectHashMap<JVectorReader.FieldEntry> fields;

    public JVectorReader(SegmentReadState state, FlatVectorsReader flatVectorsReader) throws IOException {
        this.flatVectorsReader = flatVectorsReader;
        this.fieldInfos = state.fieldInfos;
        this.baseDataFileName = state.segmentInfo.name + "_" + state.segmentSuffix;
        String metaFileName =
                IndexFileNames.segmentFileName(
                        state.segmentInfo.name, state.segmentSuffix, JVectorFormat.META_EXTENSION);

        Directory dir = state.directory;
        while (!(dir instanceof FSDirectory)) {
            final String dirType = dir.getClass().getName();
            log.info("unwrapping dir of type: {} to find path", dirType);
            if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                throw new IllegalArgumentException("directory must be FSDirectory or a wrapper around it but instead had type: " + dirType);
            }
        }

        var fsDir = (FSDirectory)dir;
        this.directoryBasePath = fsDir.getDirectory();

        boolean success = false;
        try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName, state.context)) {
            CodecUtil.checkIndexHeader(
                    meta,
                    JVectorFormat.META_CODEC_NAME,
                    JVectorFormat.VERSION_START,
                    JVectorFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix);
            //readFields(meta);

            this.indexDataFileName =
                    IndexFileNames.segmentFileName(
                            state.segmentInfo.name,
                            state.segmentSuffix,
                            JVectorFormat.VECTOR_INDEX_EXTENSION);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public void checkIntegrity() throws IOException {
        flatVectorsReader.checkIntegrity();
        // TODO: Implement this, for now this will always pass
        //CodecUtil.checksumEntireFile(vectorIndex);
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return flatVectorsReader.getFloatVectorValues(field);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return flatVectorsReader.getByteVectorValues(field);
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
        /* TODO: remove this
                *** score provider using the raw, in-memory vectors ***
        FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        FloatVectorValues floatVectorValues = flatVectorsReader.getFloatVectorValues(field);
        var vectors = new ArrayList<VectorFloat<?>>(flatVectorsReader.getFloatVectorValues(field).size());
        while (floatVectorValues.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            vectors.add(VECTOR_TYPE_SUPPORT.createFloatVector(floatVectorValues.vectorValue()));
        }

        var originalDimension = fieldInfo.getVectorDimension();
        assert originalDimension == vectors.get(0).length();
        RandomAccessVectorValues randomAccessVectorValues = new ListRandomAccessVectorValues(vectors, originalDimension)
         */

        final Path jvecFilePath = JVectorFormat.getVectorIndexPath(directoryBasePath, baseDataFileName, field);
        // on-disk indexes require a ReaderSupplier (not just a Reader) because we will want it to
        // open additional readers for searching
        try (ReaderSupplier rs = ReaderSupplierFactory.open(jvecFilePath)) {
            OnDiskGraphIndex index = OnDiskGraphIndex.load(rs);

            // search for a random vector using a GraphSearcher and SearchScoreProvider
            VectorFloat<?> q = VECTOR_TYPE_SUPPORT.createFloatVector(target);
            try (GraphSearcher searcher = new GraphSearcher(index)) {
                SearchScoreProvider ssp = SearchScoreProvider.exact(q, io.github.jbellis.jvector.vector.VectorSimilarityFunction.EUCLIDEAN, index.getView());
                SearchResult sr = searcher.search(ssp, knnCollector.k(), io.github.jbellis.jvector.util.Bits.ALL);
                for (SearchResult.NodeScore ns : sr.getNodes()) {
                    knnCollector.collect(ns.node, ns.score);
                }
            }
        }

    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
        // TODO: implement this
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(flatVectorsReader);
        // TODO: also close the vectorIndex
        //IOUtils.close(vectorIndex);
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    /*
    private void readFields(ChecksumIndexInput meta) throws IOException {
        for (FieldInfo info : fieldInfos) {
            if (info.hasVectors()) {
                final FieldEntry fieldEntry = readField(meta, info);
                //validateFieldEntry(info, fieldEntry);
                fields.put(info.number, fieldEntry);

            }
        }
    }

    private FieldEntry readField(IndexInput input, FieldInfo info) throws IOException {
        VectorEncoding vectorEncoding = readVectorEncoding(input);
        VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
        if (similarityFunction != info.getVectorSimilarityFunction()) {
            throw new IllegalStateException(
                    "Inconsistent vector similarity function for field=\""
                            + info.name
                            + "\"; "
                            + similarityFunction
                            + " != "
                            + info.getVectorSimilarityFunction());
        }
        return FieldEntry.create(input, vectorEncoding, info.getVectorSimilarityFunction());
    }
    */
    static class FieldEntry {
            VectorSimilarityFunction similarityFunction;
            VectorEncoding vectorEncoding;
            long vectorIndexOffset;
            long vectorIndexLength;
            int M;
            int numLevels;
            int dimension;
            int size;
            int[][] nodesByLevel;
            // for each level the start offsets in vectorIndex file from where to read neighbours
            DirectMonotonicReader.Meta offsetsMeta;
            long offsetsOffset;
            int offsetsBlockShift;
            long offsetsLength;

        FieldEntry(
                VectorSimilarityFunction similarityFunction,
                VectorEncoding vectorEncoding,
                long vectorIndexOffset,
                long vectorIndexLength,
                int M,
                int numLevels,
                int dimension,
                int size,
                int[][] nodesByLevel,
                DirectMonotonicReader.Meta offsetsMeta,
                long offsetsOffset,
                int offsetsBlockShift,
                long offsetsLength) {
            this.similarityFunction = similarityFunction;
            this.vectorEncoding = vectorEncoding;
            this.vectorIndexOffset = vectorIndexOffset;
            this.vectorIndexLength = vectorIndexLength;
            this.M = M;
            this.numLevels = numLevels;
            this.dimension = dimension;
            this.size = size;
            this.nodesByLevel = nodesByLevel;
            this.offsetsMeta = offsetsMeta;
            this.offsetsOffset = offsetsOffset;
            this.offsetsBlockShift = offsetsBlockShift;
            this.offsetsLength = offsetsLength;
        }

        static FieldEntry create(
                IndexInput input,
                VectorEncoding vectorEncoding,
                VectorSimilarityFunction similarityFunction)
        throws IOException {
            final var vectorIndexOffset = input.readVLong();
            final var vectorIndexLength = input.readVLong();
            final var dimension = input.readVInt();
            final var size = input.readInt();
            // read nodes by level
            final var M = input.readVInt();
            final var numLevels = input.readVInt();
            final var nodesByLevel = new int[numLevels][];
            long numberOfOffsets = 0;
            final long offsetsOffset;
            final int offsetsBlockShift;
            final DirectMonotonicReader.Meta offsetsMeta;
            final long offsetsLength;
            for (int level = 0; level < numLevels; level++) {
                if (level > 0) {
                    int numNodesOnLevel = input.readVInt();
                    numberOfOffsets += numNodesOnLevel;
                    nodesByLevel[level] = new int[numNodesOnLevel];
                    nodesByLevel[level][0] = input.readVInt();
                    for (int i = 1; i < numNodesOnLevel; i++) {
                        nodesByLevel[level][i] = nodesByLevel[level][i - 1] + input.readVInt();
                    }
                } else {
                    numberOfOffsets += size;
                }
            }
            if (numberOfOffsets > 0) {
                offsetsOffset = input.readLong();
                offsetsBlockShift = input.readVInt();
                offsetsMeta = DirectMonotonicReader.loadMeta(input, numberOfOffsets, offsetsBlockShift);
                offsetsLength = input.readLong();
            } else {
                offsetsOffset = 0;
                offsetsBlockShift = 0;
                offsetsMeta = null;
                offsetsLength = 0;
            }
            return new FieldEntry(
                    similarityFunction,
                    vectorEncoding,
                    vectorIndexOffset,
                    vectorIndexLength,
                    M,
                    numLevels,
                    dimension,
                    size,
                    nodesByLevel,
                    offsetsMeta,
                    offsetsOffset,
                    offsetsBlockShift,
                    offsetsLength);
        }
    }
}

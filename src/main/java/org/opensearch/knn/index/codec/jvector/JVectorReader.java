/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.jvector;

import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.*;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.disk.ReaderSupplierFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;
import static org.opensearch.knn.index.codec.jvector.JVectorWriter.FieldWriter.getVectorSimilarityFunction;

@Log4j2
public class JVectorReader extends KnnVectorsReader {
    private static final VectorTypeSupport VECTOR_TYPE_SUPPORT = VectorizationProvider.getInstance().getVectorTypeSupport();

    private final FieldInfos fieldInfos;
    private final String indexDataFileName;
    private final String baseDataFileName;
    private final Path directoryBasePath;
    // Maps field name to field entries
    private final Map<String, FieldEntry> fieldEntryMap = new HashMap<>(1);


    public JVectorReader(SegmentReadState state) throws IOException {
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
            readFields(meta);
            CodecUtil.checkFooter(meta);

            this.indexDataFileName =
                    IndexFileNames.segmentFileName(
                            state.segmentInfo.name,
                            state.segmentSuffix,
                            JVectorFormat.VECTOR_INDEX_EXTENSION);

            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public void checkIntegrity() throws IOException {
        // TODO: Implement this, for now this will always pass
        //CodecUtil.checksumEntireFile(vectorIndex);
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        // TODO: get the similarity function from field!! remove from here!
        return new JVectorFloatVectorValues(fieldEntryMap.get(field).index, io.github.jbellis.jvector.vector.VectorSimilarityFunction.EUCLIDEAN);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        //return flatVectorsReader.getByteVectorValues(field);
        return null;
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {

        // load the index
        // TODO: cache this
        final OnDiskGraphIndex index = fieldEntryMap.get(field).index;

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

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
        // TODO: implement this
    }

    @Override
    public void close() throws IOException {
        for (FieldEntry fieldEntry : fieldEntryMap.values()) {
            IOUtils.close(fieldEntry.readerSupplier::close);
        }
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }


    private void readFields(ChecksumIndexInput meta) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            final FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber); // read field number)
            final VectorEncoding vectorEncoding = readVectorEncoding(meta);
            final VectorSimilarityFunction similarityFunction = VectorSimilarityMapper.ordToDistFunc(meta.readInt());
            final long vectorIndexOffset = meta.readVLong();
            final long vectorIndexLength = meta.readVLong();
            final int dimension = meta.readVInt();
            fieldEntryMap.put(fieldInfo.name, new FieldEntry(fieldInfo, similarityFunction, vectorEncoding, vectorIndexOffset, vectorIndexLength, dimension));
        }
    }

    class FieldEntry {
        private final FieldInfo fieldInfo;
        private final VectorEncoding vectorEncoding;
        private final VectorSimilarityFunction similarityFunction;
        private final long vectorIndexOffset;
        private final long vectorIndexLength;
        private final int dimension;
        private final ReaderSupplier readerSupplier;
        private final OnDiskGraphIndex index;

        public FieldEntry(
                FieldInfo fieldInfo,
                VectorSimilarityFunction similarityFunction,
                VectorEncoding vectorEncoding,
                long vectorIndexOffset,
                long vectorIndexLength,
                int dimension) throws IOException {
            this.fieldInfo = fieldInfo;
            this.similarityFunction = similarityFunction;
            this.vectorEncoding = vectorEncoding;
            this.vectorIndexOffset = vectorIndexOffset;
            this.vectorIndexLength = vectorIndexLength;
            this.dimension = dimension;
            this.readerSupplier = ReaderSupplierFactory.open(JVectorFormat.getVectorIndexPath(directoryBasePath, baseDataFileName, fieldInfo.name));
            this.index = OnDiskGraphIndex.load(readerSupplier);
        }
    }


    /**
     * Utility class to map between Lucene and jVector similarity functions and metadata ordinals.
     */
    public static class VectorSimilarityMapper {
        /**
         List of vector similarity functions supported by <a href="https://github.com/jbellis/jvector">jVector library</a>
         The similarity functions orders matter in this list because it is later used to resolve the similarity function by ordinal.
         */
        public static final List<VectorSimilarityFunction> JVECTOR_SUPPORTED_SIMILARITY_FUNCTIONS =
                List.of(
                        VectorSimilarityFunction.EUCLIDEAN,
                        VectorSimilarityFunction.DOT_PRODUCT,
                        VectorSimilarityFunction.COSINE);

        public static final Map<org.apache.lucene.index.VectorSimilarityFunction, VectorSimilarityFunction> luceneToJVectorMap = Map.of(
                org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN, VectorSimilarityFunction.EUCLIDEAN,
                org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT, VectorSimilarityFunction.DOT_PRODUCT,
                org.apache.lucene.index.VectorSimilarityFunction.COSINE, VectorSimilarityFunction.COSINE
        );

        public static int distFuncToOrd(org.apache.lucene.index.VectorSimilarityFunction func) {
            if (luceneToJVectorMap.containsKey(func)) {
                return JVECTOR_SUPPORTED_SIMILARITY_FUNCTIONS.indexOf(luceneToJVectorMap.get(func));
            }

            throw new IllegalArgumentException("invalid distance function: " + func);
        }

        public static VectorSimilarityFunction ordToDistFunc(int ord) {
            return JVECTOR_SUPPORTED_SIMILARITY_FUNCTIONS.get(ord);
        }
    }

}

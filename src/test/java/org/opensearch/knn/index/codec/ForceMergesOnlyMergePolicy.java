/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec;

import org.apache.lucene.index.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ForceMergesOnlyMergePolicy extends MergePolicy {
    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
        return null;
    }

    @Override
    public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, MergeContext mergeContext) throws IOException {
        final List<SegmentCommitInfo> segments = segmentInfos.asList();
        MergeSpecification spec = new MergeSpecification();
        final OneMerge merge = new OneMerge(segments);
        spec.add(merge);
        return spec;
    }

    @Override
    public boolean useCompoundFile(SegmentInfos segmentInfos, SegmentCommitInfo newSegment, MergeContext mergeContext) throws IOException {
        return false;
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
        return null;
    }
}

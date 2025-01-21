/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.engine.jvector;

import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.engine.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JVector extends JVMLibrary  {
    /**
     * Constructor
     *
     * @param methods Map of k-NN methods that the library supports
     * @param version String representing version of library
     */
    public JVector(Map<String, KNNMethod> methods, String version) {
        super(methods, version);
    }

    public final static JVector INSTANCE = new JVector();

    @Override
    public String getExtension() {
        return "";
    }

    @Override
    public String getCompoundExtension() {
        return "";
    }

    @Override
    public float score(float rawScore, SpaceType spaceType) {
        return 0;
    }

    @Override
    public Float distanceToRadialThreshold(Float distance, SpaceType spaceType) {
        return 0f;
    }

    @Override
    public Float scoreToRadialThreshold(Float score, SpaceType spaceType) {
        return 0f;
    }

    @Override
    public ResolvedMethodContext resolveMethod(KNNMethodContext knnMethodContext, KNNMethodConfigContext knnMethodConfigContext, boolean shouldRequireTraining, SpaceType spaceType) {
        return null;
    }

    // TODO: add actual file suffix there
    @Override
    public List<String> mmapFileExtensions() {
        return Collections.emptyList();
    }
}

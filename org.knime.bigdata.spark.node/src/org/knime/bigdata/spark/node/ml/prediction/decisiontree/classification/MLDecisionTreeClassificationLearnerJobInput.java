/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on May 20, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree.classification;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.EnumContainer.InformationGain;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTreeJobInput;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLDecisionTreeClassificationLearnerJobInput extends MLDecisionTreeJobInput {

    /** Criterion used for information gain calculation. Supported values: "gini" (recommended) or "entropy". */
    private static final String QUALITY_MEASURE = "qualityMeasure";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public MLDecisionTreeClassificationLearnerJobInput() {
    }

    /**
     *
     * @param namedInputObject Key/ID of the named input object (DataFrame/RDD) to learn on.
     * @param namedOutputModel Key/ID for the model that shall be produced by the job.
     * @param targetColIdx The column index of the target/class column.
     * @param featureColIdxs the feature column indices starting with 0
     * @param maxDepth
     * @param maxNoOfBins
     * @param minRowsPerNodeChild
     * @param qualityMeasure
     */
    protected MLDecisionTreeClassificationLearnerJobInput(final String namedInputObject,
        final String namedOutputModel,
        final int targetColIdx,
        final Integer[] featureColIdxs,
        final int maxDepth,
        final int maxNoOfBins,
        final int minRowsPerNodeChild,
        final InformationGain qualityMeasure) {

        super(namedInputObject, namedOutputModel, targetColIdx, featureColIdxs, maxDepth, maxNoOfBins,
            minRowsPerNodeChild);
        set(QUALITY_MEASURE, qualityMeasure.name());
    }

    /**
     * @return the {@link InformationGain} method to use
     */
    public InformationGain getQualityMeasure() {
        final String measure = get(QUALITY_MEASURE);
        return InformationGain.valueOf(measure);
    }
}

/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import com.knime.bigdata.spark.core.job.ClassificationWithNominalFeatureInfoJobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.EnumContainer.InformationGain;
import com.knime.bigdata.spark.core.job.util.NominalFeatureInfo;
/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class DecisionTreeJobInput extends ClassificationWithNominalFeatureInfoJobInput {

    /**Criterion used for information gain calculation. Supported values: "gini" (recommended) or "entropy".*/
    private static final String QUALITY_MEASURE = "impurity";

    /**
     * maxDepth - Maximum depth of the tree. E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf
     * nodes. (suggested value: 5)
     */
    private static final String MAX_DEPTH = "maxDepth";

    /**maxBins - maximum number of bins used for splitting features (suggested value: 32)*/
    private static final String MAX_BINS = "maxBins";


    /**
     * Paramless constructor for automatic deserialization.
     */
    public DecisionTreeJobInput() {}

    /**
     * @param namedInputObject
     * @param featureColIdxs
     * @param nominalFeatureInfo
     * @param classColIdx
     * @param noOfClasses
     * @param maxDepth
     * @param maxNoOfBins
     * @param qualityMeasure
     */
    protected DecisionTreeJobInput(final String namedInputObject, final Integer[] featureColIdxs,
        final NominalFeatureInfo nominalFeatureInfo, final int classColIdx, final Long noOfClasses, final int maxDepth,
        final int maxNoOfBins, final InformationGain qualityMeasure) {
        super(namedInputObject, nominalFeatureInfo, noOfClasses, classColIdx, featureColIdxs);
        set(MAX_DEPTH, maxDepth);
        set(MAX_BINS, maxNoOfBins);
        set(QUALITY_MEASURE, qualityMeasure.name());
    }

    /**
     * @return the maximum tree depth
     */
    public int getMaxDepth() {
        return getInteger(MAX_DEPTH);
    }
    /**
     * @return the maximum number of bins
     */
    public int getMaxNoOfBins() {
        return getInteger(MAX_BINS);
    }

    /**
     * @return the {@link InformationGain} method to use
     */
    public InformationGain getQualityMeasure() {
        final String measure = get(QUALITY_MEASURE);
        return InformationGain.valueOf(measure);
    }
}

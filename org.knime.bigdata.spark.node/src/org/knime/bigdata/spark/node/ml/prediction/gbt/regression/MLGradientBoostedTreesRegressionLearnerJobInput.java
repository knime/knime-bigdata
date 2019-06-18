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
 *   Created on Jun 12, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.gbt.regression;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.EnumContainer.GBTLossFunction;
import org.knime.bigdata.spark.node.ml.prediction.gbt.MLGradientBoostedTreesLearnerJobInput;

/**
 * Job input for the GBT regression learner job.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLGradientBoostedTreesRegressionLearnerJobInput extends MLGradientBoostedTreesLearnerJobInput {

    private static final String KEY_LOSS_FUNCTION = "lossFunction";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public MLGradientBoostedTreesRegressionLearnerJobInput() {
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
     * @param minInformationGain
     * @param seed
     * @param featureSubsetStrategy Number of features to consider for splits at each node.
     * @param subsamplingRate
     * @param maxIterations
     * @param learningRate
     * @param lossFunction
     */
    protected MLGradientBoostedTreesRegressionLearnerJobInput(final String namedInputObject,
        final String namedOutputModel,
        final int targetColIdx,
        final Integer[] featureColIdxs,
        final int maxDepth,
        final int maxNoOfBins,
        final int minRowsPerNodeChild,
        final double minInformationGain,
        final long seed,
        final String featureSubsetStrategy,
        final double subsamplingRate,
        final int maxIterations,
        final double learningRate,
        final GBTLossFunction lossFunction) {

        super(namedInputObject, namedOutputModel, targetColIdx, featureColIdxs, maxDepth, maxNoOfBins,
            minRowsPerNodeChild, minInformationGain, seed, featureSubsetStrategy, subsamplingRate, maxIterations,
            learningRate);

        set(KEY_LOSS_FUNCTION, lossFunction.name());
    }

    /**
     * @return the {@link GBTLossFunction} to use
     */
    public GBTLossFunction getLossFunction() {
        final String lossFunction = get(KEY_LOSS_FUNCTION);
        return GBTLossFunction.valueOf(lossFunction);
    }
}

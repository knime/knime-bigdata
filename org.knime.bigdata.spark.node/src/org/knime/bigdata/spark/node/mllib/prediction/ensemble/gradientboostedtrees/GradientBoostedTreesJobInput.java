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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.EnumContainer.QualityMeasure;
import org.knime.bigdata.spark.core.job.util.EnumContainer.LossFunction;
import org.knime.bigdata.spark.core.job.util.NominalFeatureInfo;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.DecisionTreeJobInput;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class GradientBoostedTreesJobInput extends DecisionTreeJobInput {

    /**loss function - either AbsoluteError, LogLoss, or SquaredError*/
    public static final String LOSS_FUNCTION = "lossFunction";

    private static final String NO_OF_ITERATIONS = "noofIterations";

    private static final String LEARNING_RATE = "learningRate";

    //there are more options, search for 'further options' below

    //note that as of Spark 1.2.1 only binary classification or regression is supported!

    //note that max depth must be <= 30 (at least in Spark 1.2.1)

    /**
     * Paramless constructor for automatic deserialization.
     */
    public GradientBoostedTreesJobInput() {}

    /**
     * @param namedInputObject
     * @param featureColIdxs
     * @param nominalFeatureInfo
     * @param classColIdx
     * @param noOfClasses
     * @param maxDepth
     * @param maxNoOfBins
     * @param aNumIterations
     * @param aLearningRate
     * @param aIsClassification
     * @param qualityMeasure
     */
    protected GradientBoostedTreesJobInput(final String namedInputObject, final Integer[] featureColIdxs,
        final NominalFeatureInfo nominalFeatureInfo, final int classColIdx, final Long noOfClasses, final int maxDepth,
        final int maxNoOfBins, final int aNumIterations, final double aLearningRate, final boolean aIsClassification,
        final QualityMeasure qualityMeasure) {
        super(namedInputObject, featureColIdxs, nominalFeatureInfo, classColIdx, noOfClasses, aIsClassification, maxDepth, maxNoOfBins,
            qualityMeasure);
        set(NO_OF_ITERATIONS, aNumIterations);
        set(LEARNING_RATE, aLearningRate);
        final LossFunction lossFunction =
            aIsClassification ? LossFunction.LogLoss : LossFunction.SquaredError;
        set(LOSS_FUNCTION, lossFunction.name());
    }

    /**
     * @return the number of iterations
     */
    public int getNoOfIterations() {
        return getInteger(NO_OF_ITERATIONS);
    }

    /**
     * @return the learning rate. Default is 0.1.
     */
    public double getLearningRate() {
        return getDouble(LEARNING_RATE);
    }

    /**
     * @return the {@link LossFunction}
     */
    public LossFunction getLossFunction() {
        String functionName = get(LOSS_FUNCTION);
        return LossFunction.valueOf(functionName);
    }
}

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
 */
package org.knime.bigdata.spark.node.ml.prediction.linear.classification;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.ml.prediction.linear.MLLinearLearnerJobInput;

/**
 * Spark ml-based logistic regression learner job input.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class MLLogisticRegressionLearnerJobInput extends MLLinearLearnerJobInput {

    /**
     * Paramless constructor for automatic deserialization.
     */
    public MLLogisticRegressionLearnerJobInput() {
    }

    /**
     * Default constructor.
     *
     * @param namedInputObject Key/ID of the named input object (DataFrame/RDD) to learn on.
     * @param namedOutputModel Key/ID for the model that shall be produced by the job.
     * @param targetColIdx The column index of the target/class column.
     * @param featureColIdxs the feature column indices starting with 0
     * @param maxIter maximum iterations
     * @param standardization standardize features
     * @param fitIntercept fit intercept
     * @param regularizer NONE, RIDGE, LASO or ELASTIC_NET regularization to use
     * @param regParam regularization parameter (only used in RIDGE, LASO or ELASTIC_NET mode)
     * @param elasticNetParam elastic net parameter (only used in ELASTIC_NET mode)
     * @param convergenceTolerance convergence tolerance
     * @param handleInvalid how to handle invalid data (skip or error)
     */
    public MLLogisticRegressionLearnerJobInput(final String namedInputObject, final String namedOutputModel,
        final int targetColIdx, final Integer[] featureColIdxs, final int maxIter, final boolean standardization,
        final boolean fitIntercept, final String regularizer, final double regParam, final double elasticNetParam,
        final double convergenceTolerance, final String handleInvalid) {

        super(namedInputObject, namedOutputModel, targetColIdx, featureColIdxs, maxIter, standardization, fitIntercept,
            regularizer, regParam, elasticNetParam, convergenceTolerance, handleInvalid);
    }
}

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
package org.knime.bigdata.spark.node.ml.prediction.linear;

import org.knime.bigdata.spark.core.job.NamedModelLearnerJobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Abstract ML-based linear learner job input.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public abstract class MLLinearLearnerJobInput extends NamedModelLearnerJobInput {

    private static final String KEY_MAX_ITER = "maxIter";

    private static final String KEY_STANDARDIZATION = "standardization";

    private static final String KEY_FIT_INTERCEPT = "fitIntercept";

    private static final String KEY_REGULARIZER = "regularizer";

    private static final String KEY_REG_PARAM = "regParam";

    private static final String KEY_ELASTIC_NET_PARAM = "elasticNetParam";

    private static final String KEY_CONVERGENCE_TOLERANCE = "convergenceTolerance";

    /**
     * Paramless constructor for automatic deserialization.
     */
    protected MLLinearLearnerJobInput() {
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
     * @param regularizer NONE, RIDGE, LASO or ELASTIC_NET
     * @param regParam regularization parameter (only used in RIDGE, LASO or ELASTIC_NET mode)
     * @param elasticNetParam elastic net parameter (only used in ELASTIC_NET mode)
     * @param convergenceTolerance convergence tolerance
     */
    protected MLLinearLearnerJobInput(final String namedInputObject, final String namedOutputModel,
        final int targetColIdx, final Integer[] featureColIdxs, final int maxIter, final boolean standardization,
        final boolean fitIntercept, final String regularizer, final double regParam,
        final double elasticNetParam, final double convergenceTolerance) {

        super(namedInputObject, namedOutputModel, targetColIdx, featureColIdxs);
        set(KEY_MAX_ITER, maxIter);
        set(KEY_STANDARDIZATION, standardization);
        set(KEY_FIT_INTERCEPT, fitIntercept);
        set(KEY_REGULARIZER, regularizer);
        set(KEY_REG_PARAM, regParam);
        set(KEY_ELASTIC_NET_PARAM, elasticNetParam);
        set(KEY_CONVERGENCE_TOLERANCE, convergenceTolerance);
    }

    /**
     * @return maximum iterations
     */
    public int getMaxIter() {
        return getInteger(KEY_MAX_ITER);
    }

    /**
     * @return standardize features
     */
    public boolean getStandardization() {
        return get(KEY_STANDARDIZATION);
    }

    /**
     * @return fit interceptor
     */
    public boolean getFitIntercept() {
        return get(KEY_FIT_INTERCEPT);
    }

    /**
     * @return regularization type
     */
    public String getRegularizer() {
        return get(KEY_REGULARIZER);
    }

    /**
     * @return regularization parameter
     */
    public double getRegParam() {
        return getDouble(KEY_REG_PARAM);
    }

    /**
     * @return elastic net parameter
     */
    public double getElasticNetParam() {
        return getDouble(KEY_ELASTIC_NET_PARAM);
    }

    /**
     * @return convergence tolerance
     */
    public double getConvergenceTolerance() {
        return getDouble(KEY_CONVERGENCE_TOLERANCE);
    }

}

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
package com.knime.bigdata.spark.node.mllib.prediction.linear;

import com.knime.bigdata.spark.core.job.ClassificationJobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.EnumContainer.LinearLossFunction;
import com.knime.bigdata.spark.core.job.util.EnumContainer.LinearRegularizer;

/**
 * @author Tobias Koetter, KNIME.com, dwk
 */
@SparkClass
public class LinearLearnerJobInput extends ClassificationJobInput {

    /**
     * number of optimization iterations
     */
    private static final String NUM_ITERATIONS = "noOfIterations";

    /**
     * regularization parameter, should be some float between 0 and 1 (0.1)
     */
    private static final String REGULARIZATION = "Regularization";

    /**
     * used to perform steps (weight update) using Gradient Descent methods.
     */
    private static final String REGULARIZER = "regularizer";

    /**
     * gradient descent type
     */
    private static final String LOSS_FUNCTION = "lossFunction";

    /**
     * (SGD only)
     */
    private static final String STEP_SIZE = "stepSize";

    /**
     * (SGD only)
     */
    private static final String FRACTION = "fraction";

    /**
     *  should the algorithm validate data before training?
     */
    private static final String VALIDATE_DATA = "validateData";

    /**
     * should algorithm  add an intercept?
     */
    private static final String ADD_INTERCEPT = "addIntercept";

    private static final String USE_FEATURE_SCALING = "useFeatureScaling";

    /**
     * use SGD or LBFGS optimization
     */
    private static final String USE_SGD = "useSGD";

    /**
     * number of corrections (for LBFGS optimization)
     */
    private static final String CORRECTIONS = "numCorrections";

    /**
     * tolerance (for LBFGS optimization)
     */
    private static final String TOLERANCE = "tolerance";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public LinearLearnerJobInput() {}

    /**
     * Constructor for methods that do not use SGD.
     * @param aInputRDD
     * @param featureColIdxs
     * @param classColIdx
     * @param aNumIterations
     * @param aRegularization
     * @param regularizer
     * @param aValidateData
     * @param aAddIntercept
     * @param aUseFeatureScaling
     * @param lossFunction
     * @param aTolerance - only required when aUseSGD == false
     * @param aNumCorrections - only required when aUseSGD == false
     */
    public LinearLearnerJobInput(final String aInputRDD, final Integer[] featureColIdxs, final int classColIdx,
        final int aNumIterations, final double aRegularization, final LinearRegularizer regularizer, final Boolean aValidateData,
        final Boolean aAddIntercept, final Boolean aUseFeatureScaling, final LinearLossFunction lossFunction,
        final Integer aNumCorrections, final Double aTolerance) {
        this(aInputRDD, featureColIdxs, classColIdx, aNumIterations, aRegularization, regularizer, aValidateData,
            aAddIntercept, aUseFeatureScaling, lossFunction, false, null, null, aNumCorrections, aTolerance);
    }


    /**
     * Constructor for methods that use SGD.
     *  @param aInputRDD
     * @param featureColIdxs
     * @param classColIdx
     * @param aNumIterations
     * @param aRegularization
     * @param regularizer
     * @param aValidateData
     * @param aAddIntercept
     * @param aUseFeatureScaling
     * @param lossFunction
     * @param aStepSize
     * @param aFraction
     */
    public LinearLearnerJobInput(final String aInputRDD, final Integer[] featureColIdxs, final int classColIdx,
        final int aNumIterations, final double aRegularization, final LinearRegularizer regularizer, final Boolean aValidateData,
        final Boolean aAddIntercept, final Boolean aUseFeatureScaling, final LinearLossFunction lossFunction,
        final Double aStepSize, final Double aFraction) {
        this(aInputRDD, featureColIdxs, classColIdx, aNumIterations, aRegularization, regularizer, aValidateData,
            aAddIntercept, aUseFeatureScaling, lossFunction, true, aStepSize, aFraction, null, null);
    }


    /**
     *  @param aInputRDD
     * @param featureColIdxs
     * @param classColIdx
     * @param aNumIterations
     * @param aRegularization
     * @param regularizer
     * @param aValidateData
     * @param aAddIntercept
     * @param aUseFeatureScaling
     * @param lossFunction
     * @param aUseSGD
     * @param aTolerance - only required when aUseSGD == false
     * @param aNumCorrections - only required when aUseSGD == false
     * @param aStepSize  - only required when aUseSGD == true
     * @param aFraction  - only required when aUseSGD == true
     */
    LinearLearnerJobInput(final String aInputRDD, final Integer[] featureColIdxs, final int classColIdx,
        final int aNumIterations, final double aRegularization, final LinearRegularizer regularizer, final Boolean aValidateData,
        final Boolean aAddIntercept, final Boolean aUseFeatureScaling, final LinearLossFunction lossFunction,
        final boolean aUseSGD, final Double aStepSize, final Double aFraction, final Integer aNumCorrections,
        final Double aTolerance) {
        super(aInputRDD, classColIdx, featureColIdxs);
        if (aUseSGD) {
            if (aStepSize == null) {
                throw new IllegalArgumentException("Step size must not be null for SGD job");
            }
            if (aFraction == null) {
                throw new IllegalArgumentException("Fraction must not be null for SGD job");
            }
        } else {
            if (aTolerance == null) {
                throw new IllegalArgumentException("Tolerance must not be null for none SGD job");
            }
            if (aNumCorrections == null) {
                throw new IllegalArgumentException("Number of corrections must not be null for none SGD job");
            }
        }
        set(NUM_ITERATIONS, aNumIterations);
        set(REGULARIZATION, aRegularization);
        set(REGULARIZER, regularizer.name());
        set(VALIDATE_DATA, aValidateData);
        set(ADD_INTERCEPT, aAddIntercept);
        set(USE_FEATURE_SCALING, aUseFeatureScaling);
        set(LOSS_FUNCTION, lossFunction.name());
        set(STEP_SIZE, aStepSize);
        set(FRACTION, aFraction);
        set(CORRECTIONS, aNumCorrections);
        set(TOLERANCE, aTolerance);
        set(USE_SGD, aUseSGD);
    }

    /**
     * @return number of corrections (for LBFGS optimization)
     */
    public Integer getNoOfCorrections() {
        return getInteger(CORRECTIONS);
    }

    /**
     * @return tolerance (for LBFGS optimization)
     */
    public Double getTolerance() {
        return getDouble(TOLERANCE);
    }

    /**
     * @return use SGD or LBFGS optimization
     */
    public Boolean useSGD() {
        return get(USE_SGD);
    }

    /**
     * @return number of optimization iterations
     */
    public Integer getNoOfIterations() {
        return getInteger(NUM_ITERATIONS);
    }

    /**
     * @return regularization parameter, should be some float between 0 and 1 (0.1)
     */
    public Double getRegularization() {
        return getDouble(REGULARIZATION);
    }

    /**
     * @return used to perform steps (weight update) using Gradient Descent methods.
     */
    public LinearRegularizer getRegularizer() {
        return LinearRegularizer.valueOf((String)get(REGULARIZER));
    }

    /**
     * @return should the algorithm validate data before training?
     */
    public Boolean validateData() {
        return get(VALIDATE_DATA);
    }

    /**
     * @return should algorithm  add an intercept?
     */
    public Boolean addIntercept() {
        return get(ADD_INTERCEPT);
    }

    /**
     * @return should feature scaling be used
     */
    public Boolean useFeatureScaling() {
        return get(USE_FEATURE_SCALING);
    }

    /**
     * @return gradient descent type
     */
    public LinearLossFunction getLossFunction() {
        return LinearLossFunction.valueOf((String)get(LOSS_FUNCTION));
    }

    /**
     * @return (SGD only)
     */
    public Double getStepSize() {
        return getDouble(STEP_SIZE);
    }

    /**
     * @return (SGD only)
     */
    public Double getFraction() {
        return getDouble(FRACTION);
    }
}

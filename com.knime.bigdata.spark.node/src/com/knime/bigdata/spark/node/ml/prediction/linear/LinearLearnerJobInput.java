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
package com.knime.bigdata.spark.node.ml.prediction.linear;

import com.knime.bigdata.spark.core.job.ClassificationWithNominalFeatureInfoJobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.EnumContainer.RegressionSolver;
import com.knime.bigdata.spark.core.job.util.NominalFeatureInfo;

/**
 * @author Tobias Koetter, KNIME.com, dwk
 */
@SparkClass
public class LinearLearnerJobInput extends ClassificationWithNominalFeatureInfoJobInput {

    // see getters for descriptions
    private static final String NUM_ITERATIONS = "noOfIterations";

    private static final String REGULARIZATION = "Regularization";

    private static final String ELASTIC_NET_PARAM = "ElasticNetParam";

    private static final String SOLVER = "solver";

    private static final String ADD_INTERCEPT = "addIntercept";

    private static final String USE_FEATURE_SCALING = "useFeatureScaling";

    private static final String TOLERANCE = "tolerance";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public LinearLearnerJobInput() {
    }

    LinearLearnerJobInput(final String aInputRDD, final Integer[] featureColIdxs,
        final NominalFeatureInfo aNominalFeatureInfo, final int classColIdx, final int aNumIterations,
        final double aRegularization, final RegressionSolver aSolver, final double aElasticNetParam,
        final Boolean aAddIntercept, final Boolean aUseFeatureScaling,
        final double aTolerance) {
        super(aInputRDD, aNominalFeatureInfo, -1l, classColIdx, featureColIdxs);
        set(ELASTIC_NET_PARAM, aElasticNetParam);
        set(TOLERANCE, aTolerance);
        set(NUM_ITERATIONS, aNumIterations);
        set(REGULARIZATION, aRegularization);
        set(SOLVER, aSolver.name());
        set(ADD_INTERCEPT, aAddIntercept);
        set(USE_FEATURE_SCALING, aUseFeatureScaling);
    }

    /**
     * get the ElasticNet mixing parameter. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1
     * penalty. For alpha in (0,1), the penalty is a combination of L1 and L2. Default is 0.0 which is an L2 penalty.
     *
     * @return number of corrections (for LBFGS optimization)
     */
    public Double getElasticNetParam() {
        return getDouble(ELASTIC_NET_PARAM);
    }

    /**
     * the convergence tolerance of iterations. Smaller value will lead to higher accuracy with the cost of more iterations.
     * @return tolerance
     */
    public Double getTolerance() {
        return getDouble(TOLERANCE);
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
     * @return the solver algorithm used for optimization.
     * In case of linear regression, this can be "l-bfgs", "normal" and "auto".
     */
    public String getSolver() {
        final String solver = get(SOLVER);
        if (RegressionSolver.l_bfgs.name().equals(solver)) {
            return "l-bfgs";
        }
        return solver;
    }

    /**
     * @return should algorithm add an intercept? (if we should fit the intercept)
     */
    public Boolean addIntercept() {
        return get(ADD_INTERCEPT);
    }

    /**
     * @return should feature scaling be used (Whether to standardize the training features before fitting the model.)
     */
    public Boolean useFeatureScaling() {
        return get(USE_FEATURE_SCALING);
    }

    /**
     * TODO - decide if we need / want this, might require changes to the pipeline
     * Whether to over-/under-sample training instances according to the given weights in weightCol.
     * If not set or empty, all instances are treated equally (weight 1.0). Default is not set, so all instances have weight one.
     * @return null
     */
   public String getWeightColumn() {
       return null;
   }
}

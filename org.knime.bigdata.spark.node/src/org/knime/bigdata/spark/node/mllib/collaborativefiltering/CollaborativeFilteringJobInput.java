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
package org.knime.bigdata.spark.node.mllib.collaborativefiltering;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class CollaborativeFilteringJobInput extends JobInput {

    /**
     * column index of the user
     */
    private static final String PARAM_USER_INDEX = "UserIx";

    /**
     * column index of the product
     */
    private static final String PARAM_PRODUCT_INDEX = "ProductIx";

    /**
     * column index of the rating
     */
    private static final String PARAM_RATING_INDEX = "RatingIx";

    /**
     * lambda parameter
     */
    private static final String PARAM_LAMBDA = "Lambda";

    /**
     * the constant used in computing confidence in implicit ALS. Default: 1.0.
     */
    private static final String PARAM_ALPHA = "Alpha";

    /**
     * whether the least-squares problems solved at each iteration should have nonnegativity constraints.
     */
    private static final String PARAM_IS_NON_NEGATIVE = "NonNegative";

    /**
     * the number of user blocks to parallelize the computation.
     */
    private static final String PARAM_NUM_USER_BLOCKS = "NumUserBlocks";

    /**
     * the number of product blocks to parallelize the computation.
     */
    private static final String PARAM_NUM_PRODUCT_BLOCKS = "NumProductBlocks";

    //private static final String PARAM_STORAGE_LEVEL = null;

    /**
     * whether to use implicit preference.
     */
    private static final String PARAM_IMPLICIT_PREFS = "UseImplicitPrefs";

    /**
     * the number of blocks to parallelize the computation into; pass -1 for an
     * auto-configured number of blocks. Default: -1
     */
    private static final String PARAM_NUM_BLOCKS = "NumBlocks";

    /**
     * the number of iterations to run.
     */
    private static final String PARAM_NUM_ITERATIONS = "noOfIterations";

    /**
     * the rank of the feature matrices computed (number of features).
     */
    private static final String PARAM_RANK = "Rank";

    /**
     * a random seed to have deterministic results.
     */
    private static final String PARAM_SEED = "Seed";


    //These variables are also used in the prediction job to identify the user and product column in the
    //include column indices of the PredictionJobInput which are the columns based on the input MLlibSettings.
    /**Feature column index of the user column in the MLlibSettings object.*/
    public static final int MLLIB_SETTINGS_USER_COL_IDX = 0;

    /**Feature column index of the product column in the MLlibSettings object.*/
    public static final int MLLIB_SETTINGS_PRODUCT_COL_IDX = 1;

    /**
     * Paramless constructor for automatic deserialization.
     */
    public CollaborativeFilteringJobInput() {}


    CollaborativeFilteringJobInput(final String tableName, final String predictions, final int userIdx,
        final int productIdx, final int ratingIdx, final double lambda, final double alpha, final int iterations,
        final int rank, final boolean implicitPrefs, final int noOfBlocks) {
        this(tableName, predictions, userIdx, productIdx, ratingIdx, lambda, alpha, iterations, rank, implicitPrefs,
            noOfBlocks, null, null, null, null);
    }

    CollaborativeFilteringJobInput(final String aInputRDD, final String namedOutputObject, final int aUserIdx,
        final int aProductIdx, final int aRatingIdx, final Double aLambda, final Double aAlpha,
        final int aIterations, final Integer aRank, final boolean implicitPrefs, final Integer noOfBlocks,
        final Integer noOfUserBlocks, final Integer noOfProductBlocks, final Boolean isNonNegative,
        final Long randomSeed) {
        addNamedInputObject(aInputRDD);
        if (namedOutputObject != null) {
            addNamedOutputObject(namedOutputObject);
        }

        //note that user and product columns must be integer valued
        set(PARAM_USER_INDEX, aUserIdx);
        set(PARAM_PRODUCT_INDEX, aProductIdx);
        //rating column should be double valued
        set(PARAM_RATING_INDEX, aRatingIdx);

        set(PARAM_LAMBDA, aLambda);
        set(PARAM_ALPHA, aAlpha);
        set(PARAM_NUM_ITERATIONS, aIterations);
        set(PARAM_RANK, aRank);
        set(PARAM_IMPLICIT_PREFS, implicitPrefs);
        set(PARAM_NUM_BLOCKS, noOfBlocks);
        set(PARAM_NUM_USER_BLOCKS, noOfUserBlocks);
        set(PARAM_NUM_PRODUCT_BLOCKS, noOfProductBlocks);
        set(PARAM_IS_NON_NEGATIVE, isNonNegative);
        set(PARAM_SEED, randomSeed);
    }

    /**
     * @return the user column index
     */
    public int getUserIdx () {
        return getInteger(PARAM_USER_INDEX);
    }

    /**
     * @return the column index of the product
     */
    public int getProductIdx() {
        return getInteger(PARAM_PRODUCT_INDEX);
    }

    /**
     * @return the column index of the rating
     */
    public int getRatingIdx() {
        return getInteger(PARAM_RATING_INDEX);
    }

    /**
     * @return the lambda parameter
     */
    public Double getLambda() {
        return getDouble(PARAM_LAMBDA);
    }

    /**
     * @return the constant used in computing confidence in implicit ALS
     */
    public Double getAlpha() {
        return getDouble(PARAM_ALPHA);
    }

    /**
     * @return the number of iterations to run
     */
    public Integer getNoOfIterations() {
        return getInteger(PARAM_NUM_ITERATIONS);
    }

    /**
     * @return the rank of the feature matrices computed (number of features)
     */
    public Integer getRank() {
        return getInteger(PARAM_RANK);
    }

    /**
     * @return whether to use implicit preference
     */
    public Boolean isImplicitPrefs() {
        return get(PARAM_IMPLICIT_PREFS);
    }

    /**
     * @return the number of blocks to parallelize the computation into
     */
    public Integer getNoOfBlocks() {
        return getInteger(PARAM_NUM_BLOCKS);
    }

    /**
     * @return the number of user blocks to parallelize the computation
     */
    public Integer getNoOfUserBlocks() {
        return getInteger(PARAM_NUM_USER_BLOCKS);
    }

    /**
     * @return the number of product blocks to parallelize the computation
     */
    public Integer getNoOfProductBlocks() {
        return getInteger(PARAM_NUM_PRODUCT_BLOCKS);
    }

    /**
     * @return whether the least-squares problems solved at each iteration should have nonnegativity constraints
     */
    public Boolean isNonNegative() {
        return get(PARAM_IS_NON_NEGATIVE);
    }

    /**
     * @return a random seed to have deterministic results
     */
    public Long getSeed() {
        return getLong(PARAM_SEED);
    }
}

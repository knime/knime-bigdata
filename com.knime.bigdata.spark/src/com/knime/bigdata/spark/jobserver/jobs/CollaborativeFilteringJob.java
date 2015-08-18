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
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ModelUtils;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * runs MLlib Collaborative Filtering on a given RDD
 *
 * @author koetter, dwk
 */
public class CollaborativeFilteringJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * column index of the user
     */
    public static final String PARAM_USER_INDEX = "UserIx";

    /**
     * column index of the product
     */
    public static final String PARAM_PRODUCT_INDEX = "ProductIx";

    /**
     * column index of the rating
     */
    public static final String PARAM_RATING_INDEX = "RatingIx";

    /**
     * lambda parameter
     */
    public static final String PARAM_LAMBDA = "Lambda";

    /**
     * the constant used in computing confidence in implicit ALS. Default: 1.0.
     */
    public static final String PARAM_ALPHA = "Alpha";

    /**
     * whether the least-squares problems solved at each iteration should have nonnegativity constraints.
     */
    public static final String PARAM_IS_NON_NEGATIVE = "NonNegative";

    /**
     * the number of user blocks to parallelize the computation.
     */
    public static final String PARAM_NUM_USER_BLOCKS = "NumUserBlocks";

    /**
     * a random seed to have deterministic results.
     */
    public static final String PARAM_SEED = "Seed";

    /**
     * the number of product blocks to parallelize the computation.
     */
    public static final String PARAM_NUM_PRODUCT_BLOCKS = "NumProductBlocks";

    //private static final String PARAM_STORAGE_LEVEL = null;

    /**
     * whether to use implicit preference.
     */
    public static final String PARAM_IMPLICIT_PREFS = "UseImplicitPrefs";

    /**
     * the number of blocks for both user blocks and product blocks to parallelize the computation into; pass -1 for an
     * auto-configured number of blocks. Default: -1
     */
    public static final String PARAM_NUM_BLOCKS = "NumBlocks";

    /**
     * the number of iterations to run.
     */
    public static final String PARAM_NUM_ITERATIONS = ParameterConstants.PARAM_NUM_ITERATIONS;

    /**
     * the rank of the feature matrices computed (number of features).
     */
    public static final String PARAM_RANK = "Rank";

    private final static Logger LOGGER = Logger.getLogger(CollaborativeFilteringJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;

        if (!aConfig.hasInputParameter(PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
        }

        //the only required parameters are input rdd, user, product, rating
        if (msg == null) {
            msg = checkIntegerParam(aConfig, PARAM_USER_INDEX);
        }
        if (msg == null) {
            msg = checkIntegerParam(aConfig, PARAM_PRODUCT_INDEX);
        }
        if (msg == null) {
            msg = checkIntegerParam(aConfig, PARAM_RATING_INDEX);
        }

        if (msg == null) {
            if (aConfig.hasInputParameter(PARAM_LAMBDA)) {

                try {
                    if (aConfig.getInputParameter(PARAM_LAMBDA, Double.class) == null) {
                        msg = "Input parameter '" + PARAM_LAMBDA + "' is empty.";
                    }
                } catch (Exception e) {
                    msg = "Input parameter '" + PARAM_LAMBDA + "' is not of expected type 'double'.";
                }
            }
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * @param paramUserIndex
     * @return
     */
    private String checkIntegerParam(final JobConfig aConfig, final String aParamName) {
        if (!aConfig.hasInputParameter(aParamName)) {
            return "Input parameter '" + aParamName + "' missing.";
        } else {
            try {
                if ((Integer)aConfig.getInputParameter(aParamName, Integer.class) == null) {
                    return "Input parameter '" + aParamName + "' is empty.";
                }
            } catch (Exception e) {
                return "Input parameter '" + aParamName + "' is not of expected type 'integer'.";
            }
        }

        return null;
    }

    /**
     * run the actual job, the result is serialized back to the client
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        SupervisedLearnerUtils.validateInput(aConfig, this, LOGGER);
        LOGGER.log(Level.INFO, "starting Collaborative Filtering job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));

        final JavaRDD<Rating> ratings = convertRowRDD2RatingsRdd(aConfig, rowRDD);
        final MatrixFactorizationModel model = execute(sc, aConfig, ratings);

        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(model);

        if (aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            final JavaRDD<Row> predictions = ModelUtils.predict(rowRDD, ratings, model);
            addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), predictions);
        }
        LOGGER.log(Level.INFO, " Collaborative Filtering done");
        // note that with Spark 1.4 we can use PMML instead
        return res;

    }

    /**
     * @param aConfig
     */
    static ALS setLambda(final ALS aSolver, final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_LAMBDA)) {
            return aSolver.setLambda((Double)aConfig.getInputParameter(PARAM_LAMBDA, Double.class));
        }
        return aSolver;
    }

    /**
     * constant used in computing confidence in implicit ALS. Default: 1.0.
     *
     * @param aConfig
     * @return
     */
    static ALS setAlpha(final ALS aSolver, final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_ALPHA)) {
            return aSolver.setAlpha((Double)aConfig.getInputParameter(PARAM_ALPHA, Double.class));
        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setIsNonnegative(final ALS aSolver, final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_IS_NON_NEGATIVE)) {
            return aSolver.setNonnegative((Boolean)aConfig.getInputParameter(PARAM_IS_NON_NEGATIVE, Boolean.class));
        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setNumUserBlocks(final ALS aSolver, final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_NUM_USER_BLOCKS)) {
            return aSolver.setUserBlocks((Integer)aConfig.getInputParameter(PARAM_NUM_USER_BLOCKS, Integer.class));
        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setSeed(final ALS aSolver, final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_SEED)) {
            return aSolver.setSeed((Long)aConfig.getInputParameter(PARAM_SEED, Long.class));
        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setNumProductBlocks(final ALS aSolver, final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_NUM_PRODUCT_BLOCKS)) {
            return aSolver
                .setProductBlocks((Integer)aConfig.getInputParameter(PARAM_NUM_PRODUCT_BLOCKS, Integer.class));
        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setStorageLevel(final ALS aSolver, final JobConfig aConfig) {
        //needed?
        //        if (aConfig.hasInputParameter(PARAM_STORAGE_LEVEL)) {
        //            return aSolver.setIntermediateRDDStorageLevel((StorageLevel)aConfig.getInputParameter(PARAM_STORAGE_LEVEL, Integer.class));
        //        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setImplicitPrefs(final ALS aSolver, final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_IMPLICIT_PREFS)) {
            return aSolver.setImplicitPrefs((Boolean)aConfig.getInputParameter(PARAM_IMPLICIT_PREFS, Boolean.class));
        }
        return aSolver;
    }

    /**
     * Set the number of blocks for both user blocks and product blocks to parallelize the computation into; pass -1 for
     * an auto-configured number of blocks. Default: -1
     *
     * @param aConfig
     * @return
     */
    static ALS setNumBlocks(final ALS aSolver, final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_NUM_BLOCKS)) {
            return aSolver.setBlocks((Integer)aConfig.getInputParameter(PARAM_NUM_BLOCKS, Integer.class));
        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setNumIterations(final ALS aSolver, final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_NUM_ITERATIONS)) {
            return aSolver.setIterations((Integer)aConfig.getInputParameter(PARAM_NUM_ITERATIONS, Integer.class));
        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setRank(final ALS aSolver, final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_RANK)) {
            return aSolver.setRank((Integer)aConfig.getInputParameter(PARAM_RANK, Integer.class));
        }
        return aSolver;
    }

    /**
     *
     * @param aContext
     * @param aConfig
     * @param aInputData - Training dataset: RDD of Ratings
     * @return model
     */
    static MatrixFactorizationModel execute(final SparkContext aContext, final JobConfig aConfig,
        final JavaRDD<Rating> aRatings) {

        ALS solver = new ALS();
        // Build the recommendation model using ALS
        solver = setRank(solver, aConfig);
        solver = setNumIterations(solver, aConfig);
        solver = setAlpha(solver, aConfig);
        solver = setNumBlocks(solver, aConfig);
        solver = setImplicitPrefs(solver, aConfig);
        solver = setStorageLevel(solver, aConfig);
        solver = setNumProductBlocks(solver, aConfig);
        solver = setSeed(solver, aConfig);
        solver = setNumUserBlocks(solver, aConfig);
        solver = setIsNonnegative(solver, aConfig);
        //ALS.train(JavaRDD.toRDD(aRatings), rank, numIterations, 0.01);
        return solver.run(aRatings);
    }

    /**
     * @param aConfig
     * @param aInputRdd
     * @return
     */
    public static JavaRDD<Rating> convertRowRDD2RatingsRdd(final JobConfig aConfig, final JavaRDD<Row> aInputRdd) {
        final int userIx = aConfig.getInputParameter(PARAM_USER_INDEX, Integer.class);
        final int productIx = aConfig.getInputParameter(PARAM_PRODUCT_INDEX, Integer.class);
        final int ratingIx = aConfig.getInputParameter(PARAM_RATING_INDEX, Integer.class);
        return RDDUtilsInJava.convertRowRDD2RatingsRdd(userIx, productIx, ratingIx, aInputRdd);
    }
}

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
package org.knime.bigdata.spark1_6.jobs.mllib.collaborativefiltering;

import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.collaborativefiltering.CollaborativeFilteringJobInput;
import org.knime.bigdata.spark.node.mllib.collaborativefiltering.CollaborativeFilteringJobOutput;
import org.knime.bigdata.spark.node.mllib.prediction.predictor.PredictionJobInput;
import org.knime.bigdata.spark1_6.api.ModelUtils;
import org.knime.bigdata.spark1_6.api.NamedObjects;
import org.knime.bigdata.spark1_6.api.RDDUtilsInJava;
import org.knime.bigdata.spark1_6.api.SparkJob;

import scala.Tuple2;

/**
 * runs MLlib Collaborative Filtering on a given RDD
 *
 * @author koetter, dwk
 */
@SparkClass
public class CollaborativeFilteringJob implements SparkJob<CollaborativeFilteringJobInput, CollaborativeFilteringJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(CollaborativeFilteringJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public CollaborativeFilteringJobOutput runJob(final SparkContext sparkContext,
        final CollaborativeFilteringJobInput input, final NamedObjects namedObjects)
                throws KNIMESparkException, Exception {
        LOGGER.log(Level.INFO, "Starting Collaborative Filtering job...");
        final String inputRDDName = input.getFirstNamedInputObject();
        final JavaRDD<Row> rowRdd = namedObjects.getJavaRdd(inputRDDName);
        final int productIdx = input.getProductIdx();
        final int userIdx = input.getUserIdx();
        final JavaRDD<Rating> ratings = RDDUtilsInJava.convertRowRDD2RatingsRdd(userIdx,
            productIdx, input.getRatingIdx(), rowRdd);
        final MatrixFactorizationModel model = execute(sparkContext, input, ratings);
        LOGGER.log(Level.INFO, " Collaborative Filtering done");

        if (!input.getNamedOutputObjects().isEmpty()) {
            LOGGER.log(Level.INFO, "Apply model to input data...");
            final JavaRDD<Row> predictions = ModelUtils.predict(rowRdd, ratings, userIdx, productIdx, model);
            namedObjects.addJavaRdd(input.getFirstNamedOutputObject(), predictions);
            LOGGER.log(Level.INFO, " model applyed.");
        }
        final String userFeaturesRDDName = UUID.randomUUID().toString();
        final String productFeaturesRDDName = UUID.randomUUID().toString();
        final RDD<Tuple2<Object, double[]>> userRDD = model.userFeatures();
        final RDD<Tuple2<Object, double[]>> productRDD = model.productFeatures();
        namedObjects.addRdd(userFeaturesRDDName, userRDD);
        namedObjects.addRdd(productFeaturesRDDName, productRDD);
        final CollaborativeFilteringModel serializableModel = new CollaborativeFilteringModel(model.rank(),
            userFeaturesRDDName, productFeaturesRDDName);
        // note that with Spark 1.4 we can use PMML instead
        return new CollaborativeFilteringJobOutput(serializableModel, userRDD.name(), productRDD.name());
    }

    /**
     * @param input {@link PredictionJobInput}
     * @param namedObjects {@link NamedObjects}
     * @param rowRdd input {@link JavaRDD}
     * @param cModel {@link CollaborativeFilteringModel}
     * @return {@link JavaRDD} with prediction information
     */
    public static JavaRDD<Row> predict(final PredictionJobInput input, final NamedObjects namedObjects,
        final JavaRDD<Row> rowRdd, final CollaborativeFilteringModel cModel) {
        final RDD<Tuple2<Object, double[]>> userRDD = namedObjects.getRdd(cModel.getUserFeaturesRDDID());
        final RDD<Tuple2<Object, double[]>> productRDD = namedObjects.getRdd(cModel.getProductFeaturesRDDID());
        final MatrixFactorizationModel model =
        		new MatrixFactorizationModel(cModel.getRank(), userRDD, productRDD);
        LOGGER.fine("MatrixFactorizationModel (Collaborative Filtering) found for prediction");
        final List<Integer> featureColIdxs = input.getIncludeColumnIndices();
        final Integer userColIdx = featureColIdxs.get(CollaborativeFilteringJobInput.MLLIB_SETTINGS_USER_COL_IDX);
        final Integer productColIdx =
                featureColIdxs.get(CollaborativeFilteringJobInput.MLLIB_SETTINGS_PRODUCT_COL_IDX);
        final int ratingsIdx = -1; //we do not have a ratings column when we predict previously unknown data
        final JavaRDD<Rating> ratings =
            RDDUtilsInJava.convertRowRDD2RatingsRdd(userColIdx, productColIdx, ratingsIdx, rowRdd);
        final JavaRDD<Row> predictedData = ModelUtils.predict(rowRdd, ratings, userColIdx, productColIdx, model);
        return predictedData;
    }

    /**
     * @param aConfig
     */
    static ALS setLambda(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getLambda() != null) {
            return aSolver.setLambda(aConfig.getLambda());
        }
        return aSolver;
    }

    /**
     * constant used in computing confidence in implicit ALS. Default: 1.0.
     *
     * @param aConfig
     * @return
     */
    static ALS setAlpha(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getAlpha() != null) {
            return aSolver.setAlpha(aConfig.getAlpha());
        }
        return aSolver;
    }

    /**
     * @param input
     * @return
     */
    static ALS setIsNonnegative(final ALS aSolver, final CollaborativeFilteringJobInput input) {
        if (input.isNonNegative() != null && input.isNonNegative()) {
            return aSolver.setNonnegative(input.isNonNegative());
        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setNumUserBlocks(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getNoOfUserBlocks() != null) {
            return aSolver.setUserBlocks(aConfig.getNoOfUserBlocks());
        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setSeed(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getSeed() != null) {
            return aSolver.setSeed(aConfig.getSeed());
        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setNumProductBlocks(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getNoOfProductBlocks() != null) {
            return aSolver.setProductBlocks(aConfig.getNoOfProductBlocks());
        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setStorageLevel(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
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
    static ALS setImplicitPrefs(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.isImplicitPrefs() != null) {
            return aSolver.setImplicitPrefs(aConfig.isImplicitPrefs());
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
    static ALS setNumBlocks(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getNoOfBlocks() != null) {
            return aSolver.setBlocks(aConfig.getNoOfBlocks());
        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setNumIterations(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getNoOfIterations() != null) {
            return aSolver.setIterations(aConfig.getNoOfIterations());
        }
        return aSolver;
    }

    /**
     * @param aConfig
     * @return
     */
    static ALS setRank(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getRank() != null) {
            return aSolver.setRank(aConfig.getRank());
        }
        return aSolver;
    }

    /**
     *
     * @param aContext
     * @param input
     * @param aInputData - Training dataset: RDD of Ratings
     * @return model
     */
    static MatrixFactorizationModel execute(final SparkContext aContext, final CollaborativeFilteringJobInput input,
        final JavaRDD<Rating> aRatings) {

        ALS solver = new ALS();
        // Build the recommendation model using ALS
        solver = setLambda(solver, input);
        solver = setRank(solver, input);
        solver = setNumIterations(solver, input);
        solver = setAlpha(solver, input);
        solver = setNumBlocks(solver, input);
        solver = setImplicitPrefs(solver, input);
        solver = setStorageLevel(solver, input);
        solver = setNumProductBlocks(solver, input);
        solver = setSeed(solver, input);
        solver = setNumUserBlocks(solver, input);
        solver = setIsNonnegative(solver, input);
        //ALS.train(JavaRDD.toRDD(aRatings), rank, numIterations, 0.01);
        return solver.run(aRatings);
    }
}

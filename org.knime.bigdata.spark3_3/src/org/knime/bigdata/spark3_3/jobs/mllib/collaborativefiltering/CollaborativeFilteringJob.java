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
package org.knime.bigdata.spark3_3.jobs.mllib.collaborativefiltering;

import static org.apache.spark.sql.functions.col;

import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.collaborativefiltering.CollaborativeFilteringJobInput;
import org.knime.bigdata.spark.node.mllib.collaborativefiltering.CollaborativeFilteringJobOutput;
import org.knime.bigdata.spark.node.mllib.prediction.predictor.PredictionJobInput;
import org.knime.bigdata.spark3_3.api.NamedObjects;
import org.knime.bigdata.spark3_3.api.SparkJob;

/**
 * Runs ML Collaborative Filtering on a given data frame.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class CollaborativeFilteringJob implements SparkJob<CollaborativeFilteringJobInput, CollaborativeFilteringJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(CollaborativeFilteringJob.class.getName());

    @Override
    public CollaborativeFilteringJobOutput runJob(final SparkContext sparkContext,
        final CollaborativeFilteringJobInput input, final NamedObjects namedObjects)
                throws KNIMESparkException, Exception {

        LOGGER.info("Starting Collaborative Filtering job...");
        final String inputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputDataset = namedObjects.getDataFrame(inputObject);
        final String userColumn = inputDataset.columns()[input.getUserIdx()];
        final String productColumn = inputDataset.columns()[input.getProductIdx()];
        final String ratingColumn = inputDataset.columns()[input.getRatingIdx()];
        final String predictColumn = input.getPredictColumnName();
        final ALSModel model = execute(input, inputDataset, userColumn, productColumn, ratingColumn, predictColumn);
        LOGGER.info("Collaborative Filtering done");

        if (!input.getNamedOutputObjects().isEmpty()) {
            LOGGER.info("Apply model to input data...");
            final Dataset<Row> predictions = model.transform(inputDataset);
            namedObjects.addDataFrame(input.getFirstNamedOutputObject(), predictions);
            LOGGER.info("model applyed.");
        }

        final String userFeaturesObjectName = UUID.randomUUID().toString();
        final String productFeaturesObjectName = UUID.randomUUID().toString();
        namedObjects.addDataFrame(userFeaturesObjectName, model.userFactors());
        namedObjects.addDataFrame(productFeaturesObjectName, model.itemFactors());
        final CollaborativeFilteringModel serializableModel = new CollaborativeFilteringModel(
            model.uid(), model.rank(), userFeaturesObjectName, userColumn, productFeaturesObjectName, productColumn);

        return new CollaborativeFilteringJobOutput(serializableModel, userFeaturesObjectName, productFeaturesObjectName);
    }

    /**
     * @param input {@link PredictionJobInput}
     * @param namedObjects {@link NamedObjects}
     * @param inputDataset input {@link JavaRDD}
     * @param cModel {@link CollaborativeFilteringModel}
     * @return {@link JavaRDD} with prediction information
     */
    public static Dataset<Row> predict(final PredictionJobInput input, final NamedObjects namedObjects,
            final Dataset<Row> inputDataset, final CollaborativeFilteringModel cModel) {

        final Dataset<Row> userFeatures = namedObjects.getDataFrame(cModel.getUserFeaturesObjectName());

        final Dataset<Row> productFeatures = namedObjects.getDataFrame(cModel.getProductFeaturesObjectName());
        final ALSModel model = new ALSModel(cModel.getUid(), cModel.getRank(), userFeatures, productFeatures);
        model.setPredictionCol(input.getPredictionColumnName());
        model.setUserCol(cModel.getUserFeaturesColumnName());
        model.setItemCol(cModel.getProductFeaturesColumnName());
        model.setColdStartStrategy("nan");
        return model.transform(inputDataset);
    }

    /**
     * constant used in computing confidence in implicit ALS. Default: 1.0.
     *
     * @param aConfig
     * @return
     */
    private ALS setAlpha(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getAlpha() != null) {
            return aSolver.setAlpha(aConfig.getAlpha());
        }
        return aSolver;
    }

    private ALS setImplicitPrefs(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.isImplicitPrefs() != null) {
            return aSolver.setImplicitPrefs(aConfig.isImplicitPrefs());
        }
        return aSolver;
    }

    private ALS setIsNonnegative(final ALS aSolver, final CollaborativeFilteringJobInput input) {
        if (input.isNonNegative() != null && input.isNonNegative()) {
            return aSolver.setNonnegative(input.isNonNegative());
        }
        return aSolver;
    }

    private ALS setLambda(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getLambda() != null) {
            return aSolver.setRegParam(aConfig.getLambda());
        }
        return aSolver;
    }

    /**
     * Set the number of blocks for both user blocks and product blocks to parallelize the computation into.
     * Don't use -1 in Spark > 2.0!
     */
    private ALS setNumBlocks(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getNoOfBlocks() != null && aConfig.getNoOfBlocks() > 0) {
            return aSolver.setNumBlocks(aConfig.getNoOfBlocks());
        }
        return aSolver;
    }

    private ALS setNumIterations(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getNoOfIterations() != null) {
            return aSolver.setMaxIter(aConfig.getNoOfIterations());
        }
        return aSolver;
    }

    private ALS setNumProductBlocks(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getNoOfProductBlocks() != null) {
            return aSolver.setNumItemBlocks(aConfig.getNoOfProductBlocks());
        }
        return aSolver;
    }

    private ALS setNumUserBlocks(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getNoOfUserBlocks() != null) {
            return aSolver.setNumUserBlocks(aConfig.getNoOfUserBlocks());
        }
        return aSolver;
    }

    private ALS setRank(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getRank() != null) {
            return aSolver.setRank(aConfig.getRank());
        }
        return aSolver;
    }

    private ALS setSeed(final ALS aSolver, final CollaborativeFilteringJobInput aConfig) {
        if (aConfig.getSeed() != null) {
            return aSolver.setSeed(aConfig.getSeed());
        }
        return aSolver;
    }

    /**
     * Configure and train model.
     *
     * @param input job configuration
     * @param inputDataset training data set with ratings
     * @return model trained model
     */
    private ALSModel execute(final CollaborativeFilteringJobInput input, final Dataset<Row> inputDataset,
            final String userColumn, final String itemColumn, final String ratingColumn, final String predicColumn) {

        // ensure that rating contains floats
        final Dataset<Row> dataset = inputDataset.select(
            col(userColumn), col(itemColumn), col(ratingColumn).cast(DataTypes.FloatType));

        ALS solver = new ALS()
            .setUserCol(userColumn)
            .setItemCol(itemColumn)
            .setRatingCol(ratingColumn)
            .setPredictionCol(predicColumn);
        solver = setAlpha(solver, input);
        solver = setImplicitPrefs(solver, input);
        solver = setIsNonnegative(solver, input);
        solver = setLambda(solver, input);
        solver = setNumBlocks(solver, input);
        solver = setNumIterations(solver, input);
        solver = setNumProductBlocks(solver, input);
        solver = setNumUserBlocks(solver, input);
        solver = setRank(solver, input);
        solver = setSeed(solver, input);

        return solver.fit(dataset);
    }
}

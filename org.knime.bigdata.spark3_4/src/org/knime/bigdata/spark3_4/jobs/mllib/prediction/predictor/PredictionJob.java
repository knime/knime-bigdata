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
package org.knime.bigdata.spark3_4.jobs.mllib.prediction.predictor;

import java.io.File;
import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.prediction.predictor.PredictionJobInput;
import org.knime.bigdata.spark3_4.api.ModelUtils;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJobWithFiles;
import org.knime.bigdata.spark3_4.api.TypeConverters;
import org.knime.bigdata.spark3_4.jobs.mllib.clustering.kmeans.KMeansJob;
import org.knime.bigdata.spark3_4.jobs.mllib.collaborativefiltering.CollaborativeFilteringJob;
import org.knime.bigdata.spark3_4.jobs.mllib.collaborativefiltering.CollaborativeFilteringModel;

/**
 * applies previously learned MLlib model to given data frame, predictions are inserted into a new data frame and (temporarily)
 * stored in the map of named objects, optionally saved to disk
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
@SparkClass
public class PredictionJob implements SparkJobWithFiles<PredictionJobInput, EmptyJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(PredictionJob.class.getName());

    @Override
    public EmptyJobOutput runJob(final SparkContext sparkContext, final PredictionJobInput input, final List<File> inputFiles, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {

        LOGGER.info("Starting prediction job...");
        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final String inputKey = input.getFirstNamedInputObject();
        final String outputKey = input.getFirstNamedOutputObject();
        final Dataset<Row> inputDataset = namedObjects.getDataFrame(inputKey);
        final Dataset<Row> outputDataset;

        LOGGER.info("Reading model from " + inputFiles.get(0));
        final Serializable model = input.readModelFromTemporaryFile(inputFiles.get(0));

        if (model instanceof CollaborativeFilteringModel) {
            LOGGER.info("Collaborative filtering model found. Create matrix factorization model");
            // this is a very special model as we need to convert it to the real model first
            outputDataset = CollaborativeFilteringJob.predict(input, namedObjects, inputDataset,
                (CollaborativeFilteringModel) model);

        } else if (model instanceof KMeansModel) {
            LOGGER.info("Predicting using clusters from KMeans model.");
            outputDataset = KMeansJob.transform(input, inputDataset, (KMeansModel) model);

        } else {
            final StructType predictSchema = TypeConverters.convertSpec(input.getSpec(outputKey));
            final JavaRDD<Row> predictedData = ModelUtils.predict(input.getIncludeColumnIndices(), inputDataset, model);
            outputDataset = spark.createDataFrame(predictedData, predictSchema);
        }

        LOGGER.info("Prediction done");
        namedObjects.addDataFrame(outputKey, outputDataset);

        return new EmptyJobOutput();
    }


}

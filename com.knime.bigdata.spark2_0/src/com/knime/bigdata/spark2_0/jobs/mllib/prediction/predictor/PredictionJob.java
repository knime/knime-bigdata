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
package com.knime.bigdata.spark2_0.jobs.mllib.prediction.predictor;

import java.io.File;
import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.ml.param.shared.HasPredictionCol;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.EmptyJobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.mllib.prediction.predictor.PredictionJobInput;
import com.knime.bigdata.spark2_0.api.ModelUtils;
import com.knime.bigdata.spark2_0.api.NamedObjects;
import com.knime.bigdata.spark2_0.api.SparkJobWithFiles;
import com.knime.bigdata.spark2_0.api.TypeConverters;
import com.knime.bigdata.spark2_0.jobs.mllib.clustering.kmeans.KMeansJob;
import com.knime.bigdata.spark2_0.jobs.mllib.collaborativefiltering.CollaborativeFilteringJob;
import com.knime.bigdata.spark2_0.jobs.mllib.collaborativefiltering.CollaborativeFilteringModel;

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

        } else if (model instanceof PipelineModel) {
            LOGGER.info("Predicting using ML pipeline.");
            final PipelineModel pipeline = (PipelineModel)model;
            outputDataset = renamePredictionColumn(pipeline, pipeline.transform(inputDataset), input.getPredictionColumnName());
            //LOGGER.info("prediction data set columns: "+Arrays.toString(outputDataset.columns()));
        } else {
            final StructType predictSchema = TypeConverters.convertSpec(input.getSpec(outputKey));
            final JavaRDD<Row> predictedData = ModelUtils.predict(input.getIncludeColumnIndices(), inputDataset.javaRDD(), model);
            outputDataset = spark.createDataFrame(predictedData, predictSchema);
        }

        LOGGER.info("Prediction done");
        namedObjects.addDataFrame(outputKey, outputDataset);

        return EmptyJobOutput.getInstance();
    }

    /**
     * @param aModel
     * @param aDataset
     * @param aNewPredictionColumnName
     * @return aDataset with a renamed prediction column if one can be found or the original Dataset
     */
    private static Dataset<Row>  renamePredictionColumn(final PipelineModel aModel, final Dataset<Row> aDataset,
        final String aNewPredictionColumnName) {
        //find the last stage in the pipeline that has a outputColumn parameter or a prediction column
        // - that should be our prediction column (possibly with the re-mapped index labels)
        String outputColName = null;
        for (PipelineStage stage : aModel.stages()) {
            LOGGER.info("Stage: "+stage.logName());
            if (stage instanceof HasOutputCol) {
                outputColName = ((HasOutputCol)stage).getOutputCol();
                LOGGER.info("Output column name: "+ outputColName);
            }
            if (stage instanceof HasPredictionCol) {
                outputColName = ((HasPredictionCol)stage).getPredictionCol();
                LOGGER.info("Prediction column name: "+ outputColName);
            }

        }
        if (outputColName != null) {
            return aDataset.withColumnRenamed(outputColName, aNewPredictionColumnName);
        }
        return aDataset;

    }
}

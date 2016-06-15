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
package com.knime.bigdata.spark1_6.jobs.mllib.prediction.predictor;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.mllib.prediction.predictor.PredictionJobInput;
import com.knime.bigdata.spark1_6.api.ModelUtils;
import com.knime.bigdata.spark1_6.api.NamedObjects;
import com.knime.bigdata.spark1_6.api.SimpleSparkJob;
import com.knime.bigdata.spark1_6.jobs.mllib.collaborativefiltering.CollaborativeFilteringJob;
import com.knime.bigdata.spark1_6.jobs.mllib.collaborativefiltering.CollaborativeFilteringModel;

/**
 * applies previously learned MLlib model to given RDD, predictions are inserted into a new RDD and (temporarily)
 * stored in the map of named RDDs, optionally saved to disk
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
@SparkClass
public class PredictionJob implements SimpleSparkJob<PredictionJobInput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(PredictionJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final PredictionJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {
        LOGGER.log(Level.INFO, "starting prediction job...");
        final JavaRDD<Row> rowRdd = namedObjects.getJavaRdd(input.getNamedInputObjects().get(0));
        final JavaRDD<Row> predictedData;
        final Serializable model = input.getModel();
        if (model instanceof CollaborativeFilteringModel) {
            LOGGER.log(Level.INFO, "Collaborative filtering model found. Create matrix factorization model");
            //this is a very special model as we need to convert it to the real model first
            predictedData = CollaborativeFilteringJob.predict(input, namedObjects, rowRdd,
                (CollaborativeFilteringModel) model);
        } else {
            predictedData = ModelUtils.predict(input.getIncludeColumnIndices(), rowRdd, model);
        }
        LOGGER.log(Level.INFO, "Prediction done");
        namedObjects.addJavaRdd(input.getNamedOutputObjects().get(0), predictedData);
    }
}

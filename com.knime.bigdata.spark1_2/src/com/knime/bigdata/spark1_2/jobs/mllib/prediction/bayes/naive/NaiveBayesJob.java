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
package com.knime.bigdata.spark1_2.jobs.mllib.prediction.bayes.naive;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.api.java.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.ModelJobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.mllib.prediction.bayes.naive.NaiveBayesJobInput;
import com.knime.bigdata.spark1_2.base.NamedObjects;
import com.knime.bigdata.spark1_2.base.SparkJob;
import com.knime.bigdata.spark1_2.base.SupervisedLearnerUtils;

/**
 * runs MLlib Naive Bayes on a given RDD
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
@SparkClass
public class NaiveBayesJob implements SparkJob<NaiveBayesJobInput, ModelJobOutput> {

    private static final long serialVersionUID = 1L;


    private final static Logger LOGGER = Logger.getLogger(NaiveBayesJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public ModelJobOutput runJob(final SparkContext sparkContext, final NaiveBayesJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {
        LOGGER.log(Level.INFO, "starting Naive Bayes learner job...");
        final JavaRDD<Row> inputData = namedObjects.getJavaRdd(input.getFirstNamedInputObject());
        final JavaRDD<LabeledPoint> trainingsData = SupervisedLearnerUtils.getTrainingData(input, inputData);
        //cache input object to speedup calculation
        trainingsData.cache();
        final NaiveBayesModel model = NaiveBayes.train(trainingsData.rdd(), input.getLambda());
        SupervisedLearnerUtils.storePredictions(sparkContext, namedObjects, input, inputData, trainingsData, model,
            LOGGER);
        LOGGER.log(Level.INFO, " Naive Bayes Learner done");
        // note that with Spark 1.4 we can use PMML instead
        return new ModelJobOutput(model);

    }
}

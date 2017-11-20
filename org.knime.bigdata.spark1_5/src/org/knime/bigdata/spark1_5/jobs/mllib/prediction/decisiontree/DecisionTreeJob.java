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
package org.knime.bigdata.spark1_5.jobs.mllib.prediction.decisiontree;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.ModelJobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.DecisionTreeJobInput;
import org.knime.bigdata.spark1_5.api.NamedObjects;
import org.knime.bigdata.spark1_5.api.SparkJob;
import org.knime.bigdata.spark1_5.api.SupervisedLearnerUtils;

/**
 * runs MLlib DecisionTree on a given RDD to create a decision tree, model is returned as result
 *
 * @author koetter, dwk
 */
@SparkClass
public class DecisionTreeJob implements SparkJob<DecisionTreeJobInput, ModelJobOutput> {

    private static final long serialVersionUID = 1L;
    private final static Logger LOGGER = Logger.getLogger(DecisionTreeJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public ModelJobOutput runJob(final SparkContext sparkContext, final DecisionTreeJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {
        LOGGER.log(Level.INFO, "starting Decision Tree learner job...");
        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());
        final JavaRDD<LabeledPoint> inputRdd = SupervisedLearnerUtils.getTrainingData(input, rowRDD);

        final DecisionTreeModel model = execute(sparkContext, input, inputRdd);

        SupervisedLearnerUtils.storePredictions(sparkContext, namedObjects, input, rowRDD,
            inputRdd, model, LOGGER);
        LOGGER.log(Level.INFO, "Decision Tree Learner done");
        // note that with Spark 1.4 we can use PMML instead
        return new ModelJobOutput(model);

    }

    /**
     *
     * @param aContext
     * @param aConfig
     * @param aInputData - Training dataset: RDD of LabeledPoint. Labels should take values {0, 1, ..., numClasses-1}.
     * @return DecisionTreeModel
     * @throws KNIMESparkException
     */
    private DecisionTreeModel execute(final SparkContext sparkContext, final DecisionTreeJobInput input,
        final JavaRDD<LabeledPoint> aInputData) throws KNIMESparkException {
        aInputData.cache();
        final Map<Integer, Integer> nominalFeatureInfo = input.getNominalFeatureInfo().getMap();
        final Long numClasses = SupervisedLearnerUtils.getNoOfClasses(input, aInputData);
        LOGGER.log(Level.FINE, "Training decision tree for " + numClasses + " classes.");
        LOGGER.log(Level.FINE, "Training decision tree with info for " + nominalFeatureInfo.size()
            + " nominal features: ");

        if (!input.isClassification()) {
            return DecisionTree.trainRegressor(aInputData, nominalFeatureInfo, "variance", input.getMaxDepth(),
                input.getMaxNoOfBins());
        } else {
            try {
                return DecisionTree.trainClassifier(aInputData, numClasses.intValue(), nominalFeatureInfo,
                    input.getQualityMeasure().name(), input.getMaxDepth(), input.getMaxNoOfBins());
            } catch (Exception e) {
                throw new KNIMESparkException(e);
            }
        }
    }

}

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
package com.knime.bigdata.spark2_0.jobs.ml.prediction.decisiontree;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.ModelJobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.mllib.prediction.decisiontree.DecisionTreeJobInput;
import com.knime.bigdata.spark2_0.api.ModelUtils;
import com.knime.bigdata.spark2_0.api.NamedObjects;
import com.knime.bigdata.spark2_0.api.SparkJob;
import com.knime.bigdata.spark2_0.api.SupervisedLearnerUtils;

/**
 * runs ML DecisionTree on a given RDD to create a decision tree, model is returned as result
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
    public ModelJobOutput runJob(final SparkContext sparkContext, final DecisionTreeJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {
        LOGGER.log(Level.INFO, "starting Decision Tree learner job...");

        final ModelJobOutput model = SupervisedLearnerUtils.constructAndExecutePipeline(sparkContext, input,
            namedObjects.getDataFrame(input.getFirstNamedInputObject()), getConfiguredClassifier(input), true);
        //SupervisedLearnerUtils.storePredictions(sparkContext, namedObjects, input, rowRDD, /*TODO */rowRDD, model);
        LOGGER.log(Level.INFO, "Decision Tree Learner done");
        // note that with Spark 1.4 we can use PMML instead
        return model;

    }


    private DecisionTreeClassifier getConfiguredClassifier(final DecisionTreeJobInput aConfigurationInput) {
        //TODO - set other parameters
        return new DecisionTreeClassifier();
    }

    /**
     *
     * @param aContext
     * @param aConfig
     * @param aInputData - Training dataset: Dataset with feature vector and label column. Labels should take values {0,
     *            1, ..., numClasses-1}.
     * @return DecisionTreeModel
     * @throws KNIMESparkException
     */
    private ModelJobOutput execute(final SparkContext sparkContext, final DecisionTreeJobInput input,
        final Triple<String, String, Dataset<Row>> aTrainingData) throws KNIMESparkException {

        final String labelColumnName = aTrainingData.getLeft();
        final String featureColumn = aTrainingData.getMiddle();

        final String tmpClassColumn = ModelUtils.getTemporaryColumnName("label");

        final PipelineStage[] pipelineStages = new PipelineStage[4];

        final Dataset<Row> trainingData = aTrainingData.getRight();
        LOGGER.log(Level.FINE, "Training decision tree");
        //        LOGGER.log(Level.FINE,
        //            "Training decision tree with info for " + nominalFeatureInfo.size() + " nominal features: ");

        // Index labels, adding metadata to the label column.
        // TODO:: Fit on whole (!) dataset to include all labels in index.
        StringIndexerModel labelIndexer =
            new StringIndexer().setInputCol(labelColumnName).setOutputCol(tmpClassColumn).fit(trainingData);
        pipelineStages[0] = labelIndexer;
        // Automatically identify categorical features, and index them.
        //VectorIndexerModel[] featureIndexer = new VectorIndexerModel[nominalFeatureInfo.size()];

        LOGGER.log(Level.INFO, "Constructing decision tree training pipeline");
        int i = 1;

        pipelineStages[i++] = new VectorIndexer().setInputCol(featureColumn).setOutputCol(featureColumn + "_i")
            .setMaxCategories(10).fit(trainingData);

        // Train a DecisionTree model.
        //DecisionTreeClassifier dt =
        pipelineStages[i++] =
            new DecisionTreeClassifier().setLabelCol(tmpClassColumn).setFeaturesCol(featureColumn + "_i");

        // Convert indexed labels back to original labels.
        //IndexToString labelConverter =
        pipelineStages[i++] = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel")
            .setLabels(labelIndexer.labels());

        // Chain indexers and tree in a Pipeline.
        Pipeline pipeline = new Pipeline().setStages(pipelineStages);

        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(trainingData);
        return new ModelJobOutput(model);

        //        if (!input.isClassification()) {
        //            return DecisionTree.trainRegressor(aInputData, nominalFeatureInfo, "variance", input.getMaxDepth(),
        //                input.getMaxNoOfBins());
        //        } else {
        //            try {
        //                return DecisionTree.trainClassifier(aInputData, numClasses.intValue(), nominalFeatureInfo,
        //                    input.getQualityMeasure().name(), input.getMaxDepth(), input.getMaxNoOfBins());
        //            } catch (Exception e) {
        //                throw new KNIMESparkException(e);
        //            }
        //        }
    }

}

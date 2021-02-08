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
package org.knime.bigdata.spark3_0.jobs.ml.prediction.decisiontree.classification;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.Classifier;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTree;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTreeMetaData;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.classification.MLDecisionTreeClassificationLearnerJobInput;
import org.knime.bigdata.spark3_0.api.FileUtils;
import org.knime.bigdata.spark3_0.api.MLUtils;
import org.knime.bigdata.spark3_0.jobs.ml.prediction.MLClassificationLearnerJob;
import org.knime.bigdata.spark3_0.jobs.ml.prediction.decisiontree.MLDecisionTreeConverter;

/**
 * Learner job for spark.ml-based decision tree classification models.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLDecisionTreeClassificationLearnerJob extends MLClassificationLearnerJob<MLDecisionTreeClassificationLearnerJobInput> {

    private static final long serialVersionUID = 2157035134300354540L;

    /**
     * {@inheritDoc}
     */
    @Override
    protected Classifier<?, ?, ?> createClassifier(final MLDecisionTreeClassificationLearnerJobInput input) {
        return new DecisionTreeClassifier()
                .setImpurity(input.getQualityMeasure().toString())
                .setMaxDepth(input.getMaxDepth())
                .setMaxBins(input.getMaxNoOfBins())
                .setMinInstancesPerNode(input.getMinRowsPerTreeNode())
                .setMinInfoGain(input.getMinInformationGain())
                .setSeed(input.getSeed());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Path generateModelInterpreterData(final SparkContext sparkContext, final PipelineModel pipelineModel)
        throws IOException {

        final DecisionTreeClassificationModel dtModel =
            MLUtils.findFirstStageOfType(pipelineModel, DecisionTreeClassificationModel.class);

        final MLDecisionTree knimeTree = MLDecisionTreeConverter.convert(dtModel);
        final Path file = FileUtils.createTempFile(sparkContext, "mldecisiontree", null);
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file)))) {
            knimeTree.write(out);
        }

        return file;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected MLMetaData createModelMetaData(final PipelineModel pipelineModel) {

        final DecisionTreeClassificationModel dtModel =
            MLUtils.findFirstStageOfType(pipelineModel, DecisionTreeClassificationModel.class);

        final MLDecisionTreeMetaData metaData =
            new MLDecisionTreeMetaData(dtModel.numNodes(), dtModel.depth(), dtModel.featureImportances().toArray());
        MLUtils.addNominalValueMappingsToMetaData(pipelineModel, metaData);
        return metaData;
    }
}

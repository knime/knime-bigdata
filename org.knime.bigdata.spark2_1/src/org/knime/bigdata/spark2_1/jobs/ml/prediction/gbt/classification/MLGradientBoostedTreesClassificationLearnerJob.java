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
package org.knime.bigdata.spark2_1.jobs.ml.prediction.gbt.classification;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.GBTClassifier;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTreeEnsemble;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTreeEnsembleMetaData;
import org.knime.bigdata.spark.node.ml.prediction.gbt.classification.MLGradientBoostedTreesClassificationLearnerJobInput;
import org.knime.bigdata.spark2_1.api.FileUtils;
import org.knime.bigdata.spark2_1.api.MLUtils;
import org.knime.bigdata.spark2_1.jobs.ml.prediction.MLClassificationLearnerJob;
import org.knime.bigdata.spark2_1.jobs.ml.prediction.decisiontree.MLDecisionTreeConverter;

/**
 * Learner job for spark.ml-based gradient boosted trees classification models.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLGradientBoostedTreesClassificationLearnerJob
    extends MLClassificationLearnerJob<MLGradientBoostedTreesClassificationLearnerJobInput> {

    private static final long serialVersionUID = 2157035134300354540L;

    /**
     * {@inheritDoc}
     */
    @Override
    protected Predictor<?, ?, ?> createClassifier(final MLGradientBoostedTreesClassificationLearnerJobInput input) {
        return new GBTClassifier()
                .setImpurity(input.getQualityMeasure().toString())
                .setMaxDepth(input.getMaxDepth())
                .setMaxBins(input.getMaxNoOfBins())
                .setMinInstancesPerNode(input.getMinRowsPerTreeNode())
                .setMinInfoGain(input.getMinInformationGain())
                .setSeed(input.getSeed())
                .setSubsamplingRate(input.getSubsamplingRate())
                .setMaxIter(input.getMaxIterations())
                .setStepSize(input.getLearningRate());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Path generateModelInterpreterData(final SparkContext sparkContext, final PipelineModel pipelineModel)
        throws IOException {

        final GBTClassificationModel gbtModel =
            MLUtils.findFirstStageOfType(pipelineModel, GBTClassificationModel.class);

        final MLDecisionTreeEnsemble knimeTreeEnsemble = MLDecisionTreeConverter.convert(gbtModel);
        final Path file = FileUtils.createTempFile(sparkContext, "mldecisiontree", null);
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file)))) {
            knimeTreeEnsemble.write(out);
        }

        return file;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected MLMetaData createModelMetaData(final PipelineModel pipelineModel) {

        final GBTClassificationModel gbtModel =
            MLUtils.findFirstStageOfType(pipelineModel, GBTClassificationModel.class);

        final MLDecisionTreeEnsembleMetaData metaData = new MLDecisionTreeEnsembleMetaData(gbtModel.numTrees(),
            gbtModel.totalNumNodes(), gbtModel.treeWeights(), gbtModel.featureImportances().toArray());
        MLUtils.addNominalValueMappingsToMetaData(pipelineModel, metaData);
        return metaData;
    }
}

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
package org.knime.bigdata.spark3_3.jobs.ml.prediction.gbt.regression;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.GBTRegressor;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTreeEnsemble;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTreeEnsembleMetaData;
import org.knime.bigdata.spark.node.ml.prediction.gbt.regression.MLGradientBoostedTreesRegressionLearnerJobInput;
import org.knime.bigdata.spark3_3.api.FileUtils;
import org.knime.bigdata.spark3_3.api.MLUtils;
import org.knime.bigdata.spark3_3.jobs.ml.prediction.MLRegressionLearnerJob;
import org.knime.bigdata.spark3_3.jobs.ml.prediction.decisiontree.MLDecisionTreeConverter;

/**
 * Learner job for spark.ml-based gradient boosted trees regression models.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLGradientBoostedTreesRegressionLearnerJob
    extends MLRegressionLearnerJob<MLGradientBoostedTreesRegressionLearnerJobInput> {

    private static final long serialVersionUID = 2157035134300354540L;

    /**
     * {@inheritDoc}
     */
    @Override
    protected Predictor<?, ?, ?> createRegressor(final MLGradientBoostedTreesRegressionLearnerJobInput input) {
        return new GBTRegressor()
                .setMaxDepth(input.getMaxDepth())
                .setMaxBins(input.getMaxNoOfBins())
                .setMinInstancesPerNode(input.getMinRowsPerTreeNode())
                .setMinInfoGain(input.getMinInformationGain())
                .setSeed(input.getSeed())
                .setFeatureSubsetStrategy(input.getFeatureSubsetStrategy())
                .setSubsamplingRate(input.getSubsamplingRate())
                .setLossType(input.getLossFunction().name())
                .setMaxIter(input.getMaxIterations())
                .setStepSize(input.getLearningRate());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Path generateModelInterpreterData(final SparkContext sparkContext, final PipelineModel pipelineModel)
        throws IOException {

        final GBTRegressionModel gbtModel = MLUtils.findFirstStageOfType(pipelineModel, GBTRegressionModel.class);

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

        final GBTRegressionModel gbtModel = MLUtils.findFirstStageOfType(pipelineModel, GBTRegressionModel.class);

        final MLDecisionTreeEnsembleMetaData metaData = new MLDecisionTreeEnsembleMetaData(gbtModel.getNumTrees(),
            gbtModel.totalNumNodes(), gbtModel.treeWeights(), gbtModel.featureImportances().toArray());
        MLUtils.addNominalValueMappingsToMetaData(pipelineModel, metaData);
        return metaData;
    }
}

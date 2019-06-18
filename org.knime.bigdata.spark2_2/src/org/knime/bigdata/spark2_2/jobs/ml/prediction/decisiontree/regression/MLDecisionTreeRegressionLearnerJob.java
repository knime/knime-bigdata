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
package org.knime.bigdata.spark2_2.jobs.ml.prediction.decisiontree.regression;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTree;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTreeLearnerJobInput;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.MLDecisionTreeMetaData;
import org.knime.bigdata.spark2_2.api.FileUtils;
import org.knime.bigdata.spark2_2.api.MLUtils;
import org.knime.bigdata.spark2_2.jobs.ml.prediction.MLRegressionLearnerJob;
import org.knime.bigdata.spark2_2.jobs.ml.prediction.decisiontree.MLDecisionTreeConverter;

/**
 * Learner job for spark.ml-based decision tree regression models.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLDecisionTreeRegressionLearnerJob
    extends MLRegressionLearnerJob<MLDecisionTreeLearnerJobInput> {

    private static final long serialVersionUID = 2157035134300354540L;

    /**
     * {@inheritDoc}
     */
    @Override
    protected Predictor<?, ?, ?> createRegressor(final MLDecisionTreeLearnerJobInput input) {
        return new DecisionTreeRegressor()
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

        final DecisionTreeRegressionModel dtModel =
            MLUtils.findFirstStageOfType(pipelineModel, DecisionTreeRegressionModel.class);

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

        final DecisionTreeRegressionModel dtModel =
                MLUtils.findFirstStageOfType(pipelineModel, DecisionTreeRegressionModel.class);

        final MLDecisionTreeMetaData metaData =
            new MLDecisionTreeMetaData(dtModel.numNodes(), dtModel.depth(), dtModel.featureImportances().toArray());
        MLUtils.addNominalValueMappingsToMetaData(pipelineModel, metaData);
        return metaData;
    }
}

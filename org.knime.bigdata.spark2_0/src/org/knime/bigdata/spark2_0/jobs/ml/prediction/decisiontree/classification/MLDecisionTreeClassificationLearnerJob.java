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
package org.knime.bigdata.spark2_0.jobs.ml.prediction.decisiontree.classification;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.tree.CategoricalSplit;
import org.apache.spark.ml.tree.ContinuousSplit;
import org.apache.spark.ml.tree.InternalNode;
import org.apache.spark.ml.tree.Node;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.MLModelLearnerJobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.classification.MLDecisionTreeClassificationLearnerJobInput;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.classification.MLDecisionTreeMetaData;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.classification.MLDecisionTreeNode;
import org.knime.bigdata.spark2_0.api.FileUtils;
import org.knime.bigdata.spark2_0.api.MLUtils;
import org.knime.bigdata.spark2_0.api.NamedObjects;
import org.knime.bigdata.spark2_0.api.SparkJob;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLDecisionTreeClassificationLearnerJob
    implements SparkJob<MLDecisionTreeClassificationLearnerJobInput, MLModelLearnerJobOutput> {

    private static final long serialVersionUID = 1L;

    /**
     * {@inheritDoc}
     */
    @Override
    public MLModelLearnerJobOutput runJob(final SparkContext sparkContext,
        final MLDecisionTreeClassificationLearnerJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {

        final Dataset<Row> dataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());

        final List<String> actualFeatureColumns = new ArrayList<>();
        final List<PipelineStage> stages = new ArrayList<>();

        final String targetColumn = dataset.schema().fields()[input.getTargetColumnIndex()].name();
        final String indexedTargetColumn = targetColumn + "_" + UUID.randomUUID().toString();
        stages.add(new StringIndexer()
            .setInputCol(targetColumn)
            .setOutputCol(indexedTargetColumn)
            .setHandleInvalid("keep"));

        for (int featureColIndex : input.getColumnIdxs()) {
            final StructField field = dataset.schema().fields()[featureColIndex];

            if (field.dataType() == DataTypes.StringType) {
                final String indexedFeatureColumn = field.name() + "_" + UUID.randomUUID().toString();

                stages.add(new StringIndexer()
                    .setInputCol(field.name())
                    .setOutputCol(indexedFeatureColumn)
                    .setHandleInvalid("keep"));

                actualFeatureColumns.add(indexedFeatureColumn);
            } else {
                actualFeatureColumns.add(field.name());
            }
        }

        final String featureVectorColumn = "features_" + UUID.randomUUID().toString();
        final VectorAssembler vectorAssembler = new VectorAssembler()
            .setInputCols(actualFeatureColumns.toArray(new String[0]))
            .setOutputCol(featureVectorColumn);
        stages.add(vectorAssembler);

        stages.add(new DecisionTreeClassifier().setFeaturesCol(featureVectorColumn).setLabelCol(indexedTargetColumn)
            .setImpurity(input.getQualityMeasure().toString()).setMaxBins(input.getMaxNoOfBins())
            .setMaxDepth(input.getMaxDepth()).setMinInstancesPerNode(input.getMinRowsPerNodeChild())
            .setProbabilityCol("prediction_prob_" + UUID.randomUUID().toString()));

        final Pipeline pipeline = new Pipeline();
        pipeline.setStages(stages.toArray(new PipelineStage[0]));

        final PipelineModel model = pipeline.fit(dataset);

        Path serializedModelDir = null;
        Path serializedModelZip = null;
        try {
            serializedModelDir = FileUtils.createTempDir(sparkContext, "mlmodel");
            model.write().overwrite().save(serializedModelDir.toString());

            serializedModelZip = FileUtils.createTempFile(sparkContext, "mlmodel", ".zip");
            FileUtils.zipDirectory(serializedModelDir, serializedModelZip);
        } finally {
            if (serializedModelDir != null) {
                FileUtils.deleteRecursively(serializedModelDir);
            }
        }

        namedObjects.add(input.getNamedModelId(), model);

        final DecisionTreeClassificationModel dtModel = MLUtils.findFirstStageOfType(model, DecisionTreeClassificationModel.class);

        final MLDecisionTreeMetaData metaData = new MLDecisionTreeMetaData(dtModel.numNodes(), dtModel.depth());
        MLUtils.addNominalValueMappingsToMetaData(model, metaData);

        final Path modelInterpreterData = generateModelInterpreterData(sparkContext, dtModel);

        return new MLModelLearnerJobOutput(serializedModelZip, modelInterpreterData, metaData);

    }

    private static Path generateModelInterpreterData(final SparkContext sparkContext,
        final DecisionTreeClassificationModel dtModel) throws IOException {
        MLDecisionTreeNode rootNode = new MLDecisionTreeNode(null);
        copyAttributesRecursively(dtModel.rootNode(), null, rootNode);

        final Path file = FileUtils.createTempFile(sparkContext, "mldecisiontree", null);
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file)))) {
            writeRecursively(rootNode, out);
        }

        return file;
    }

    private static void writeRecursively(final MLDecisionTreeNode node, final DataOutputStream out) throws IOException {
        node.write(out);
        if (node.getLeftNode() != null) {
            writeRecursively((MLDecisionTreeNode)node.getLeftNode(), out);
            writeRecursively((MLDecisionTreeNode)node.getRightNode(), out);
        }
    }

    private static void copyAttributesRecursively(final Node from, final InternalNode fromParent,
        final MLDecisionTreeNode to) {

        to.setImpurity(from.impurity());
        to.setPrediction(from.prediction());

        if (fromParent != null) {
            if (fromParent.split() instanceof CategoricalSplit) {
                final CategoricalSplit parentSplit = (CategoricalSplit)fromParent.split();
                to.setCategorical(true);
                transferCategoricalSplit(from, fromParent, to, parentSplit);
            } else {
                final ContinuousSplit parentSplit = (ContinuousSplit)fromParent.split();
                to.setCategorical(false);
                to.setThreshold(parentSplit.threshold());
            }
        }

        if (from instanceof InternalNode) {
            final InternalNode internalFrom = (InternalNode)from;
            to.setGain(internalFrom.gain());
            to.setNumDescendants(from.numDescendants());
            to.setSplitFeature(internalFrom.split().featureIndex());

            MLDecisionTreeNode toLeftNode = new MLDecisionTreeNode(to);
            to.setLeftNode(toLeftNode);
            copyAttributesRecursively(internalFrom.leftChild(), internalFrom, toLeftNode);

            MLDecisionTreeNode toRightNode = new MLDecisionTreeNode(to);
            to.setRightNode(toRightNode);
            copyAttributesRecursively(internalFrom.rightChild(), internalFrom, toRightNode);
        }
    }

    private static void transferCategoricalSplit(final Node from, final InternalNode fromParent,
        final MLDecisionTreeNode to, final CategoricalSplit parentSplit) {

        List<Object> categories = new LinkedList<>();

        if (from == fromParent.leftChild()) {
            for (double catIndex : parentSplit.leftCategories()) {
                categories.add((int)catIndex);
            }
        } else {
            for (double catIndex : parentSplit.rightCategories()) {
                categories.add((int)catIndex);
            }
        }
        to.setCategories(categories);
    }
}

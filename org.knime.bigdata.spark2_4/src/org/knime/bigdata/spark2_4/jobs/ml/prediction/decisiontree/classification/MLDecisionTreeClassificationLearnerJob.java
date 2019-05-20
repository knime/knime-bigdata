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
package org.knime.bigdata.spark2_4.jobs.ml.prediction.decisiontree.classification;

import java.nio.file.Path;
import java.util.ArrayList;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.MLModelLearnerJobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.classification.MLDecisionTreeClassificationLearnerJobInput;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.classification.MLDecisionTreeMetaData;
import org.knime.bigdata.spark2_4.api.FileUtils;
import org.knime.bigdata.spark2_4.api.MLUtils;
import org.knime.bigdata.spark2_4.api.NamedObjects;
import org.knime.bigdata.spark2_4.api.SparkJob;

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

        // corner cases:
        // - during training:
        //      - missing values in features and/or target
        // - during testing/predicting
        //      - feature columns are missing
        //      - feature columns have the wrong type (e.g. string instead of numeric. as long as they are numeric it is probably not an issue)
        //      - feature columns contain missing values, NaNs, etc
        //      - nominal columns contain previously unseen labels

        // features:
        // - treat all string features with string indexer
        // - assemble vector
        // - use vector indexer with maxCategories >= maxCategory over all indexed string features
        // - add decision tree model with parames from job input
        // - IndexToString to map predicted values back to strings

        final Dataset<Row> dataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());

        final List<String> actualFeatureColumns = new ArrayList<>();
        final List<PipelineStage> stages = new ArrayList<>();

        final String targetColumn = dataset.schema().fields()[input.getTargetColumnIndex()].name();
        final String indexedTargetColumn = targetColumn + "_" + UUID.randomUUID().toString();
        stages.add(new StringIndexer().setInputCol(targetColumn).setOutputCol(indexedTargetColumn));

        for (int featureColIndex : input.getColumnIdxs()) {
            final StructField field = dataset.schema().fields()[featureColIndex];

            if (field.dataType() == DataTypes.StringType) {
                final String indexedFeatureColumn = field.name() + "_" + UUID.randomUUID().toString();

                stages.add(new StringIndexer().setInputCol(field.name()).setOutputCol(indexedFeatureColumn));

                actualFeatureColumns.add(indexedFeatureColumn);
            } else {
                actualFeatureColumns.add(field.name());
            }
        }

        final String featureVectorColumn = "features_" + UUID.randomUUID().toString();
        final VectorAssembler vectorAssembler = new VectorAssembler()
            .setInputCols(actualFeatureColumns.toArray(new String[0])).setOutputCol(featureVectorColumn);
        // FIXME: strategy to handle missing values
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

        return new MLModelLearnerJobOutput(serializedModelZip, metaData);

    }
}

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
 *   Created on Jun 17, 2019 by bjoern
 */
package org.knime.bigdata.spark2_4.jobs.ml.prediction;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.MLModelLearnerJobOutput;
import org.knime.bigdata.spark.core.job.NamedModelLearnerJobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;
import org.knime.bigdata.spark2_4.api.FileUtils;
import org.knime.bigdata.spark2_4.api.NamedObjects;
import org.knime.bigdata.spark2_4.api.SparkJob;

/**
 * Abstract superclass for spark.ml-based regression model learner jobs. This class handles all of the boilerplate stuff
 * such as assembling a pipeline to map nominal features to numerical ones, saving the pipeline etc. Subclasses only
 * need to implements methods that provide the regressor and some meta data.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @param <I> The job input type.
 */
@SparkClass
public abstract class MLRegressionLearnerJob<I extends NamedModelLearnerJobInput>
    implements SparkJob<I, MLModelLearnerJobOutput> {

    private static final long serialVersionUID = 7648804611106075408L;

    /**
     * {@inheritDoc}
     */
    @Override
    public MLModelLearnerJobOutput runJob(final SparkContext sparkContext, final I input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final Dataset<Row> dataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());

        final List<String> actualFeatureColumns = new ArrayList<>();
        final List<PipelineStage> stages = new ArrayList<>();

        final String targetColumn = dataset.schema().fields()[input.getTargetColumnIndex()].name();

        // index all nominal feature columns
        for (int featureColIndex : input.getColumnIdxs()) {
            final StructField field = dataset.schema().fields()[featureColIndex];

            if (field.dataType() == DataTypes.StringType) {
                final String indexedFeatureColumn = field.name() + "_" + UUID.randomUUID().toString();

                stages.add(new StringIndexer().setInputCol(field.name()).setOutputCol(indexedFeatureColumn)
                    .setHandleInvalid("keep"));

                actualFeatureColumns.add(indexedFeatureColumn);
            } else {
                actualFeatureColumns.add(field.name());
            }
        }

        // assemble vector
        final String featureVectorColumn = "features_" + UUID.randomUUID().toString();
        final VectorAssembler vectorAssembler =
            new VectorAssembler().setInputCols(actualFeatureColumns.toArray(new String[0]))
                .setOutputCol(featureVectorColumn).setHandleInvalid("keep");
        stages.add(vectorAssembler);

        // add the regressor
        final String predictionCol = "prediction_" + UUID.randomUUID().toString();
        final Predictor<?, ?, ?> classifier = createRegressor(input);
        classifier.setFeaturesCol(featureVectorColumn)
            .setLabelCol(targetColumn)
            .setPredictionCol(predictionCol);
        stages.add(classifier);

        // assemble pipeline and fit
        final Pipeline pipeline = new Pipeline();
        pipeline.setStages(stages.toArray(new PipelineStage[0]));
        final PipelineModel model = pipeline.fit(dataset);

        Path serializedModelDir = null;
        Path serializedModelZip = null;
        try {
            serializedModelDir = FileUtils.createTempDir(sparkContext, "mlmodel");
            model.write().overwrite().save(serializedModelDir.toUri().toString());

            serializedModelZip = FileUtils.createTempFile(sparkContext, "mlmodel", ".zip");
            FileUtils.zipDirectory(serializedModelDir, serializedModelZip);
        } finally {
            if (serializedModelDir != null) {
                FileUtils.deleteRecursively(serializedModelDir);
            }
        }

        namedObjects.add(input.getNamedModelId(), model);

        final MLMetaData modelMetaData = createModelMetaData(model);

        final Path modelInterpreterData = generateModelInterpreterData(sparkContext, model);

        return new MLModelLearnerJobOutput(serializedModelZip, modelInterpreterData, modelMetaData);
    }

    /**
     * Subclasses must implement this method to create a regressor.
     *
     * @param input The job input.
     * @return a regressor.
     */
    protected abstract Predictor<?, ?, ?> createRegressor(final I input);

    /**
     * Subclasses must implement this method to provide some meta data on the learned model.
     *
     * @param model The learned model.
     * @return metadata taken from the learned model, or null if none should be provided.
     */
    protected abstract MLMetaData createModelMetaData(final PipelineModel model);

    /**
     * Subclasses must implement this method to provide additional data for the model interpreter in KNIME.
     *
     * @param sparkContext The underlying Spark context.
     * @param model The learned model.
     * @return a file that contains custom data for the model interpreter, or null if none should be provided.
     * @throws Exception
     */
    protected abstract Path generateModelInterpreterData(final SparkContext sparkContext, final PipelineModel model)
        throws Exception;
}

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
package org.knime.bigdata.spark3_5.jobs.ml.prediction;

import static org.knime.bigdata.spark3_5.api.SparkExceptionUtil.isMissingValueException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.Classifier;
import org.apache.spark.ml.classification.ProbabilisticClassifier;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
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
import org.knime.bigdata.spark3_5.api.DistributedFileUtils;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;

/**
 * Abstract superclass for spark.ml-based classification model learner jobs. This class handles all of the boilerplate
 * stuff such as assembling a pipeline to map nominal columns to numerical ones, saving the pipeline etc. Subclasses
 * only need to implements methods that provide the classifier and some meta data.
 *
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @param <I> The job input type.
 */
@SparkClass
public abstract class MLClassificationLearnerJob<I extends NamedModelLearnerJobInput>
    implements SparkJob<I, MLModelLearnerJobOutput> {

    private static final long serialVersionUID = 1268674094837876483L;

    /**
     * {@inheritDoc}
     */
    @Override
    public MLModelLearnerJobOutput runJob(final SparkContext sparkContext, final I input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String handleInvalid = input.handleInvalid("keep"); // backward compatibility
        final Dataset<Row> dataset;
        if (handleInvalid.equalsIgnoreCase("skip")) {
            dataset = dropRowsWithMissingValues(namedObjects.getDataFrame(input.getFirstNamedInputObject()), input);
        } else {
            dataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        }

        final List<String> actualFeatureColumns = new ArrayList<>();
        final List<String> oneHotFeatureColumns = new ArrayList<>();
        final List<PipelineStage> stages = new ArrayList<>();

        final String targetColumn = dataset.schema().fields()[input.getTargetColumnIndex()].name();

        // index the nominal target column
        final String indexedTargetColumn = targetColumn + "_" + UUID.randomUUID().toString();
        final StringIndexer targetColIndexer = new StringIndexer() //
            .setInputCol(targetColumn) //
            .setOutputCol(indexedTargetColumn) //
            .setStringOrderType(getTargetStringOrderType()) //
            .setHandleInvalid(handleInvalid);
        final StringIndexerModel targetColIndexerModel = targetColIndexer.fit(dataset);
        stages.add(targetColIndexer);

        // index all nominal feature columns
        for (int featureColIndex : input.getColumnIdxs()) {
            final StructField field = dataset.schema().fields()[featureColIndex];

            if (field.dataType() == DataTypes.StringType) {
                final String indexedFeatureColumn = field.name() + "_" + UUID.randomUUID().toString();

                stages.add(new StringIndexer() //
                    .setInputCol(field.name()) //
                    .setOutputCol(indexedFeatureColumn) //
                    .setStringOrderType(getNominalFeatureStringOrderType()) //
                    .setHandleInvalid(handleInvalid));

                actualFeatureColumns.add(indexedFeatureColumn);
                if (useNominalDummyVariables()) {
                    oneHotFeatureColumns.add(indexedFeatureColumn);
                }
            } else {
                // note: missing values are handled in vector assembler below
                actualFeatureColumns.add(field.name());
            }
        }

        if (!oneHotFeatureColumns.isEmpty()) {
            final String[] inputColumns = oneHotFeatureColumns.toArray(new String[0]);
            final String[] outputColumns = new String[inputColumns.length];
            for (int i = 0; i < inputColumns.length; i++) {
                final String outputCol = inputColumns[i] + "_one_hot";
                outputColumns[i] = outputCol;
                // replace feature column with one hot output column
                actualFeatureColumns.set(actualFeatureColumns.indexOf(inputColumns[i]), outputCol);
            }
            final OneHotEncoder oneHotEncoder = new OneHotEncoder() //
                    .setInputCols(inputColumns) //
                    .setOutputCols(outputColumns) //
                    .setDropLast(true) //
                    .setHandleInvalid("error"); // inputs are the string indexer from above and they should always present
            stages.add(oneHotEncoder);
        }

        // assemble vector
        final String featureVectorColumn = "features_" + UUID.randomUUID().toString();
        final VectorAssembler vectorAssembler = new VectorAssembler() //
            .setInputCols(actualFeatureColumns.toArray(new String[0])) //
            .setOutputCol(featureVectorColumn) //
            .setHandleInvalid(handleInvalid); // handle missing values in non categorical columns
        stages.add(vectorAssembler);

        // add the classifier
        final String predictionCol = "prediction_" + UUID.randomUUID().toString();
        final Classifier<?, ?, ?> classifier = createClassifier(input);
        classifier.setFeaturesCol(featureVectorColumn)
            .setLabelCol(indexedTargetColumn)
            .setPredictionCol(predictionCol)
            .setRawPredictionCol("rawprediction_" + UUID.randomUUID().toString());

        if (classifier instanceof ProbabilisticClassifier) {
            ((ProbabilisticClassifier<?, ?, ?>)classifier)
                .setProbabilityCol("prediction_prob_" + UUID.randomUUID().toString());
        }
        stages.add(classifier);

        // map indexed target column back to strings
        final IndexToString classIndexToString = new IndexToString()
                .setLabels(targetColIndexerModel.labels())
                .setInputCol(predictionCol)
                .setOutputCol("prediction_string_" + UUID.randomUUID().toString());
        stages.add(classIndexToString);

        // assemble pipeline and fit
        final Pipeline pipeline = new Pipeline();
        pipeline.setStages(stages.toArray(new PipelineStage[0]));
        final PipelineModel model;
        try {
            model = pipeline.fit(dataset);
        } catch (final Exception e) { // NOSONAR
            if (handleInvalid.equalsIgnoreCase("error") && isMissingValueException(e)) {
                throw new KNIMESparkException("Observed missing values in input columns.", e);
            } else {
                throw e;
            }
        }
        namedObjects.add(input.getNamedModelId(), model);

        final Path serializedModelZip = DistributedFileUtils.zipModel(sparkContext, model);
        final MLMetaData modelMetaData = createModelMetaData(model);
        final Path modelInterpreterData = generateModelInterpreterData(sparkContext, model);

        return new MLModelLearnerJobOutput(serializedModelZip, modelInterpreterData, modelMetaData);
    }

    /**
     * Drop all rows in given data set that contains any missing value in target or feature columns.
     */
    private Dataset<Row> dropRowsWithMissingValues(final Dataset<Row> dataset, final I input) {
        final String[] fields = dataset.schema().fieldNames();
        final ArrayList<String> columns = new ArrayList<>();
        columns.add(fields[input.getTargetColumnIndex()]);
        for (int featureColIndex : input.getColumnIdxs()) {
            columns.add(fields[featureColIndex]);
        }
        return dataset.na().drop(columns.toArray(new String[0]));
    }

    /**
     * Subclasses must implement this method to create a classifier.
     *
     * @param input The job input.
     * @return a classifier.
     */
    protected abstract Classifier<?, ?, ?> createClassifier(final I input);

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

    /**
     * Subclasses can overwrite this method to convert nominal values into dummy variables.
     *
     * @return {@code true} if a one hot encoder should be used on nominal values
     */
    protected boolean useNominalDummyVariables() {
        return false;
    }

    /**
     * How to order target string values before indexing them.
     *
     * @return string indexer string order type
     */
    protected String getTargetStringOrderType() {
        return "frequencyDesc"; // Spark default / backward compatibility
    }

    /**
     * How to order nominal feature string values before indexing them.
     *
     * @return string indexer string order type
     */
    protected String getNominalFeatureStringOrderType() {
        return "frequencyDesc"; // Spark default / backward compatibility
    }
}

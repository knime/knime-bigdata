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
package org.knime.bigdata.spark2_2.jobs.ml.predictor.classification;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.classification.ProbabilisticClassificationModel;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.ml.prediction.predictor.classification.MLPredictorClassificationJobInput;
import org.knime.bigdata.spark2_2.api.MLUtils;
import org.knime.bigdata.spark2_2.api.NamedObjects;
import org.knime.bigdata.spark2_2.api.SimpleSparkJob;

/**
 * Applies previously learned {@link PipelineModel} with a classification model to a given data frame.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLPredictorClassificationJob implements SimpleSparkJob<MLPredictorClassificationJobInput> {
    private static final long serialVersionUID = 1L;

    @Override
    public void runJob(final SparkContext sparkContext, final MLPredictorClassificationJobInput input,
        final NamedObjects namedObjects) throws Exception {

        // fetch named objects
        final Dataset<Row> inputDataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final PipelineModel model = namedObjects.get(input.getNamedModelId());

        final Set<String> features = getFeatures(model, inputDataset);
        final Dataset<Row> withMVs = MLUtils.retainRowsWithMissingValuesInFeatures(inputDataset, features);
        final Dataset<Row> withoutMVs = MLUtils.retainRowsWithoutMissingValuesInFeatures(inputDataset, features);

        // apply pipeline model (classification)
        final Dataset<Row> predictedDataset = model.transform(withoutMVs);

        // clean up columns and unpack conditional class probabilities if desired
        final Dataset<Row> cleanedDataset = predictedDataset.selectExpr(
            determinePredictedRowOutputColumns(inputDataset, input, model));

        final Dataset<Row> expandedWithMVs = withMVs.selectExpr(fillRowsWithMVsWithDummyValues(withMVs, cleanedDataset.schema()));

        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), cleanedDataset.union(expandedWithMVs));
    }

    public static String[] fillRowsWithMVsWithDummyValues(final Dataset<Row> funnyRows, final StructType schema) {
        final List<String> outputColumns = new LinkedList<>();

        // first append all input columns
        outputColumns.add("*");

        // then add zeros in conditional class probability columns, if present
        for (int i = funnyRows.schema().length(); i < schema.length() -1; i++) {
            outputColumns.add(String.format("0 as `%s`", schema.fields()[i].name()));
        }

        // then add null value in prediction column
        outputColumns.add(String.format("NULL as `%s`", schema.fields()[schema.length()-1].name()));

        return outputColumns.toArray(new String[0]);
    }

    public static Set<String> getFeatures(final PipelineModel model, final Dataset<Row> dataset) {
        final Set<String> features = new HashSet<>();

        final Set<String> toIgnore = new HashSet<>();

        for(Transformer stage : model.stages()) {
            if (stage instanceof StringIndexerModel ) {
                final StringIndexerModel stringIndexer = (StringIndexerModel) stage;
                final String maybeFeature = stringIndexer.getInputCol();
                if (dataset.schema().getFieldIndex(maybeFeature).isDefined()) {
                    features.add(maybeFeature);
                    toIgnore.add(stringIndexer.getOutputCol());
                }
            } else if (stage instanceof VectorAssembler) {
                final VectorAssembler vecAssembler = (VectorAssembler) stage;
                for (String maybeFeature : vecAssembler.getInputCols()) {
                    if (!toIgnore.contains(maybeFeature)) {
                        features.add(maybeFeature);
                    }
                }
                break;
            }
        }
        return features;
    }

    private static String[] determinePredictedRowOutputColumns(final Dataset<Row> inputDataset,
        final MLPredictorClassificationJobInput input, final PipelineModel model) {

        final IndexToString predictionColumnStringifier =
                MLUtils.findFirstStageOfType(model, IndexToString.class);

        final List<String> outputColumns = new LinkedList<>();

        // first retain all input columns
        for (String inputColumn : inputDataset.columns()) {
            outputColumns.add(String.format("`%s`", inputColumn));
        }

        // then add conditional class probabilities, if desired
        if (input.appendProbabilityColumns()) {
            final String[] targetColumnLabels = predictionColumnStringifier.getLabels();

            // the index of each label in the probability vector column
            final Map<String, Integer> labelIndicesInVector = createLabelIndicesMap(targetColumnLabels);

            // target column labels sorted in ascending order
            final String[] sortedTargetColumnLabels = sortTargetColumnLabels(targetColumnLabels);

            // the name of the probability vector column
            final String probabilityColumn =
                MLUtils.findFirstStageOfType(model, ProbabilisticClassificationModel.class).getProbabilityCol();

            appendProbabilityColumns(input, labelIndicesInVector, sortedTargetColumnLabels, probabilityColumn,
                outputColumns);
        }

        // then retain the prediction column
        outputColumns.add(String.format("`%s` as `%s`",
            predictionColumnStringifier.getOutputCol(),
            input.getPredictionColumn()));

        return outputColumns.toArray(new String[0]);
    }

    private static void appendProbabilityColumns(final MLPredictorClassificationJobInput input,
        final Map<String, Integer> labelIndicesInVector, final String[] sortedTargetColumnLabels,
        final String probabilityColumn, final List<String> outputColumns) {

        for (int i = 0; i < sortedTargetColumnLabels.length; i++) {
            final int labelIndexInVector = labelIndicesInVector.get(sortedTargetColumnLabels[i]);

            outputColumns.add(String.format("vectorToArray(`%s`)[%d] as `P (%s=%s)%s`",
                probabilityColumn, // the vector-typed probability column
                labelIndexInVector, // the vector index for the current label
                input.getOriginalTargetColumn(), // the name of the original target column during learning
                sortedTargetColumnLabels[i], // the current label
                input.getProbabilityColumnSuffix())); // the probability column suffix
        }
    }

    private static String[] sortTargetColumnLabels(final String[] targetColumnLabels) {
        final String[] toReturn = targetColumnLabels.clone();
        Arrays.sort(toReturn);
        return toReturn;
    }

    private static Map<String, Integer> createLabelIndicesMap(final String[] targetColumnLabels) {
        final Map<String, Integer> toReturn = new HashMap<>();
        for (int i = 0; i < targetColumnLabels.length; i++) {
            toReturn.put(targetColumnLabels[i], i);
        }
        return toReturn;
    }
}

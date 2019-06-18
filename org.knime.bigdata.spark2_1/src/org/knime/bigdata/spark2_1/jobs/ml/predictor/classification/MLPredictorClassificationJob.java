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
package org.knime.bigdata.spark2_1.jobs.ml.predictor.classification;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.ClassificationModel;
import org.apache.spark.ml.classification.ProbabilisticClassificationModel;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.ml.prediction.predictor.classification.MLPredictorClassificationJobInput;
import org.knime.bigdata.spark2_1.api.MLUtils;
import org.knime.bigdata.spark2_1.api.NamedObjects;
import org.knime.bigdata.spark2_1.api.SimpleSparkJob;

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

        final IndexToString targetColumnStringifier =
                MLUtils.findFirstStageOfType(model, IndexToString.class);
        targetColumnStringifier.setOutputCol(input.getPredictionColumn());

        // apply pipeline model (classification)
        final Dataset<Row> predictedDataset = model.transform(inputDataset);

        // clean up columns and unpack conditional class probabilities if desired
        final ClassificationModel<?, ?> classificationModel =
            MLUtils.findFirstStageOfType(model, ClassificationModel.class);

        final Dataset<Row> cleanedDataset = predictedDataset.selectExpr(
            determineOutputColumns(inputDataset, input, classificationModel, targetColumnStringifier.getLabels()));

        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), cleanedDataset);
    }

    private static String[] determineOutputColumns(final Dataset<Row> inputDataset,
        final MLPredictorClassificationJobInput input, final ClassificationModel<?, ?> classificationModel,
        final String[] targetColumnLabels) {

        final List<String> outputColumns = new LinkedList<>();

        // first retain all input columns
        for (String inputColumn : inputDataset.columns()) {
            outputColumns.add(String.format("`%s`", inputColumn));
        }

        // then retain the desired prediction column
        outputColumns.add(String.format("`%s`", input.getPredictionColumn()));

        // then add conditional class probabilities, if desired
        if (input.appendProbabilityColumns()) {
            final ProbabilisticClassificationModel<?, ?> probClassificationModel =
                (ProbabilisticClassificationModel<?, ?>)classificationModel;

            for (int i = 0; i < targetColumnLabels.length; i++) {
                outputColumns.add(String.format("vectorToArray(`%s`)[%d] as `P (%s=%s)%s`",
                    probClassificationModel.getProbabilityCol(), i, input.getPredictionColumn(),
                    targetColumnLabels[i], input.getProbabilityColumnSuffix()));
            }

        }
        return outputColumns.toArray(new String[0]);
    }
}

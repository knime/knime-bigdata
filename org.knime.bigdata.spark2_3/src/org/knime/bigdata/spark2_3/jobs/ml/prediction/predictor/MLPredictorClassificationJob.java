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
package org.knime.bigdata.spark2_3.jobs.ml.prediction.predictor;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.ClassificationModel;
import org.apache.spark.ml.classification.ProbabilisticClassificationModel;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.ml.prediction.predictor.classification.MLPredictorClassificationJobInput;
import org.knime.bigdata.spark2_3.api.MLUtils;
import org.knime.bigdata.spark2_3.api.NamedObjects;
import org.knime.bigdata.spark2_3.api.SimpleSparkJob;

/**
 * Applies previously learned {@link PipelineModel} to a given data frame.
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

        // apply pipeline model (classification)
        final Dataset<Row> predictedDataset = model.transform(inputDataset);

        // translate numeric labels back to strings, clean up columns
        // and unpack conditional class probabilities if desired
        final ClassificationModel<?, ?> classificationModel =
            MLUtils.findFirstStageOfType(model, ClassificationModel.class);
        final StringIndexerModel targetColumnStringIndexer =
            MLUtils.findFirstStageOfType(model, StringIndexerModel.class);
        final IndexToString classIndexToString = new IndexToString().setInputCol(classificationModel.getPredictionCol())
            .setOutputCol(input.getPredictionColumn()).setLabels(targetColumnStringIndexer.labels());
        final Dataset<Row> predictedWithLabels = classIndexToString.transform(predictedDataset)
            .selectExpr(determineOutputColumns(inputDataset, input, classificationModel, targetColumnStringIndexer));

        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), predictedWithLabels);
    }

    private static String[] determineOutputColumns(final Dataset<Row> inputDataset,
        final MLPredictorClassificationJobInput input, final ClassificationModel<?, ?> classificationModel,
        final StringIndexerModel targetColIndexerModel) {

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

            final String[] targetColumnLabels = targetColIndexerModel.labels();
            for (int i = 0; i < targetColumnLabels.length; i++) {
                outputColumns.add(String.format("vectorToArray(`%s`)[%d] as `P (%s=%s)%s`",
                    probClassificationModel.getProbabilityCol(), i, targetColIndexerModel.getInputCol(),
                    targetColumnLabels[i], input.getProbabilityColumnSuffix()));
            }

        }
        return outputColumns.toArray(new String[0]);
    }
}

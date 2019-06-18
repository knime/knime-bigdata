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
package org.knime.bigdata.spark2_2.jobs.ml.predictor.regression;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PredictionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.ml.prediction.predictor.regression.MLPredictorRegressionJobInput;
import org.knime.bigdata.spark2_2.api.MLUtils;
import org.knime.bigdata.spark2_2.api.NamedObjects;
import org.knime.bigdata.spark2_2.api.SimpleSparkJob;

/**
 * Applies previously learned {@link PipelineModel} with a regression model to a given data frame.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLPredictorRegressionJob implements SimpleSparkJob<MLPredictorRegressionJobInput> {
    private static final long serialVersionUID = 1L;

    @Override
    public void runJob(final SparkContext sparkContext, final MLPredictorRegressionJobInput input,
        final NamedObjects namedObjects) throws Exception {

        // fetch named objects
        final Dataset<Row> inputDataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final PipelineModel model = namedObjects.get(input.getNamedModelId());

        final PredictionModel<?, ?> regressionModel = MLUtils.findFirstStageOfType(model, PredictionModel.class);
        regressionModel.setPredictionCol(input.getPredictionColumn());

        // apply pipeline model (classification)
        final Dataset<Row> predictedDataset = model.transform(inputDataset);

        // clean up columns and unpack conditional class probabilities if desired
        final Dataset<Row> cleanedDataset = predictedDataset.selectExpr(determineOutputColumns(inputDataset, input));

        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), cleanedDataset);
    }

    private static String[] determineOutputColumns(final Dataset<Row> inputDataset,
        final MLPredictorRegressionJobInput input) {

        final List<String> outputColumns = new LinkedList<>();

        // first retain all input columns
        for (String inputColumn : inputDataset.columns()) {
            outputColumns.add(String.format("`%s`", inputColumn));
        }

        // then retain the desired prediction column
        outputColumns.add(String.format("`%s`", input.getPredictionColumn()));

        return outputColumns.toArray(new String[0]);
    }
}

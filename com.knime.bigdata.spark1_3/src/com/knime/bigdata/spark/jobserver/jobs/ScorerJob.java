/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.NumericScorerData;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.ScorerData;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

import scala.Tuple2;
import spark.jobserver.SparkJobValidation;

/**
 * computes classification / regression scores
 *
 * @author dwk
 */
public class ScorerJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(ScorerJob.class.getName());

    /**
     * index of column with actual (true) class or regression values
     */
    public static final String PARAM_ACTUAL_COL_INDEX = "actualColIndex";

    /**
     * index of column with predicted class or regression values
     */
    public static final String PARAM_PREDICTION_COL_INDEX = "predictionColIndex";

    /**
     * is this a classification or regression task?
     */
    public static final String PARAM_IS_CLASSIFICATION = "isClassification";

    Logger getLogger() {
        return LOGGER;
    }

    String getAlgName() {
        return "Scorer";
    }

    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        if (!aConfig.hasInputParameter(PARAM_ACTUAL_COL_INDEX)) {
            msg = "Input parameter '" + PARAM_ACTUAL_COL_INDEX + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_PREDICTION_COL_INDEX)) {
            msg = "Input parameter '" + PARAM_PREDICTION_COL_INDEX + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_IS_CLASSIFICATION)) {
            msg = "Input parameter '" + PARAM_IS_CLASSIFICATION + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(KnimeSparkJob.PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + KnimeSparkJob.PARAM_INPUT_TABLE + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }

        return ValidationResultConverter.valid();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JobResult runJobWithContext(final SparkContext aSparkContext, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        SupervisedLearnerUtils.validateInput(aConfig, this, getLogger());
        getLogger().log(Level.INFO, "START " + getAlgName() + " job...");

        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final boolean isClassification = aConfig.getInputParameter(PARAM_IS_CLASSIFICATION, Boolean.class);
        final Serializable scores;
        if (isClassification) {
            scores = scoreClassification(aConfig, rowRDD);
        } else {
            scores = scoreRegression(aConfig, rowRDD);
        }
        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(scores);

        getLogger().log(Level.INFO, "DONE " + getAlgName() + " job...");
        return res;
    }

    static Serializable scoreClassification(final JobConfig aConfig, final JavaRDD<Row> aRowRDD) {

        final Integer classCol = aConfig.getInputParameter(PARAM_ACTUAL_COL_INDEX, Integer.class);
        final Integer predictionCol = aConfig.getInputParameter(PARAM_PREDICTION_COL_INDEX, Integer.class);

        Map<Tuple2<Object, Object>, Integer> counts = RDDUtilsInJava.aggregatePairs(aRowRDD, classCol, predictionCol);

        List<Object> labels = SupervisedLearnerUtils.getDistinctValuesOfColumn(aRowRDD, classCol).collect();
        final int[][] confusionMatrix = new int[labels.size()][];
        for (int i = 0; i < confusionMatrix.length; i++) {
            confusionMatrix[i] = new int[labels.size()];
            Arrays.fill(confusionMatrix[i], 0);
        }
        int i = 0;
        int falseCount = 0;
        int correctCount = 0;
        for (Object label : labels) {
            int j = 0;
            for (Object predictedLabel : labels) {
                final Tuple2<Object, Object> key = new Tuple2<>(label, predictedLabel);
                if (counts.containsKey(key)) {
                    final int count = counts.get(key).intValue();
                    confusionMatrix[i][j] = count;
                    if (i == j) {
                        correctCount += count;
                    } else {
                        falseCount += count;
                    }
                }
                j++;
            }
            i++;
        }
        return new ScorerData(confusionMatrix, aRowRDD.count(), falseCount, correctCount, classCol, predictionCol,
            labels);

    }

    //    - Numeric Scorer liefert R^2, mean absolute error, mean absolute error,
    //    root mean square error und mean signed difference. In KNIME ist das die
    //    Klasse org.knime.base.node.mine.scorer.numeric.NumericScorerNodeModel

    private final static int REFERENCE_IX = 0;
    private final static int ABBS_ERROR_IX = 1;
    private final static int SQUARED_ERROR_IX =2 ;
    private final static int SIGNED_DIFF_IX = 3;

    static Serializable scoreRegression(final JobConfig aConfig, final JavaRDD<Row> aRowRDD) {
        final Integer classCol = aConfig.getInputParameter(PARAM_ACTUAL_COL_INDEX, Integer.class);
        final Integer predictionCol = aConfig.getInputParameter(PARAM_PREDICTION_COL_INDEX, Integer.class);

        final JavaRDD<Row> filtered = aRowRDD.filter(new Function<Row, Boolean>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(final Row aRow) throws Exception {
                return !aRow.isNullAt(classCol);
            }
        });
        final JavaRDD<Double[]> stats = filtered.map(new Function<Row, Double[]>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Double[] call(final Row aRow) {

                final double ref = RDDUtils.getDouble(aRow, classCol);
                final double pred = RDDUtils.getDouble(aRow, predictionCol);
                //observed, abs err, squared error, signed diff
                return new Double[]{ref, Math.abs(ref - pred), Math.pow(ref - pred, 2.0), pred - ref};
            }
        });

        Double[] means = stats.reduce(new Function2<Double[], Double[], Double[]>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Double[] call(final Double[] arg0, final Double[] arg1) throws Exception {
                return new Double[]{arg0[REFERENCE_IX] + arg1[REFERENCE_IX], arg0[ABBS_ERROR_IX] + arg1[ABBS_ERROR_IX],
                    arg0[SQUARED_ERROR_IX] + arg1[SQUARED_ERROR_IX], arg0[SIGNED_DIFF_IX] + arg1[SIGNED_DIFF_IX]};
            }
        });
        final long nRows = aRowRDD.count();
        final double meanObserved = means[REFERENCE_IX] / nRows;

        final double absError = means[ABBS_ERROR_IX] / nRows;
        final double squaredError = means[SQUARED_ERROR_IX] / nRows;
        final double signedDiff = means[SIGNED_DIFF_IX] / nRows;

        final Double ssErrorNullModel = JavaDoubleRDD.fromRDD(filtered.map(new Function<Row, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Double call(final Row aRow) {
                double ref = RDDUtils.getDouble(aRow, classCol);
                return Math.pow(ref - meanObserved, 2.0);
            }
        }).rdd()).mean();

        LOGGER.info("R^2: "+ (1 - squaredError / ssErrorNullModel));
        LOGGER.info("mean absolute error: "+ absError);
        LOGGER.info("mean squared error: "+ squaredError);
        LOGGER.info("root mean squared deviation: "+ Math.sqrt(squaredError));
        LOGGER.info("mean signed difference: "+ signedDiff);

        return new NumericScorerData(aRowRDD.count(), (1 - squaredError / ssErrorNullModel), absError, squaredError, Math.sqrt(squaredError), signedDiff, classCol, predictionCol);
    }
}

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
package org.knime.bigdata.spark1_5.jobs.scorer;

import java.util.logging.Logger;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.scorer.ScorerJobInput;
import org.knime.bigdata.spark.node.scorer.numeric.NumericScorerJobOutput;
import org.knime.bigdata.spark1_5.api.RDDUtilsInJava;

/**
 * computes classification / regression scores
 *
 * @author dwk
 */
@SparkClass
public class NumericScorerJob extends AbstractScorerJob {

    private static final long serialVersionUID = 1L;
    protected static final Logger LOGGER = Logger.getLogger(NumericScorerJob.class.getName());

    private final static int REFERENCE_IX = 0;
    private final static int ABBS_ERROR_IX = 1;
    private final static int SQUARED_ERROR_IX =2 ;
    private final static int SIGNED_DIFF_IX = 3;

    //  - Numeric Scorer liefert R^2, mean absolute error, mean absolute error,
    //    root mean square error und mean signed difference. In KNIME ist das die
    //    Klasse org.knime.base.node.mine.scorer.numeric.NumericScorerNodeModel

    /**
     * {@inheritDoc}
     * @throws KNIMESparkException
     */
    @Override
    protected JobOutput doScoring(final ScorerJobInput input, final JavaRDD<Row> rowRDD) throws KNIMESparkException {
        final Integer classCol = input.getRefColIdx();
        final Integer predictionCol = input.getPredictionColIdx();

        final JavaRDD<Row> filtered = rowRDD.filter(new Function<Row, Boolean>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(final Row aRow) throws Exception {
                return !aRow.isNullAt(classCol);
            }
        });

        final long nRows = filtered.count();
        if (nRows == 0) {
            throw new KNIMESparkException("Unsupported empty input dataset detected.");
        }

        final JavaRDD<Double[]> stats = filtered.map(new Function<Row, Double[]>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Double[] call(final Row aRow) {

                final double ref = RDDUtilsInJava.getDouble(aRow, classCol);
                final double pred = RDDUtilsInJava.getDouble(aRow, predictionCol);
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
        final double meanObserved = means[REFERENCE_IX] / nRows;

        final double absError = means[ABBS_ERROR_IX] / nRows;
        final double squaredError = means[SQUARED_ERROR_IX] / nRows;
        final double signedDiff = means[SIGNED_DIFF_IX] / nRows;

        final Double ssErrorNullModel = JavaDoubleRDD.fromRDD(filtered.map(new Function<Row, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Double call(final Row aRow) {
                double ref = RDDUtilsInJava.getDouble(aRow, classCol);
                return Math.pow(ref - meanObserved, 2.0);
            }
        }).rdd()).mean();

        LOGGER.info("R^2: "+ (1 - squaredError / ssErrorNullModel));
        LOGGER.info("mean absolute error: "+ absError);
        LOGGER.info("mean squared error: "+ squaredError);
        LOGGER.info("root mean squared deviation: "+ Math.sqrt(squaredError));
        LOGGER.info("mean signed difference: "+ signedDiff);

        return new NumericScorerJobOutput((1 - squaredError / ssErrorNullModel),
            absError, squaredError, Math.sqrt(squaredError), signedDiff, 0 /* TODO missing values */);
    }
}

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
package org.knime.bigdata.spark2_3.jobs.scorer;

import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.mean;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.pow;
import static org.apache.spark.sql.functions.sum;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.scorer.ScorerJobInput;
import org.knime.bigdata.spark.node.scorer.numeric.NumericScorerJobOutput;

/**
 * Computes classification / regression scores
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class NumericScorerJob extends AbstractScorerJob {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(NumericScorerJob.class.getName());

    @Override
    protected JobOutput doScoring(final ScorerJobInput input, final Dataset<Row> dataset) throws KNIMESparkException {

        final String refCol = dataset.columns()[input.getActualColIdx()]; // ignore rows with missing values
        final String predictionCol = dataset.columns()[input.getPredictionColIdx()]; // fail on missing values

        final Dataset<Row> filtered = dataset
                .select(col(refCol).cast(DataTypes.DoubleType), col(predictionCol).cast(DataTypes.DoubleType))
                .where(col(refCol).isNotNull().and(not(col(refCol).isNaN()))).toDF(); // drop rows with missing reference column

        final long count = filtered.count();
        if (count == 0) {
            throw new KNIMESparkException("Unsupported empty input dataset detected.");
        }

        if (filtered.where(col(predictionCol).isNull().or(col(predictionCol).isNaN())).count() > 0) {
            throw new KNIMESparkException(
                String.format("Unsupported missing values in prediction column '%s' detected.", predictionCol));
        }

        final Row stats = filtered.select(
                col(refCol).as("ref"),
                abs(col(refCol).minus(col(predictionCol))).as("absError"),
                pow(col(refCol).minus(col(predictionCol)), 2.0).as("sqErr"),
                col(predictionCol).minus(col(refCol)).as("sigDiff")
            ).agg(sum("ref"), sum("absError"), sum("sqErr"), sum("sigDiff")).first();

        final double meanObserved = stats.getDouble(0) / count;
        final double absError = stats.getDouble(1) / count;
        final double squaredError = stats.getDouble(2) / count;
        final double signedDiff = stats.getDouble(3) / count;
        final double ssErrorNullModel = filtered.select(pow(col(refCol).minus(meanObserved), 2.0).as("ssErr")).agg(mean("ssErr")).first().getDouble(0);

        return new NumericScorerJobOutput(count, (1 - squaredError / ssErrorNullModel),
            absError, squaredError, Math.sqrt(squaredError), signedDiff, input.getActualColIdx(), input.getPredictionColIdx());
    }

    @Override
    protected String getScorerName() {
        return "numeric";
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }
}

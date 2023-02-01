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
 */
package org.knime.bigdata.spark3_3.jobs.preproc.joiner;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import org.knime.bigdata.spark.node.preproc.joiner.JoinMode;
import org.knime.bigdata.spark.node.preproc.joiner.SparkJoinerJobInput;
import org.knime.bigdata.spark3_3.api.NamedObjects;
import org.knime.bigdata.spark3_3.api.SimpleSparkJob;

/**
 * Joins two data frames and into a new one.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class JoinJob implements SimpleSparkJob<SparkJoinerJobInput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(JoinJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final SparkJoinerJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {

        final String resultKey = input.getFirstNamedOutputObject();
        final Dataset<Row> left = namedObjects.getDataFrame(input.getLeftInputObject()).as("left");
        final Dataset<Row> right = namedObjects.getDataFrame(input.getRightNamedObject()).alias("right");
        final Column joinExpression = joinExpr(input, left, right);
        final String mode = sparkJoinMode(input.getJoineMode());
        final Column columns[] = selectedColumns(input, left, right);

        LOGGER.info("Joining data frames via " + mode + "...");
        final Dataset<Row> result = left.join(right, joinExpression, mode).select(columns);

        LOGGER.info("Storing join result under key: " + input.getFirstNamedOutputObject());
        namedObjects.addDataFrame(resultKey, result);
    }

    /** @return join expression with left and right columns */
    private Column joinExpr(final SparkJoinerJobInput input, final Dataset<Row> left, final Dataset<Row> right) {
        final String leftCols[] = left.columns();
        final Integer leftColIdx[] = input.getJoinColIdxsLeft();
        final String rightCols[] = right.columns();
        final Integer rightColIdx[] = input.getJoinColIdxsRight();

        Column expr = left.col(leftCols[leftColIdx[0]]).equalTo(right.col(rightCols[rightColIdx[0]]));

        for (int i = 1; i < leftColIdx.length; i++) {
            expr = expr.and(col("left." + leftCols[leftColIdx[i]]).equalTo(col("right." + rightCols[rightColIdx[i]])));
        }

        return expr;
    }

    /** @return Spark join mode */
    private String sparkJoinMode(final JoinMode mode) throws KNIMESparkException {
        switch (mode) {
            case InnerJoin: return "inner";
            case LeftOuterJoin: return "left_outer";
            case RightOuterJoin: return "right_outer";
            case FullOuterJoin: return "full_outer";
            default: throw new KNIMESparkException("Unsupported join mode: " + mode);
        }
    }

    /** @return selected columns of joined data frame */
    private Column[] selectedColumns(final SparkJoinerJobInput input, final Dataset<Row> left, final Dataset<Row> right) {
        final String leftCols[] = left.columns();
        final Integer leftColIdx[] = input.getSelectColIdxsLeft();
        final String rightCols[] = right.columns();
        final Integer rightColIdx[] = input.getSelectColIdxsRight();
        final IntermediateField resultFields[] = input.getSpec(input.getFirstNamedOutputObject()).getFields();
        final ArrayList<Column> columns = new ArrayList<>(leftColIdx.length + rightColIdx.length);

        for (int index : leftColIdx) {
            final String outputName = resultFields[columns.size()].getName();
            columns.add(col("left." + leftCols[index]).as(outputName));
        }

        for (int index : rightColIdx) {
            final String outputName = resultFields[columns.size()].getName();
            columns.add(col("right." + rightCols[index]).as(outputName));
        }

        return columns.toArray(new Column[0]);
    }
}

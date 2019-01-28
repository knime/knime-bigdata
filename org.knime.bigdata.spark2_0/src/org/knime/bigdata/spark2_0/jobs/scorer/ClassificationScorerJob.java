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
 *   Created on May 12, 2016 by oole
 */
package org.knime.bigdata.spark2_0.jobs.scorer;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.not;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.scorer.accuracy.ScorerJobInput;
import org.knime.bigdata.spark.node.scorer.accuracy.ScorerJobOutput;
import org.knime.bigdata.spark2_0.api.RDDUtilsInJava;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 *  Scoring for classification
 *
 * @author Ole Ostergaard
 */
@SparkClass
public class ClassificationScorerJob extends AbstractScorerJob {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(ClassificationScorerJob.class.getName());

    @Override
    protected JobOutput doScoring(final ScorerJobInput input, final Dataset<Row> dataset) {
        final Integer classCol = input.getActualColIdx();
        final Integer predictionCol = input.getPredictionColIdx();

        final String classColName = dataset.columns()[classCol];
        final String predictionColName = dataset.columns()[predictionCol];

        // need to determine reference labels before filtering rows with missing values
        final List<Object> labels = JavaConversions.seqAsJavaList(dataset.where(col(classColName).isNotNull())
                .select(collect_set(col(classColName)))
                .first()
                .<Seq<Object>>getAs(0));

        final long rowsWithMissingValues =
            dataset.where(col(classColName).isNull().or(col(predictionColName).isNull())).count();

        final Dataset<Row> filtered =
            dataset.where(not(col(classColName).isNull().or(col(predictionColName).isNull())));

        final JavaRDD<Row> rowRDD = filtered.javaRDD();
        Map<Tuple2<Object, Object>, Integer> counts = RDDUtilsInJava.aggregatePairs(rowRDD, classCol, predictionCol);

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

        return new ScorerJobOutput(confusionMatrix, rowRDD.count(), falseCount, correctCount, classCol, predictionCol,
            labels, rowsWithMissingValues);
    }

    @Override
    protected String getScorerName() {
        return "classification";
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }
}
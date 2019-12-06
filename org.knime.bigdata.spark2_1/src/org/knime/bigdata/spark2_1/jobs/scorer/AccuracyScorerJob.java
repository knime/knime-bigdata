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
package org.knime.bigdata.spark2_1.jobs.scorer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.scorer.ScorerJobInput;
import org.knime.bigdata.spark.node.scorer.accuracy.AccuracyScorerJobOutput;

import scala.Tuple2;

/**
 *  Scoring for classification
 *
 * @author Ole Ostergaard
 */
@SparkClass
public class AccuracyScorerJob extends AbstractScorerJob {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(AccuracyScorerJob.class.getName());

    @Override
    protected JobOutput doScoring(final ScorerJobInput input, final Dataset<Row> dataset) {
        final String classCol = dataset.columns()[input.getRefColIdx()];
        final String predictionCol = dataset.columns()[input.getPredictionColIdx()];

        Map<Tuple2<Object, Object>, Long> counts = aggregatePairs(dataset, classCol, predictionCol);
        List<Object> labels = getDistinctValuesOfColumn(dataset, classCol);

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

        return new AccuracyScorerJobOutput(confusionMatrix, dataset.count(), falseCount, correctCount,
            labels, 0 /* TODO missing values */);
    }

    /**
     * Count the number of times each pair of values of the given two indices occurs in the RDD.
     *
     * WARNING: this might produce a very large Map!
     *
     * @param input Dataframe to analyze
     * @param leftCol first column name
     * @param rightCol second column name
     * @return map with counts for all pairs of values that occur at least once
     */
    private static Map<Tuple2<Object, Object>, Long> aggregatePairs(final Dataset<Row> input, final String leftCol,
        final String rightCol) {

        return input.groupBy(leftCol, rightCol)
            .count()
            .javaRDD()
            .mapToPair(new PairFunction<Row, Tuple2<Object, Object>, Long>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<Tuple2<Object, Object>, Long> call(final Row row) throws Exception {
                    return Tuple2.apply(Tuple2.apply(row.get(0), row.get(1)), row.getLong(2));
                }
        }).collectAsMap();
    }


    /**
     * find the distinct values of a column
     * @param aRDD
     * @param aColumn
     * @return the distinct values for the given column index
     */
    private static List<Object> getDistinctValuesOfColumn(final Dataset<Row> input, final String column) {
        return input.select(column).distinct().javaRDD().map(new Function<Row, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object call(final Row aRow) throws Exception {
                return aRow.get(0);
            }
        }).collect();
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
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverter;
import org.knime.bigdata.spark.node.scorer.ScorerJobInput;
import org.knime.bigdata.spark.node.scorer.accuracy.AccuracyScorerJobOutput;
import org.knime.bigdata.spark2_0.api.RDDUtilsInJava;
import org.knime.bigdata.spark2_0.api.SupervisedLearnerUtils;
import org.knime.bigdata.spark2_0.api.TypeConverters;

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
        final Integer classCol = input.getRefColIdx();
        final Integer predictionCol = input.getPredictionColIdx();
        final JavaRDD<Row> rowRDD = dataset.javaRDD();

        Map<Tuple2<Object, Object>, Integer> counts = RDDUtilsInJava.aggregatePairs(rowRDD, classCol, predictionCol);

        List<Object> labels = SupervisedLearnerUtils.getDistinctValuesOfColumn(rowRDD, classCol).collect();
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

        final List<Object> convertedLabels = convertLabels(labels, dataset.schema().fields()[classCol]);
        return new AccuracyScorerJobOutput(confusionMatrix, rowRDD.count(), falseCount, correctCount,
            convertedLabels, 0 /* TODO missing values */);
    }

    private static List<Object> convertLabels(final List<Object> sparkObjects, final StructField struct) {
        final IntermediateToSparkConverter<? extends DataType> converter = TypeConverters.getConverter(struct.dataType());
        final ArrayList<Object> convertedObjects = new ArrayList<>(sparkObjects.size());
        for (final Object sparkObject : sparkObjects) {
            convertedObjects.add(converter.convert(sparkObject));
        }
        return convertedObjects;
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
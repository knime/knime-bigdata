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
package com.knime.bigdata.spark2_2.jobs.preproc.partition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.preproc.sampling.SamplingJobInput;
import com.knime.bigdata.spark.node.preproc.sampling.SamplingJobOutput;
import com.knime.bigdata.spark2_2.api.NamedObjects;
import com.knime.bigdata.spark2_2.jobs.preproc.sampling.AbstractSamplingJob;

import scala.Tuple2;

/**
 * Split input RDD into two RDDs using sampling.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class PartitionJob extends AbstractSamplingJob {
    private static final long serialVersionUID = 1L;

    /**
     * {@inheritDoc}
     *
     * This operation process all input rows twice (one run per output RDD).
     */
    @Override
    protected SamplingJobOutput randomSampling(final SparkContext context, final SamplingJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException {

        final SparkSession spark = SparkSession.builder().sparkContext(context).getOrCreate();
        final Dataset<Row> inputDataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final JavaRDD<Row> inputRdd = inputDataset.javaRDD();
        double fraction = getFractionToSample(input, inputDataset);

        if (fraction >= 1.0) {
            return noSplitRequired(context, input, namedObjects);

        } else {
            double weights[] = new double[] { fraction, 1 - fraction };
            final JavaRDD<Row> resultRdd[] = inputRdd.randomSplit(weights, input.getSeed());
            namedObjects.addDataFrame(input.getNamedOutputObjects().get(0),
                spark.createDataFrame(resultRdd[0], inputDataset.schema()));
            namedObjects.addDataFrame(input.getNamedOutputObjects().get(1),
                spark.createDataFrame(resultRdd[1], inputDataset.schema()));
            return new SamplingJobOutput(false);
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * To partition our data, we have to user a unique (row) ID here and subtract the samples from the input RDD.
     * <p/>
     * <b>WARNING:</b> We have to collect all labels, this might result in heavy memory consumption using many labels.
     * <p/>
     * <b>WARNING:</b> We might process all data several times (1 with absolute sampling size, 1 to collect all labels,
     * 1 to produce unique IDs, 2 to sample exact (or 1 otherwise) and to subtract all samples from input.
     */
    @Override
    protected SamplingJobOutput stratifiedSampling(final SparkContext context, final SamplingJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException {

        final SparkSession spark = SparkSession.builder().sparkContext(context).getOrCreate();
        final Dataset<Row> inputDataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final JavaRDD<Row> inputRdd = inputDataset.javaRDD();
        final int keyColIndex = input.getClassColIx();
        final double fraction = getFractionToSample(input, inputDataset);

        if (fraction >= 1.0) {
            return noSplitRequired(context, input, namedObjects);
        }

        final JavaPairRDD<Long, Row> inputWithUniqID = addUniqueId(inputRdd);
        final JavaPairRDD<String, Tuple2<Long, Row>> labledRdd = inputWithUniqID.keyBy(new Function<Tuple2<Long, Row>, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(final Tuple2<Long, Row> tuple) throws Exception {
                Object val = tuple._2.get(keyColIndex);
                return val == null ? null : val.toString();
            }
        });
        final List<String> labels = labledRdd.keys().distinct().collect();

        final boolean withReplacement = input.withReplacement();
        final boolean exact = input.getExact();
        final long seed = input.getSeed();

        final Map<String, Double> fractions = new HashMap<>();
        for (String label : labels) {
            fractions.put(label, fraction);
        }

        final JavaPairRDD<String, Tuple2<Long, Row>> samplesWithLabel;
        if (exact) {
            LOGGER.info("Using exact stratified sampling with fraction " + fraction);
            samplesWithLabel = labledRdd.sampleByKeyExact(withReplacement, fractions, seed);
        } else {
            LOGGER.info("Using approximate stratified sampling with fraction " + fraction);
            samplesWithLabel = labledRdd.sampleByKey(withReplacement, fractions, seed);
        }

        final JavaPairRDD<Long, Row> samplesRdd = JavaPairRDD.fromJavaRDD(samplesWithLabel.values());
        final JavaPairRDD<Long, Row> othersRdd = inputWithUniqID.subtractByKey(samplesRdd);

        final Dataset<Row> samples = spark.createDataFrame(samplesRdd.values(), inputDataset.schema());
        final Dataset<Row> others = spark.createDataFrame(othersRdd.values(), inputDataset.schema());

        namedObjects.addDataFrame(input.getNamedOutputObjects().get(0), samples);
        namedObjects.addDataFrame(input.getNamedOutputObjects().get(1), others);

        return new SamplingJobOutput(false);
    }

    /** @return Pair with unique ID as key and given row as values */
    private JavaPairRDD<Long, Row> addUniqueId(final JavaRDD<Row> rows) {
        return JavaPairRDD.fromJavaRDD(rows.zipWithUniqueId().map(new Function<Tuple2<Row,Long>, Tuple2<Long, Row>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Long, Row> call(final Tuple2<Row, Long> t) throws Exception {
                return new Tuple2<>(t._2, t._1); // flip key and value
            }
        }));
    }
}

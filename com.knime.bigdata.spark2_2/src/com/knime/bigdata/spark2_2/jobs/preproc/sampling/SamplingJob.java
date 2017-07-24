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
package com.knime.bigdata.spark2_2.jobs.preproc.sampling;

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
import com.knime.bigdata.spark2_2.jobs.preproc.partition.PartitionJob;

/**
 * Samples values from input RDD.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class SamplingJob extends AbstractSamplingJob {
    private static final long serialVersionUID = 1L;

    @Override
    protected SamplingJobOutput randomSampling(final SparkContext context, final SamplingJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException {

        final Dataset<Row> inputDataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        double fraction = getFractionToSample(input, inputDataset);

        if (fraction >= 1.0) {
            return noSplitRequired(context, input, namedObjects);

        } else {
            final Dataset<Row> samples = inputDataset.sample(input.withReplacement(), fraction, input.getSeed());
            namedObjects.addDataFrame(input.getNamedOutputObjects().get(0), samples);
            return new SamplingJobOutput(false);
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Works like {@link PartitionJob}, but does not require an additional unique ID to split the input RDD.
     * <p/>
     * <b>WARNING:</b> We have to collect all labels, this might result in heavy memory consumption using many labels.
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

        final JavaPairRDD<String, Row> labledRdd = inputRdd.keyBy(new Function<Row, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(final Row row) throws Exception {
                Object val = row.get(keyColIndex);
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

        final JavaPairRDD<String, Row> samplesWithLabel;
        if (exact) {
            LOGGER.info("Using exact stratified sampling with fraction " + fraction);
            samplesWithLabel = labledRdd.sampleByKeyExact(withReplacement, fractions, seed);
        } else {
            LOGGER.info("Using approximate stratified sampling with fraction " + fraction);
            samplesWithLabel = labledRdd.sampleByKey(withReplacement, fractions, seed);
        }

        final Dataset<Row> samplesDataset = spark.createDataFrame(samplesWithLabel.values(), inputDataset.schema());
        namedObjects.addDataFrame(input.getNamedOutputObjects().get(0), samplesDataset);
        return new SamplingJobOutput(false);
    }
}

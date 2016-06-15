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
package com.knime.bigdata.spark1_6.jobs.preproc.sampling;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.EnumContainer.SamplingMethod;
import com.knime.bigdata.spark.node.preproc.sampling.SamplingJobInput;
import com.knime.bigdata.spark.node.preproc.sampling.SamplingJobOutput;
import com.knime.bigdata.spark1_6.api.NamedObjects;
import com.knime.bigdata.spark1_6.api.SparkJob;

import scala.Tuple2;

/**
 * samples / splits rdds
 *
 * @author dwk
 */
@SparkClass
public class SamplingJob implements SparkJob<SamplingJobInput, SamplingJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(SamplingJob.class.getName());

    /**
     * run the actual job,result(s) are stored in named RDDs
     *
     * @throws KNIMESparkException if something goes wrong
     */
    @Override
    public SamplingJobOutput runJob(final SparkContext sparkContext, final SamplingJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {
        LOGGER.info("starting sampling / splitting job...");
        final List<String> rddOutNames = input.getNamedOutputObjects();
        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());

        final JavaRDD<Row> samples = sampleRdd(sparkContext, input, rowRDD);

        if (input.isSplit()) {
            final JavaRDD<Row> remainder = rowRDD.subtract(samples);
            LOGGER.info("Storing sample / split result data under key(s): "
                    + rddOutNames.get(0) + ", "
                    + rddOutNames.get(1));
            namedObjects.addJavaRdd(rddOutNames.get(1), remainder);
        } else {
            LOGGER.info("Storing sample result data under key: " + rddOutNames.get(0));
        }
        final boolean successfull = !samples.equals(rowRDD);
        final SamplingJobOutput jobOutput = new SamplingJobOutput(successfull);
        if (successfull) {
            //only if the sampling succeeded add the result rdd to the named objects map
            namedObjects.addJavaRdd(rddOutNames.get(0), samples);
        } else {
            LOGGER.info("Return input RDD as sample RDD.");
        }
        return jobOutput;
    }

    /**
     * @param aContext
     * @param aConfig
     * @param rdd2Sample
     * @return
     * @throws KNIMESparkException
     */
    private JavaRDD<Row>
        sampleRdd(final SparkContext aContext, final SamplingJobInput input, final JavaRDD<Row> rdd2Sample)
            throws KNIMESparkException {
        final SamplingMethod samplingMethod = input.getSamplingMethod();
        final JavaRDD<Row> samples;
        switch (samplingMethod) {
            case First: {
                samples = sampleFromTop(aContext, input, rdd2Sample);
                break;
            }
            case Linear: {
                throw new KNIMESparkException("ERROR: linear sampling not yet implemented");
            }
            case Stratified: {
                samples = stratifiedSample(aContext, input, rdd2Sample);
                break;
            }
            case Random: {
                final double fraction = getFractionToSample(input, rdd2Sample);
                LOGGER.info("Using random sampling, fraction: " + fraction);

                samples = rdd2Sample.sample(input.withReplacement(), fraction, input.getSeed());
                LOGGER.info("Sampled " + samples.count() + " of " + rdd2Sample.count() + " rows.");
                break;
            }
            default: {
                throw new KNIMESparkException("ERROR: unsupported sampling method: " + samplingMethod);
            }
        }
        return samples;
    }

    /**
     * @param aContext
     * @param aConfig
     * @param aRDD2Sample
     * @return
     */
    private JavaRDD<Row> stratifiedSample(final SparkContext aContext, final SamplingJobInput input,
        final JavaRDD<Row> aRDD2Sample) {
        final int classColIx = input.getClassColIx();
        final boolean exact = input.getExact();

        JavaPairRDD<String, Row> keyedRdd = aRDD2Sample.keyBy(new Function<Row, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(final Row aRow) throws Exception {
                Object val = aRow.get(classColIx);
                if (val == null) {
                    return null;
                } else {
                    return val.toString();
                }
            }
        });
        List<String> keys = keyedRdd.map(new Function<Tuple2<String, Row>, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(final Tuple2<String, Row> arg0) throws Exception {
                return arg0._1;
            }
        }).distinct().collect();

        boolean withReplacement = input.withReplacement();
        final long seed = input.getSeed();
        final double fraction = getFractionToSample(input, aRDD2Sample);

        Map<String, Object> fractions = new HashMap<>();
        for (String key : keys) {
            fractions.put(key, fraction);
        }

        final JavaPairRDD<String, Row> sampledRdd;
        if (exact) {
            LOGGER.info("Using exact stratified sampling, fraction: " + fraction);
            sampledRdd = keyedRdd.sampleByKeyExact(withReplacement, fractions, seed);
        } else {
            LOGGER.info("Using approximate stratified sampling, fraction: " + fraction);
            sampledRdd = keyedRdd.sampleByKey(withReplacement, fractions, seed);
        }
        return sampledRdd.map(new Function<Tuple2<String, Row>, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Tuple2<String, Row> aRow) throws Exception {
                return aRow._2;
            }
        });

    }

    /**
     * @param sparkContext
     * @param aConfig
     * @param rdd2Sample
     * @return
     */
    private JavaRDD<Row> sampleFromTop(final SparkContext sparkContext, final SamplingJobInput input,
        final JavaRDD<Row> rdd2Sample) {
        final JavaRDD<Row> samples;
        final long totalCount = rdd2Sample.count();
        final long c = getNumRowsToSample(input, totalCount);
        if (c >= totalCount) {
            samples = rdd2Sample;
        } else {
            @SuppressWarnings("resource")
            JavaSparkContext js = new JavaSparkContext(sparkContext);
            samples = js.parallelize(rdd2Sample.take((int)c));
        }
        return samples;
    }

    static long getNumRowsToSample(final SamplingJobInput input, final long aTotalCount) {
        switch (input.getCountMethod()) {
            case Absolute:
                return input.getCount();
            case Relative: {
                double f = input.getFraction();
                return (long)Math.ceil((aTotalCount) * f);
            }
            default:
                return 0;
        }
    }

    static double getFractionToSample(final SamplingJobInput input, final JavaRDD<Row> aRDDToSample) {
        switch (input.getCountMethod()) {
            case Absolute: {
                final double f = ((double)input.getCount()) / aRDDToSample.count();
                if (f > 1) {
                    return 1d;
                }
                return f;
            }
            case Relative: {
                return input.getFraction();
            }
            default:
                return 0d;
        }
    }
}

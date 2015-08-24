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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;

import scala.Tuple2;
import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.EnumContainer.CountMethods;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.SamplingMethods;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * samples / splits rdds
 *
 * @author dwk
 */
public class SamplingJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * count parameter (relative or absolute)
     */
    public static final String PARAM_COUNT_METHOD = "countMethod";

    /**
     * sampling with or without replacement?
     */
    public static final String PARAM_WITH_REPLACEMENT = "withReplacement";

    /**
     * sampling parameter (first, random, linear, or stratified)
     */
    public static final String PARAM_SAMPLING_METHOD = "samplingMethod";

    /**
     * sample fraction
     */
    public static final String PARAM_FRACTION = "fraction";

    /**
     * random seed parameter
     */
    public static final String PARAM_SEED = "seed";

    /**
     * absolute number of rows to sample (only relevant for sampling method 'absolute')
     */
    public static final String PARAM_COUNT = "count";

    /**
     * exact (stratified) sampling
     */
    public static final String PARAM_EXACT = "exact";

    /**
     * class column index for stratified sampling
     */
    public static final String PARAM_CLASS_COLUMN = "classColumn";

    /**
     * in case of split - name of second table
     */
    public static final String PARAM_SPLIT_TABLE_2 = "SplitTable2";

    private final static Logger LOGGER = Logger.getLogger(SamplingJob.class.getName());

    private static final long DEFAULT_RANDOM_SEED = 99990;

    private static final double DEFAULT_FRACTION = 0.5d;

    private static final int DEFAULT_COUNT = 11111;

    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        if (!aConfig.hasInputParameter(KnimeSparkJob.PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + KnimeSparkJob.PARAM_INPUT_TABLE + "' missing.";
        }

        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_SAMPLING_METHOD)) {
                msg = "Input parameter '" + PARAM_SAMPLING_METHOD + "' missing.";
            }
        }

        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_COUNT_METHOD)) {
                msg = "Input parameter '" + PARAM_COUNT_METHOD + "' missing.";
            }
        }

        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_WITH_REPLACEMENT)) {
                msg = "Input parameter '" + PARAM_WITH_REPLACEMENT + "' missing.";
            }
        }

        if (msg == null
            && (!aConfig.hasOutputParameter(PARAM_RESULT_TABLE) || aConfig.getOutputStringParameter(PARAM_RESULT_TABLE) == null)) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    void validateInput(final JobConfig aConfig) throws GenericKnimeSparkException {
        String msg = null;
        final String key = aConfig.getInputParameter(KnimeSparkJob.PARAM_INPUT_TABLE);
        if (key == null) {
            msg = "Input parameter at port 1 is missing!";
        } else if (!validateNamedRdd(key)) {
            msg = "Input data table missing!";
        }
        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    /**
     * run the actual job,result(s) are stored in named RDDs, "OK" message is returned if all went well
     *
     * @throws GenericKnimeSparkException if something goes wrong
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.INFO, "starting sampling / splitting job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));

        final JavaRDD<Row> samples = sampleRdd(sc, aConfig, rowRDD);

        if (isSplit(aConfig)) {
            final JavaRDD<Row> remainder = rowRDD.subtract(samples);
            LOGGER.log(
                Level.INFO,
                "Storing sample / split result data under key(s): "
                    + aConfig.getOutputStringParameter(PARAM_RESULT_TABLE) + ", "
                    + aConfig.getOutputStringParameter(PARAM_SPLIT_TABLE_2));
            addToNamedRdds(aConfig.getOutputStringParameter(PARAM_SPLIT_TABLE_2), remainder);
        } else {
            LOGGER.log(Level.INFO,
                "Storing sample result data under key: " + aConfig.getOutputStringParameter(PARAM_RESULT_TABLE));
        }
        addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), samples);
        final boolean successfull = !samples.equals(rowRDD);
        if (!successfull) {
            LOGGER.log(Level.WARNING,
                "Return input RDD as sample RDD.");
        }
        return JobResult.emptyJobResult().withObjectResult(Boolean.valueOf(successfull));
    }

    /**
     * @param aContext
     * @param aConfig
     * @param aRdd2Sample
     * @return
     * @throws GenericKnimeSparkException
     */
    private JavaRDD<Row>
        sampleRdd(final SparkContext aContext, final JobConfig aConfig, final JavaRDD<Row> aRdd2Sample)
            throws GenericKnimeSparkException {
        final SamplingMethods samplingMethod = getSamplingMethod(aConfig);
        final JavaRDD<Row> samples;
        switch (samplingMethod) {
            case First: {
                samples = sampleFromTop(aContext, aConfig, aRdd2Sample);
                break;
            }
            case Linear: {
                throw new GenericKnimeSparkException("ERROR: linear sampling not yet implemented");
            }
            case Stratified: {
                samples = stratifiedSample(aContext, aConfig, aRdd2Sample);
                break;
            }
            case Random: {
                final double fraction = getFractionToSample(aConfig, aRdd2Sample);
                LOGGER.log(Level.INFO, "Using random sampling, fraction: " + fraction);

                samples = aRdd2Sample.sample(getWithReplacement(aConfig), fraction, getSeed(aConfig));
                LOGGER.log(Level.INFO, "Sampled " + samples.count() + " of " + aRdd2Sample.count() + " rows.");
                break;
            }
            default: {
                throw new GenericKnimeSparkException("ERROR: unsupported sampling method: " + samplingMethod);
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
    private JavaRDD<Row> stratifiedSample(final SparkContext aContext, final JobConfig aConfig,
        final JavaRDD<Row> aRDD2Sample) {
        final int classColIx = aConfig.getInputParameter(PARAM_CLASS_COLUMN, Integer.class);
        final boolean exact = aConfig.getInputParameter(PARAM_EXACT, Boolean.class);

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

        boolean withReplacement = getWithReplacement(aConfig);
        long seed = getSeed(aConfig);
        final double fraction = getFractionToSample(aConfig, aRDD2Sample);

        Map<String, Object> fractions = new HashMap<>();
        for (String key : keys) {
            fractions.put(key, fraction);
        }

        final JavaPairRDD<String, Row> sampledRdd;
        if (exact) {
            LOGGER.log(Level.INFO, "Using exact stratified sampling, fraction: " + fraction);
            sampledRdd = keyedRdd.sampleByKeyExact(withReplacement, fractions, seed);
        } else {
            LOGGER.log(Level.INFO, "Using approximate stratified sampling, fraction: " + fraction);
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
     * @param aContext
     * @param aConfig
     * @param aRDD2Sample
     * @return
     */
    private JavaRDD<Row> sampleFromTop(final SparkContext aContext, final JobConfig aConfig,
        final JavaRDD<Row> aRDD2Sample) {
        final JavaRDD<Row> samples;
        final long totalCount = aRDD2Sample.count();
        final long c = getNumRowsToSample(aConfig, totalCount);
        if (c >= totalCount) {
            samples = aRDD2Sample;
        } else {
            @SuppressWarnings("resource")
            JavaSparkContext js = new JavaSparkContext(aContext);
            samples = js.parallelize(aRDD2Sample.take((int)c));
        }
        return samples;
    }

    static long getNumRowsToSample(final JobConfig aConfig, final long aTotalCount) {
        switch (getCountMethod(aConfig)) {
            case Absolute:
                return getCount(aConfig);
            case Relative: {
                double f = getFraction(aConfig);
                return (long)Math.ceil((aTotalCount) * f);
            }
            default:
                return 0;
        }
    }

    static double getFractionToSample(final JobConfig aConfig, final JavaRDD<Row> aRDDToSample) {
        switch (getCountMethod(aConfig)) {
            case Absolute: {
                final double f = ((double)getCount(aConfig)) / aRDDToSample.count();
                if (f > 1) {
                    return 1d;
                }
                return f;
            }
            case Relative: {
                return getFraction(aConfig);
            }
            default:
                return 0d;
        }
    }

    /**
     * @param aConfig
     * @return
     */
    static boolean isSplit(final JobConfig aConfig) {
        return aConfig.hasOutputParameter(PARAM_SPLIT_TABLE_2);
    }

    /**
     * @param aConfig
     * @return
     */
    static double getFraction(final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_FRACTION)) {
            return aConfig.getInputParameter(PARAM_FRACTION, Double.class);
        }
        return DEFAULT_FRACTION;
    }

    /**
     * @param aConfig
     * @return
     */
    static long getSeed(final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_SEED)) {
            return aConfig.getInputParameter(PARAM_SEED, Long.class);
        }
        return DEFAULT_RANDOM_SEED;
    }

    /**
     * @param aConfig
     * @return
     */
    static int getCount(final JobConfig aConfig) {
        if (aConfig.hasInputParameter(PARAM_COUNT)) {
            return aConfig.getInputParameter(PARAM_COUNT, Integer.class);
        }
        return DEFAULT_COUNT;
    }

    /**
     * @param aConfig
     * @return
     */
    static boolean getWithReplacement(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_WITH_REPLACEMENT, Boolean.class);
    }

    static CountMethods getCountMethod(final JobConfig aConfig) {
        final String countMethod = aConfig.getInputParameter(PARAM_COUNT_METHOD);
        return CountMethods.fromKnimeEnum(countMethod);
    }

    static SamplingMethods getSamplingMethod(final JobConfig aConfig) {
        final String countMethod = aConfig.getInputParameter(PARAM_SAMPLING_METHOD);
        return SamplingMethods.fromKnimeEnum(countMethod);
    }
}

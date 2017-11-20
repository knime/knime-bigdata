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
 *   Created on May 9, 2016 by oole
 */
package com.knime.bigdata.spark.node.preproc.sampling;

import java.util.Arrays;

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.EnumContainer.CountMethod;
import com.knime.bigdata.spark.core.job.util.EnumContainer.SamplingMethod;

/**
 *
 * @author Ole Ostergaard
 */
@SparkClass
public class SamplingJobInput extends JobInput {

    private static final Long DEFAULT_RANDOM_SEED = Long.valueOf(99990);

    private static final double DEFAULT_FRACTION = 0.5d;

    private static final int DEFAULT_COUNT = 11111;


    private static final String COUNT_METHOD = "countMethod";
    private static final String WITH_REPLACEMENT = "withReplacement";
    private static final String SAMPLING_METHOD = "samplingMethod";
    private static final String FRACTION = "fraction";
    private static final String COUNT = "count";
    private static final String EXACT = "exact";
    private static final String CLASS_COLUMN = "classColumn";
    private static final String SPLIT_TABLE = "splitTable";
    private static final String SEED = "seed";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public SamplingJobInput() {}

    public SamplingJobInput(final String inputNamedObject, final String[] outputNamedObjects, final CountMethod countMethod,
        final Integer count, final SamplingMethod samplingMethod, final Double fraction, final Integer classColIx,
        final Boolean isWithReplacement, final Long seed, final Boolean exact) {
        addNamedInputObject(inputNamedObject);
        addNamedOutputObjects(Arrays.asList(outputNamedObjects));
        set(COUNT_METHOD, countMethod.name());
        set(WITH_REPLACEMENT, isWithReplacement);
        set(SAMPLING_METHOD, samplingMethod.name());
        set(FRACTION, fraction != null ? fraction : DEFAULT_FRACTION);
        set(COUNT, count != null ? count : DEFAULT_COUNT);
        set(EXACT, exact);
        set(CLASS_COLUMN, classColIx);
        set(SPLIT_TABLE, outputNamedObjects.length == 2);
        set(SEED, seed != null ? seed : DEFAULT_RANDOM_SEED);
    }


    /**
     * @return the selected count method
     */
    public CountMethod getCountMethod() {
        final String methodName = get(COUNT_METHOD);
        return CountMethod.valueOf(methodName);
    }

    /**
     * @return the selected count
     */
    public int getCount() {
        return getInteger(COUNT);
    }

    /**
     * @return the selected sampling method
     */
    public SamplingMethod getSamplingMethod() {
        final String methodName = get(SAMPLING_METHOD);
        return SamplingMethod.valueOf(methodName);
    }

    /**
     * @return the selected fraction
     */
    public double getFraction() {
        return getDouble(FRACTION);
    }

    /**
     * @return the selected class column
     */
    public Integer getClassColIx() {
        return getInteger(CLASS_COLUMN);
    }

    /**
     * @return whether or not the sampling happens with replacement
     */
    public Boolean withReplacement() {
        return get(WITH_REPLACEMENT);
    }

    /**
     * @return the selected seed
     */
    public Long getSeed() {
        return getLong(SEED);
    }

    /**
     * @return whether or not the sampling is exact
     */
    public Boolean getExact() {
        return get(EXACT);
    }

    /**
     * @return whether or not the table is split
     */
    public Boolean isSplit() {
        return get(SPLIT_TABLE);
    }
}

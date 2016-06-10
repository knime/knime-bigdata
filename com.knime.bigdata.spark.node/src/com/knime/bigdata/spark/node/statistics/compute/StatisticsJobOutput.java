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
 *   Created on 11.09.2015 by dwk
 */
package com.knime.bigdata.spark.node.statistics.compute;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class StatisticsJobOutput extends JobOutput {

    private static final String COUNT = "COUNT";
    private static final String MAX = "MAX";
    private static final String MEAN = "MEAN";
    private static final String MIN = "MIN";
    private static final String L1 = "L1";
    private static final String L2 = "L2";
    private static final String NONZEROS = "NONZEROS";
    private static final String VARIANCE = "VARIANCE";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public StatisticsJobOutput() {}

    /**
     * @param count
     * @param min
     * @param max
     * @param mean
     * @param variance
     * @param normL1
     * @param normL2
     * @param numNonzeros
     */
    public StatisticsJobOutput(final long count, final double[] min, final double[] max, final double[] mean,
        final double[] variance, final double[] normL1, final double[] normL2, final double[] numNonzeros) {
        set(COUNT, count);
        set(MIN, min);
        set(MAX, max);
        set(MEAN, mean);
        set(VARIANCE, variance);
        set(L1, normL1);
        set(L2, normL2);
        set(NONZEROS, numNonzeros);
    }

    /**
     * @return count
     */
    public long count() {
        return getLong(COUNT);
    }

    /**
     * @return minimum
     */
    public double[] min() {
        return get(MIN);
    }

    /**
     * @return maximum
     */
    public double[] max() {
        return get(MAX);
    }

    /**
     * @return mean
     */
    public double[] mean() {
        return get(MEAN);
    }

    /**
     * @return L1 norm
     */
    public double[] normL1() {
        return get(L1);
    }

    /**
     * @return L2 norm
     */
    public double[] normL2() {
        return get(L2);
    }

    /**
     * @return none zeros
     */
    public double[] numNonzeros() {
        return get(NONZEROS);
    }

    /**
     * @return the variance
     */
    public double[] variance() {
        return get(VARIANCE);
    }

}

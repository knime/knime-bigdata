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
 *   Created on May 6, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.util.repartition;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Repartition spark job input.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class RepartitionJobInput extends JobInput {

    /**
     * Mode used to calculate new partition count
     */
    public enum CalculationMode {
        /** Fixed partition count */
        FIXED_VALUE,
        /** Multiply current partition count with factor */
        MULTIPLY_PART_COUNT,
        /** Divide current partition count by factor */
        DIVIDE_PART_COUNT,
        /** Multiply count of all executor cores by factor */
        MULTIPLY_EXECUTOR_CORES
    }

    private static final String CALC_MODE = "calculationMode";
    private static final String VALUE = "value";
    private static final String FACTOR = "factor";
    private static final String USE_COALESCE = "useCoalesce";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public RepartitionJobInput() {}

    /**
     * Private constructor. Use {@link #fixedValue(String, String, boolean, int)},
     * {@link #multiplyPartCount(String, String, boolean, double)},
     * {@link #dividePartCount(String, String, boolean, double)} or
     * {@link #multiplyExecutorCoresCount(String, String, boolean, double)} instead.
     *
     * @param namedInputObject name of input object
     * @param namedOutputObject name of output object
     * @param useCoalesce use coalesce to reduce partition count
     */
    private RepartitionJobInput(final String namedInputObject, final String namedOutputObject, final boolean useCoalesce) {
        addNamedInputObject(namedInputObject);
        addNamedOutputObject(namedOutputObject);
        set(USE_COALESCE, useCoalesce);
    }

    /**
     * Use a fixed new partition count.
     */
    static RepartitionJobInput fixedValue(final String namedInputObject, final String namedOutputObject,
            final boolean useCoalesce, final int value) {

        final RepartitionJobInput jobInput = new RepartitionJobInput(namedInputObject, namedOutputObject, useCoalesce);
        jobInput.set(CALC_MODE, CalculationMode.FIXED_VALUE);
        jobInput.set(VALUE, value);
        return jobInput;
    }

    /**
     * Multiply current partition count by given factor.
     */
    static RepartitionJobInput multiplyPartCount(final String namedInputObject, final String namedOutputObject,
            final boolean useCoalesce, final double factor) {

        final RepartitionJobInput jobInput = new RepartitionJobInput(namedInputObject, namedOutputObject, useCoalesce);
        jobInput.set(CALC_MODE, CalculationMode.MULTIPLY_PART_COUNT);
        jobInput.set(FACTOR, factor);
        return jobInput;
    }

    /**
     * Divide current partition count by given factor.
     */
    static RepartitionJobInput dividePartCount(final String namedInputObject, final String namedOutputObject,
            final boolean useCoalesce, final double factor) {

        final RepartitionJobInput jobInput = new RepartitionJobInput(namedInputObject, namedOutputObject, useCoalesce);
        jobInput.set(CALC_MODE, CalculationMode.DIVIDE_PART_COUNT);
        jobInput.set(FACTOR, factor);
        return jobInput;
    }

    /**
     * Multiply count of all executor cores by given factor.
     */
    static RepartitionJobInput multiplyExecutorCoresCount(final String namedInputObject, final String namedOutputObject,
        final boolean useCoalesce, final double factor) {

        final RepartitionJobInput jobInput = new RepartitionJobInput(namedInputObject, namedOutputObject, useCoalesce);
        jobInput.set(CALC_MODE, CalculationMode.MULTIPLY_EXECUTOR_CORES);
        jobInput.set(FACTOR, factor);
        return jobInput;
    }

    /**
     * @return mode to calculate new partition count
     */
    public CalculationMode getCalculationMode() {
        return get(CALC_MODE);
    }

    /**
     * @return fixed value for {@link CalculationMode#FIXED_VALUE} calculation mode
     */
    public int getFixedValue() {
        return getInteger(VALUE);
    }

    /**
     * @return factor for {@link CalculationMode#MULTIPLY_PART_COUNT}, {@link CalculationMode#DIVIDE_PART_COUNT} and
     *         {@link CalculationMode#MULTIPLY_EXECUTOR_CORES} calculation mode
     */
    public double getFactor() {
        return getDouble(FACTOR);
    }

    /**
     * @return <code>true</code> if coalesce should be used in case that partition count decreases
     */
    public boolean useCoalesce() {
        return get(USE_COALESCE);
    }
}

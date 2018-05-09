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
 */
package org.knime.bigdata.spark.node.preproc.missingval.compute;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;

/**
 * Missing value job input, containing configurations by column name or data type.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class SparkMissingValueJobInput extends JobInput {
    private static final String KEY_COL_CONFIG = "columnConfig";

    /** type of missing value replacement configuration key */
    public static final String KEY_OP_TYPE = "operationType";

    /** output type key, if column should be casted */
    public static final String KEY_OUTPUT_TYPE = "outputType";

    /** types of missing value replacement */
    public enum ReplaceOperation {
            /** Average */
            AVG,
            /** Rounded AVG */
            AVG_ROUNDED,
            /** Truncated AVG */
            AVG_TRUNCATED,
            /** Drop row */
            DROP,
            /** Replace with a fixed value */
            FIXED_VALUE,
            /** Maximum */
            MAX,
            /** Approximated median with relative error 0.001 */
            MEDIAN_APPROX,
            /** Exact median */
            MEDIAN_EXACT,
            /** Minimum */
            MIN,
            /** Most frequent */
            MOST_FREQ
    }

    /** fixed value key */
    public static final String KEY_FIXED_VALUE = "value";

    /** Empty serializer constructor */
    public SparkMissingValueJobInput() {
    }

    /**
     * Default constructor.
     *
     * @param namedInputObject id of input data frame
     * @param namedOutputObject id of output data frame
     */
    public SparkMissingValueJobInput(final String namedInputObject, final String namedOutputObject) {
        addNamedInputObject(namedInputObject);
        addNamedOutputObject(namedOutputObject);
        set(KEY_COL_CONFIG, new HashMap<String, Map<String, Serializable>>());
    }

    /**
     * Add configuration by column name.
     *
     * @param colName name of column
     * @param config configuration of column or null
     */
    public void addColumnConfig(final String colName, final Map<String, Serializable> config) {
        if (config != null) {
            final HashMap<String, Map<String, Serializable>> columns = get(KEY_COL_CONFIG);
            columns.put(colName, config);
        }
    }

    /**
     * Get column configuration if present.
     *
     * @param colName name of column
     * @return column configuration or null
     */
    public Map<String, Serializable> getConfig(final String colName) {
        final HashMap<String, Map<String, Serializable>> columns = get(KEY_COL_CONFIG);
        return columns.get(colName);
    }

    /** @return <code>true</code> if column configuration is empty */
    public boolean isEmtpy() {
        final HashMap<String, Map<String, Serializable>> columns = get(KEY_COL_CONFIG);
        return columns.isEmpty();
    }

    /**
     * Create a new fixed value configuration.
     *
     * @param value missing value replacement
     * @return fixed value configuration
     */
    public static Map<String, Serializable> createFixedValueConfig(final Serializable value) {
        final HashMap<String, Serializable> config = new HashMap<>();
        config.put(KEY_OP_TYPE, ReplaceOperation.FIXED_VALUE);
        config.put(KEY_FIXED_VALUE, value);
        return config;
    }

    /**
     * Create replacement configuration.
     *
     * @param operation how to replace missing values
     * @return replacement configuration
     */
    public static Map<String, Serializable> createConfig(final ReplaceOperation operation) {
        final HashMap<String, Serializable> config = new HashMap<>();
        config.put(KEY_OP_TYPE, operation);
        return config;
    }

    /**
     * Add output type cast to given configuration.
     *
     * @param config column configuration
     * @param outputType cast column to other data type before missing value apply
     * @return fixed value configuration
     */
    public static Map<String, Serializable> addCastConfig(final Map<String, Serializable> config, final IntermediateDataType outputType) {
        config.put(KEY_OUTPUT_TYPE, outputType);
        return config;
    }
}

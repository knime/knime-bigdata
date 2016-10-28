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
 *   Created on Aug 10, 2016 by sascha
 */
package com.knime.bigdata.spark.node.io.genericdatasource.writer;

import java.util.HashMap;
import java.util.Map;

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;


/**
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class Spark2GenericDataSourceJobInput extends JobInput {

    private static final String KEY_FORMAT = "format";
    private static final String KEY_UPLOAD_DRIVER = "uploadDriver";
    private static final String KEY_OUTPUT_PATH = "outputPath";
    private static final String KEY_SAVE_MODE = "saveMode";
    private static final String KEY_USE_HIVE_CONTEXT = "useHiveContext";
    private static final String KEY_OPTIONS = "options";
    private static final String KEY_PARTITION_BY = "partitionBy";
    private static final String KEY_NUM_PARTITIONS = "numPartitions";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public Spark2GenericDataSourceJobInput() {}

    /**
     * @param namedObject - the name of the input object (e.g. RDD)
     * @param format - fully qualified or short format name (e.g. parquet)
     * @param uploadDriver - Upload local jar files or depend on cluster version.
     * @param outputPath - the name of the output directory to be created
     * @param spec the {@link IntermediateSpec} of the table
     * @param saveMode Spark save mode
     */
    public Spark2GenericDataSourceJobInput(final String namedObject, final String format, final boolean uploadDriver,
            final String outputPath, final IntermediateSpec spec, final String saveMode) {

        addNamedInputObject(namedObject);
        withSpec(namedObject, spec);
        set(KEY_FORMAT, format);
        set(KEY_UPLOAD_DRIVER, uploadDriver);
        set(KEY_OUTPUT_PATH, outputPath);
        set(KEY_SAVE_MODE, saveMode);
    }

    /** @return format name */
    public String getFormat() { return get(KEY_FORMAT);}

    /** @return If true, upload bundled jar. */
    public boolean uploadDriver() { return get(KEY_UPLOAD_DRIVER); }

    /** @return output path */
    public String getOutputPath() { return get(KEY_OUTPUT_PATH); }

    /** @param useHiveContext - use hive context if true, SQL context otherwise */
    public void setUseHiveContext(final boolean useHiveContext) { set(KEY_USE_HIVE_CONTEXT, useHiveContext); }

    /** @return true if hive required instead of SQL context */
    public boolean useHiveContext() { return getOrDefault(KEY_USE_HIVE_CONTEXT, false); }

    /**
     * @see <a href="http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes">save modes</a>
     * @return Spark SaveMode as String
     */
    public String getSaveMode() { return get(KEY_SAVE_MODE); }

    /**
     * Custom options passed to writer
     * @param key - Option key
     * @param value - Value as string.
     */
    public void setOption(final String key, final String value) {
        if (!hasOptions()) {
            set(KEY_OPTIONS, new HashMap<String, String>());
        }

        getOptions().put(key, value);
    }

    /** @return true if job input has writer options */
    public boolean hasOptions() {
        return has(KEY_OPTIONS);
    }

    /** @return custom writer options */
    public Map<String, String> getOptions() {
        return get(KEY_OPTIONS);
    }

    /** @param partitionBy - Columns to partition on */
    public void setPartitioningBy(final String partitionBy[]) {
        set(KEY_PARTITION_BY, partitionBy);
    }

    /** @return true if partitioning columns set */
    public boolean usePartitioning() {
        return has(KEY_PARTITION_BY);
    }

    /**
     * @return Columns to partition on.
     * @see Spark2GenericDataSourceJobInput#usePartitioning() to validate that partition is used.
     */
    public String[] getPartitionBy() {
        return get(KEY_PARTITION_BY);
    }

    /** @param numPartitions - Overwrite partition count */
    public void setNumPartitions(final int numPartitions) {
        set(KEY_NUM_PARTITIONS, numPartitions);
    }

    /** @return true if partition count is set */
    public boolean overwriteNumPartitons() {
        return has(KEY_NUM_PARTITIONS);
    }

    /**
     * @return number of partitions
     * @see #overwriteNumPartitons() to validate if number is set
     */
    public int getNumPartitions() {
        return getInteger(KEY_NUM_PARTITIONS);
    }
}

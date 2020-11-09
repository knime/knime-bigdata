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
 *   Created on Aug 10, 2016 by sascha
 */
package org.knime.bigdata.spark.node.io.genericdatasource.reader;

import java.util.HashMap;
import java.util.Map;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;


/**
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class GenericDataSource2SparkJobInput extends JobInput {

    private static final String KEY_FORMAT = "format";
    private static final String KEY_UPLOAD_DRIVER = "uploadDriver";
    private static final String KEY_INPUT_PATH = "inputPath";
    private static final String KEY_USE_HIVE_CONTEXT = "useHiveContext";
    private static final String KEY_OPTIONS = "options";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public GenericDataSource2SparkJobInput() {}

    /**
     * @param namedOutputObject - the name of the output object to generate
     * @param format - fully qualified or short format name (e.g. parquet)
     * @param uploadDriver - Upload local jar files or depend on cluster version.
     * @param inputPath - the input directory or file
     */
    public GenericDataSource2SparkJobInput(final String namedOutputObject, final String format, final boolean uploadDriver,
            final String inputPath) {

        addNamedOutputObject(namedOutputObject);
        set(KEY_FORMAT, format);
        set(KEY_UPLOAD_DRIVER, uploadDriver);
        set(KEY_INPUT_PATH, inputPath);
    }

    /** @return format name */
    public String getFormat() { return get(KEY_FORMAT);}

    /** @return If true, upload bundled jar. */
    public boolean uploadDriver() { return get(KEY_UPLOAD_DRIVER); }

    /** @return Input path */
    public String getInputPath() { return get(KEY_INPUT_PATH); }

    /** @param useHiveContext - use hive context if true, SQL context otherwise */
    public void setUseHiveContext(final boolean useHiveContext) { set(KEY_USE_HIVE_CONTEXT, useHiveContext); }

    /** @return true if hive required instead of SQL context */
    public boolean useHiveContext() { return getOrDefault(KEY_USE_HIVE_CONTEXT, false); }

    /**
     * Custom options passed to reader
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

    /** @return custom reader options */
    public Map<String, String> getOptions() {
        return get(KEY_OPTIONS);
    }
}

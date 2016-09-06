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
package com.knime.bigdata.spark.node.io.parquet.reader;

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;


/**
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class Parquet2SparkJobInput extends JobInput {
    private final static String KEY_INPUT_PATH = "inputPath";
    private final static String KEY_IS_HDFS_PATH = "isHDFSPath";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public Parquet2SparkJobInput() {}

    /**
     * constructor - simply stores parameters
     * @param namedOutputObject - the name of the output object to generate
     * @param inputPath - the input parquet dir or file
     * @param isHDFSPath true if input path is in HDFS
     */
    public Parquet2SparkJobInput(final String namedOutputObject, final String inputPath, final boolean isHDFSpath) {
        addNamedOutputObject(namedOutputObject);
        set(KEY_INPUT_PATH, inputPath);
        set(KEY_IS_HDFS_PATH, isHDFSpath);
    }

    /**
     * @return Input parquet file
     */
    public String getInputPath() {
        return get(KEY_INPUT_PATH);
    }

    /**
     * @return true if input path is in HDFS
     */
    public boolean isHDFSPath() {
        return get(KEY_IS_HDFS_PATH);
    }
}

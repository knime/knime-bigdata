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
package com.knime.bigdata.spark.node.io.parquet.writer;

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;


/**
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class Spark2ParquetJobInput extends JobInput {

    private static final String KEY_OUTPUT_PATH = "outputPath";
    private static final String KEY_SAVE_MODE = "saveMode";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public Spark2ParquetJobInput() {}

    /**
     * @param namedObject the name of the input object (e.g. RDD)
     * @param outputPath the name of the parquet dir to be created
     * @param spec the {@link IntermediateSpec} of the table
     * @param saveMode Spark save mode (error, append, overwrite or ignore)
     */
    public Spark2ParquetJobInput(final String namedObject, final String outputPath, final IntermediateSpec spec, final String saveMode) {
        addNamedInputObject(namedObject);
        withSpec(namedObject, spec);
        set(KEY_OUTPUT_PATH, outputPath);
        set(KEY_SAVE_MODE, saveMode);
    }

    /**
     * @return Output parquet dir
     */
    public String getOutputPath() {
        return get(KEY_OUTPUT_PATH);
    }

    /**
     * @see <a href="http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes">save modes</a>
     * @return Spark SaveMode as String
     */
    public String getSaveMode() {
        return get(KEY_SAVE_MODE);
    }
}

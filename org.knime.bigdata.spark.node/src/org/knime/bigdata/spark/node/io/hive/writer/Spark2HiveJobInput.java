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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark.node.io.hive.writer;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class Spark2HiveJobInput extends JobInput {

    private static final String KEY_TABLE_NAME = "tableName";

    private static final String KEY_FILE_FORMAT = "fileFormat";

    private static final String KEY_COMPRESSION = "compression";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public Spark2HiveJobInput() {
    }

    /**
     * Creates the Input for the Spark2Hive Job
     * @param namedObject the name of the input object (e.g. RDD)
     * @param tableName the name of the Hive table to be created
     * @param spec the {@link IntermediateSpec} of the table
     * @param fileFormat The file format that is used for the hive table
     * @param compression the compression to use
     *
     */
    public Spark2HiveJobInput(final String namedObject, final String tableName, final IntermediateSpec spec,
        final String fileFormat, final String compression) {
        addNamedInputObject(namedObject);
        withSpec(namedObject, spec);
        set(KEY_TABLE_NAME, tableName);
        set(KEY_FILE_FORMAT, fileFormat);
        set(KEY_COMPRESSION, compression);
    }

    /**
     * @return the Hive table name
     */
    public String getHiveTableName() {
        return get(KEY_TABLE_NAME);
    }

    /**
     * @return the file format
     */
    public String getFileFormat() {
        return get(KEY_FILE_FORMAT);
    }

    /**
     * @return the compression to use
     */
    public String getCompression() {
        return get(KEY_COMPRESSION);
    }
}

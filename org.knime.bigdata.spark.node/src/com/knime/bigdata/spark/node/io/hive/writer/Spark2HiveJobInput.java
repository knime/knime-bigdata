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
package com.knime.bigdata.spark.node.io.hive.writer;

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;


/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class Spark2HiveJobInput extends JobInput {

    private static final String KEY_TABLE_NAME = "tableName";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public Spark2HiveJobInput() {}

    /**
     * @param namedObject the name of the input object (e.g. RDD)
     * @param tableName the name of the Hive table to be created
     * @param spec the {@link IntermediateSpec} of the table
     */
    public Spark2HiveJobInput(final String namedObject, final String tableName, final IntermediateSpec spec) {
        addNamedInputObject(namedObject);
        withSpec(namedObject, spec);
        set(KEY_TABLE_NAME, tableName);
    }

    /**
     * @return the Hive table name
     */
    public String getHiveTableName() {
        return get(KEY_TABLE_NAME);
    }
}

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
 *   Created on Sep 5, 2016 by sascha
 */
package org.knime.bigdata.spark.node.io.database.writer;

import java.util.Properties;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class Spark2DatabaseJobInput extends JobInput {
    private final static String KEY_URL = "url";
    private final static String KEY_TABLE = "table";
    private final static String KEY_SAVE_MODE = "saveMode";
    private final static String KEY_CON_PROPERTIES = "conProperties";

    /** Empty deserialization constructor. */
    public Spark2DatabaseJobInput() {}

    /**
     * Constructor with required parameters
     * @param namedInputObject - the name of the input object to read from
     * @param spec the {@link IntermediateSpec} of the table
     * @param url - JDBC connection URL
     * @param table - the table to write through
     * @param saveMode - spark save mode
     * @param conProperties - JDBC properties (driver, user...)
     */
    public Spark2DatabaseJobInput(final String namedInputObject, final IntermediateSpec spec,
            final String url, final String table, final String saveMode, final Properties conProperties) {

        addNamedInputObject(namedInputObject);
        withSpec(namedInputObject, spec);
        set(KEY_URL, url);
        set(KEY_TABLE, table);
        set(KEY_SAVE_MODE, saveMode);
        set(KEY_CON_PROPERTIES, conProperties);
    }

    /** @return JDB URL */
    public String getUrl() { return get(KEY_URL); }

    /** @return Destination table name. */
    public String getTable() { return get(KEY_TABLE); }

    /** @return spark save mode */
    public String getSaveMode() { return get(KEY_SAVE_MODE); }

    /** @return JDBC connection properties. */
    public Properties getConnectionProperties() { return get(KEY_CON_PROPERTIES); }

    /** @param driver - Driver name in JDBC properties. */
    public void setDriver(final String driver) {
        ((Properties) get(KEY_CON_PROPERTIES)).put("driver", driver);
    }
}

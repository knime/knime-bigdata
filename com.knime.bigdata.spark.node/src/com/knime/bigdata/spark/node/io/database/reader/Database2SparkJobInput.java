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
 *   Created on Sep 5, 2016 by sascha
 */
package com.knime.bigdata.spark.node.io.database.reader;

import java.util.Properties;

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class Database2SparkJobInput extends JobInput {
    private final static String KEY_URL = "url";
    private final static String KEY_TABLE = "table";
    private final static String KEY_CON_PROPERTIES = "conProperties";

    private final static String KEY_PARTITION_COL = "partitionCol";
    private final static String KEY_LOWER_BOUND = "lowerBound";
    private final static String KEY_UPPER_BOUND = "upperBound";
    private final static String KEY_NUM_PARTITIONS = "numPartitions";

    /** Empty deserialization constructor. */
    public Database2SparkJobInput() {}

    /**
     * Constructor with required parameters
     * @param namedOutputObject - the name of the output object to generate
     * @param url - JDBC connection URL
     * @param table - input table or query to execute
     * @param conProperties - JDBC properties (driver, user...)
     */
    public Database2SparkJobInput(final String namedOutputObject,
            final String url, final String table, final Properties conProperties) {

        addNamedOutputObject(namedOutputObject);
        set(KEY_URL, url);
        set(KEY_TABLE, cleanupSQL(table));
        set(KEY_CON_PROPERTIES, conProperties);
    }

    /** @return JDBC URL */
    public String getUrl() { return get(KEY_URL); }

    /** @return Input table or query */
    public String getTable() { return get(KEY_TABLE); }

    /** @return JDBC connection properties. */
    public Properties getConnectionProperties() { return get(KEY_CON_PROPERTIES); }

    /** @return true if partitioning settings available. */
    public boolean hasPartitioning() { return has(KEY_PARTITION_COL);  }

    /** @return Column to partition on */
    public String getPartitionColumn() { return get(KEY_PARTITION_COL); }

    /** @return Partitioning lower bound */
    public long getLowerBound() { return getLong(KEY_LOWER_BOUND); }

    /** @return Partitioning upper bound */
    public long getUpperBound() { return getLong(KEY_UPPER_BOUND); }

    /** @return Number of partitions */
    public int getNumPartitions() { return getInteger(KEY_NUM_PARTITIONS); }

    /** @param driver - Driver name in JDBC properties. */
    public void setDriver(final String driver) {
        ((Properties) get(KEY_CON_PROPERTIES)).put("driver", driver);
    }

    /**
     * Optional partitioning by column settings.
     * @param col - Column name
     * @param lower - Lower bound
     * @param upper - Upper bound
     * @param num - Number of partitions.
     */
    public void setPartitioning(final String col, final long lower, final long upper, final int num) {
        set(KEY_PARTITION_COL, col);
        set(KEY_LOWER_BOUND, lower);
        set(KEY_UPPER_BOUND, upper);
        set(KEY_NUM_PARTITIONS, num);
    }

    /**
     * @param sql the sql string to cleanup
     * @return the cleaned up sql string
     */
    public static String cleanupSQL(final String sql) {
        //TODO: Provide a (un)escape function for strings for client and server side
        if (sql == null || sql.isEmpty()) {
            return sql;
        }
        //replace all new lines with spaces
        final String cleanedSQL = sql.replaceAll("\\n", " ");
        return cleanedSQL;
    }
}

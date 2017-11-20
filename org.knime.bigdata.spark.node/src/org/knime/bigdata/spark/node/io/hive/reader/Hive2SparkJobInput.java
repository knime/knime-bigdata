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
package org.knime.bigdata.spark.node.io.hive.reader;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;


/**
 *
 * @author dwk, jfr
 */
@SparkClass
public class Hive2SparkJobInput extends JobInput {



    private final static String KEY_QUERY = "query";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public Hive2SparkJobInput() {}

    /**
     * constructor - simply stores parameters
     * @param namedOutputObject - the name of the output object to generate
     * @param hiveQuery - the hive query to execute
     */
    public Hive2SparkJobInput(final String namedOutputObject, final String hiveQuery) {
        addNamedOutputObject(namedOutputObject);
        final String sql = cleanupSQL(hiveQuery);
        set(KEY_QUERY, sql);
    }

    /**
     * @return the Hive query to execute
     */
    public String getQuery() {
        return get(KEY_QUERY);
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

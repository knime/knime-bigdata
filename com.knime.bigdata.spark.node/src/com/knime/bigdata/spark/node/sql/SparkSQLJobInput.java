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
package com.knime.bigdata.spark.node.sql;

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class SparkSQLJobInput extends JobInput {
    /** Temporary table name placeholder in query */
    public final static String TABLE_PLACEHOLDER = "#table#";

    private final static String KEY_QUERY = "query";

    /** Empty deserialization constructor. */
    public SparkSQLJobInput() {}

    /**
     * Constructor with required parameters
     * @param namedInputObject - the name of the input object
     * @param inputSpec - the {@link IntermediateSpec} of input data
     * @param namedOutputObject - the name of the output object to generate
     * @param query - Query to execute
     */
    public SparkSQLJobInput(final String namedInputObject, final IntermediateSpec inputSpec,
            final String namedOutputObject, final String query) {

        addNamedInputObject(namedInputObject);
        withSpec(namedInputObject, inputSpec);
        addNamedOutputObject(namedOutputObject);
        set(KEY_QUERY, query);
    }

    /**
     * @param temporaryTable - Temporary table name
     * @return Query to execute
     */
    public String getQuery(final String temporaryTable) {
        final String query = get(KEY_QUERY);
        return query.replaceAll("`" + TABLE_PLACEHOLDER + "`", "`" + temporaryTable + "`") // quoted version
                    .replaceAll(TABLE_PLACEHOLDER, "`" + temporaryTable + "`");
    }
}

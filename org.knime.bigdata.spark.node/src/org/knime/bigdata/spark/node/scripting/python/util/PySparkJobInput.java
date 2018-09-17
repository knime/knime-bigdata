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
 *   Created on 22.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark.node.scripting.python.util;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Class for the JobInput of a PySpark job
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
@SparkClass
public class PySparkJobInput extends JobInput {

    private static final String KEY_CODE = "sourcecode";

    private static final String KEY_UID = "uid";

    private static final String KEY_ROWS = "numRows";

    /** Empty deserialization constructor. */
    public PySparkJobInput() {
    }

    /**
     * Constructor for cases with at least one named input object and one named output object.
     *
     * @param namedInputObject May be null
     * @param optionalNamedInputObject May be null, if not then namedInputObject must not be null either.
     * @param namedOutputObject May be null
     * @param optionalNamedOutputObject May be null, if not then namedOutputObject must not be null either.
     * @param pyScript the PySpark source code
     * @param uid the unique id for the dataFrames in the collector
     * @param numRows the number of rows to run the job on
     */
    public PySparkJobInput(final String namedInputObject, final String optionalNamedInputObject,
        final String namedOutputObject, final String optionalNamedOutputObject, final String pyScript,
        final String uid, final int numRows) {

        if (namedInputObject != null) {
            addNamedInputObject(namedInputObject);
            if (optionalNamedInputObject != null) {
                addNamedInputObject(optionalNamedInputObject);
            }
        }

        if (namedOutputObject != null) {
            addNamedOutputObject(namedOutputObject);
            if (optionalNamedOutputObject != null) {
                addNamedOutputObject(optionalNamedOutputObject);
            }
        }
        set(KEY_CODE, pyScript);
        set(KEY_UID, uid);
        set(KEY_ROWS, numRows);
    }

    /**
     * @return the PySpark script
     */
    public String getPyScript() {
        return get(KEY_CODE);
    }

    /**
     * @return the unique id for the dataFrames in the collector
     */
    public String getUID() {
        return get(KEY_UID);
    }

    /**
     * @return the number of rows to execute on. -1 for all.
     */
    public int getNumRows() {
        return getInteger(KEY_ROWS);
    }
}

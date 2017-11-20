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
 *   Created on May 5, 2016 by bjoern
 */
package com.knime.bigdata.spark.node.scripting.java.util;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class JavaSnippetJobOutput extends JobOutput {

    /**
     * Empty constructor for serialization and for cases where the snippet does not
     * create a named output object (e.g. sink snippet).
     */
    public JavaSnippetJobOutput() {
    }

    /**
     * Constructor used to return a spec back to the client
     *
     * @param namedOutputObject
     * @param namedOutputObjectSpec
     */
    public JavaSnippetJobOutput(final String namedOutputObject, final IntermediateSpec namedOutputObjectSpec) {
        withSpec(namedOutputObject, namedOutputObjectSpec);
    }
}

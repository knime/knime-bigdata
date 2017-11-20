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
package org.knime.bigdata.spark.node.scripting.java.util;

import java.util.Collections;
import java.util.Map;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoer Lohrmann, KNIME.com
 */
@SparkClass
public class JavaSnippetJobInput extends JobInput {

    private final static String KEY_SNIPPET_CLASS = "snippetClass";

    private final static String FLOW_VAR_VALUES = "flowVars";

    /**
     * Zero-param constructor for serialization.
     */
    public JavaSnippetJobInput() {
    }

    /**
     * Constructor for cases with at least one named input object and one named output object.
     *
     * @param namedInputObject May be null
     * @param optionalNamedInputObject May be null, if not then namedInputObject must not be null either.
     * @param namedOutputObject May be null
     * @param snippetClass
     * @param flowVariableValues a map that associates flow variable names with values
     */
    public JavaSnippetJobInput(final String namedInputObject, final String optionalNamedInputObject,
        final String namedOutputObject, final String snippetClass, final Map<String, Object> flowVariableValues) {

        if (namedInputObject != null) {
            addNamedInputObject(namedInputObject);
            if (optionalNamedInputObject != null) {
                addNamedInputObject(optionalNamedInputObject);
            }
        }

        if (namedOutputObject != null) {
            addNamedOutputObject(namedOutputObject);
        }

        set(KEY_SNIPPET_CLASS, snippetClass);
        if (!flowVariableValues.isEmpty()) {
            set(FLOW_VAR_VALUES, flowVariableValues);
        }
    }

    /**
     * @return the name of the Java snippet class to load from the uploaded snippet jar file
     */
    public String getSnippetClass() {
        return get(KEY_SNIPPET_CLASS);
    }

    /**
     *
     * @return a map that associates flow variable names to values
     */
    public Map<String, Object> getFlowVariableValues() {
        return getOrDefault(FLOW_VAR_VALUES, Collections.<String, Object> emptyMap());
    }
}

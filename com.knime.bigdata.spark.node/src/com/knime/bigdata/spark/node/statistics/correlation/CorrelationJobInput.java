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
 *   Created on 16.05.2016 by koetter
 */
package com.knime.bigdata.spark.node.statistics.correlation;

import com.knime.bigdata.spark.core.job.ColumnsJobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.EnumContainer.CorrelationMethod;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class CorrelationJobInput extends ColumnsJobInput {

    private static final String METHOD = "METHOD";

    /**Paramless constructor for automatic deserialization.*/
    public CorrelationJobInput() {}

    /**
     * @param namedInputObject the input named object
     * @param method the {@link CorrelationMethod} to use
     * @param namedOutputObject the optional named output object name
     * @param colIdxs the indices of the columns to compute the correlation for
     */
    public CorrelationJobInput(final String namedInputObject, final CorrelationMethod method,
        final String namedOutputObject, final Integer... colIdxs) {
        super(namedInputObject, colIdxs);
        if (namedOutputObject != null) {
            addNamedOutputObject(namedOutputObject);
        }
        set(METHOD, method.name());
    }

    /**
     * @return {@link CorrelationMethod} to use
     */
    public CorrelationMethod getMethod() {
        final String methodName = get(METHOD);
        return CorrelationMethod.valueOf(methodName);
    }
}

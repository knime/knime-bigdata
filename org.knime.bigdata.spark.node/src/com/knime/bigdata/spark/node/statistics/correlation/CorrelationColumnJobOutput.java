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
 *   Created on 16.05.2016 by koetter
 */
package com.knime.bigdata.spark.node.statistics.correlation;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class CorrelationColumnJobOutput extends JobOutput {

    private static final String VAL = "value";

    /**
     *
     */
    public CorrelationColumnJobOutput() {}

    /**
     * @param correlation value between the two defined column
     */
    public CorrelationColumnJobOutput(final double correlation) {
        set(VAL, correlation);
    }

    /**
     * @return the correlation value between the two defined columns
     */
    public double getCorrelationValue() {
        return getDouble(VAL);
    }
}

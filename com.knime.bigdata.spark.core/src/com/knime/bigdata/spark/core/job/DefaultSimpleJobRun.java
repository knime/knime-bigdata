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
 *   Created on May 3, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.job;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextManager;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 * @param <I> Input type for job
 */
public class DefaultSimpleJobRun<I extends JobInput> extends DefaultJobRun<I, EmptyJobOutput>
    implements SimpleJobRun<I> {

    /**
     * Constructor
     *
     * @param input
     * @param sparkJobClass
     */
    public DefaultSimpleJobRun(final I input, final Class<?> sparkJobClass) {
        super(input, sparkJobClass, EmptyJobOutput.class);
    }

    @Override
    public EmptyJobOutput run(final SparkContextID contextID, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {

        SparkContextManager.getOrCreateSparkContext(contextID).startJobAndWaitForResult(this, exec);
        return EmptyJobOutput.getInstance();
    }
}

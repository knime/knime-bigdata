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
 *   Created on Apr 11, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.job;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextManager;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;

/**
 * Default implementation of {@link JobRun} that is sufficient for many jobs.
 *
 * @author Bjoern Lohrmann, KNIME.com
 * @param <I> The type of the input that the job expects.
 * @param <O> The type of the output that the job provides.
 */
public class DefaultJobRun<I extends JobInput, O extends JobOutput> implements JobRun<I, O> {

    private final I m_input;

    private final Class<?> m_sparkJobClass;

    private final Class<O> m_jobOutputClass;

    private final ClassLoader m_jobOutputClassLoader;

    /**
     * Constructs a new job run, where the class loader is set to the class loader of the provided
     * job class.
     *
     * @param input A job input object that provides the input parameters the job needs to run.
     * @param sparkJobClass The class that implements the job.
     * @param jobOutputClass The class that will be used by the job to return its output to KNIME.
     */
    public DefaultJobRun(final I input, final Class<?> sparkJobClass, final Class<O> jobOutputClass) {
        this(input, sparkJobClass, jobOutputClass, sparkJobClass.getClassLoader());
    }

    /**
     * Constructs a new job run.
     *
     * @param input A job input object that provides the input parameters the job needs to run.
     * @param sparkJobClass The class that implements the job.
     * @param jobOutputClass The class that will be used by the job to return its output to KNIME.
     * @param jobOutputClassLoader The class loader that should be used to load objects of jobOutputClass
     */
    public DefaultJobRun(final I input, final Class<?> sparkJobClass, final Class<O> jobOutputClass,
        final ClassLoader jobOutputClassLoader) {
        super();
        m_input = input;
        m_sparkJobClass = sparkJobClass;
        m_jobOutputClass = jobOutputClass;
        m_jobOutputClassLoader = jobOutputClassLoader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public O run(final SparkContextID contextID) throws KNIMESparkException {
        try {
            return SparkContextManager.getOrCreateSparkContext(contextID).startJobAndWaitForResult(this,
                new ExecutionMonitor());
        } catch (CanceledExecutionException e) {
            // cannot happen
            return null;
        }
    }

    @Override
    public O run(final SparkContextID contextID, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {

        if (exec == null) {
            return run(contextID);
        } else {
            return SparkContextManager.getOrCreateSparkContext(contextID).startJobAndWaitForResult(this, exec);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<?> getJobClass() {
        return m_sparkJobClass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public I getInput() {
        return m_input;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<O> getJobOutputClass() {
        return m_jobOutputClass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClassLoader getJobOutputClassLoader() {
        return m_jobOutputClassLoader;
    }
}
